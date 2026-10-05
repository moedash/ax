// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package taskclient reaches AX tasks through Temporal. Reads are queries,
// changes are updates, and listing walks the running task workflows.
package taskclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// listPageSize is how many executions one visibility page carries.
const listPageSize = 100

const (
	// changeWait is how long a change waits for its own result. A change is
	// durable once the service has it, but placing an actor on a worker can take
	// minutes, and no caller should hold a request open that long.
	changeWait = 10 * time.Second
	// queryWait bounds asking one task for its state while listing, so a single
	// task with no worker behind it cannot stall the whole listing.
	queryWait = 5 * time.Second
)

// Client is the Temporal-backed implementation of orchestration.Tasks.
type Client struct {
	client    sdkclient.Client
	taskQueue string
	// changeWait and queryWait bound the two waits. They are fields so a test
	// does not have to sit through them.
	changeWait time.Duration
	queryWait  time.Duration
}

// New returns a client that starts task workflows on the given task queue.
func New(c sdkclient.Client, taskQueue string) *Client {
	return &Client{
		client:     c,
		taskQueue:  taskQueue,
		changeWait: changeWait,
		queryWait:  queryWait,
	}
}

// Create starts a task. The spec travels as an update sent together with the
// start, so the caller gets the task back as it stands once the sandbox is in
// the state the spec asks for, in one round trip.
func (c *Client) Create(
	ctx context.Context,
	desired *workflows.TaskDesiredState,
) (*v1alpha1.Task, error) {
	if desired == nil || desired.Task.GetMetadata().GetName() == "" {
		return nil, fmt.Errorf("%w: task name is required", orchestration.ErrInvalidTask)
	}
	atespace := atespaceOf(desired.Task.GetMetadata().GetAtespace())
	name := desired.Task.GetMetadata().GetName()
	workflowID := workflows.TaskWorkflowID(atespace, name)

	start := c.client.NewWithStartWorkflowOperation(sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: c.taskQueue,
		// Tasks are immutable, so a name that is already running is refused. A
		// task whose workflow has ended, because it was deleted, is created again
		// under the same name.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		// The update carries the spec, so the start does not repeat it.
	}, workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})

	// An update is only accepted by a worker, so the wait is bounded: the task
	// has been created either way, and a control plane with no workers must not
	// hold the caller.
	waitCtx, cancel := context.WithTimeout(ctx, c.changeWait)
	defer cancel()

	handle, err := c.client.UpdateWithStartWorkflow(waitCtx, sdkclient.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: sdkclient.UpdateWorkflowOptions{
			UpdateName: workflows.UpdateApply,
			Args:       []any{desired},
			// Waiting for the result rather than for acceptance is one round
			// trip instead of two; the wait above is what bounds it either way.
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		if timedOut(ctx, waitCtx) {
			return c.acceptedTask(ctx, workflowID, desired.Task)
		}
		return nil, mapError(err)
	}

	var task v1alpha1.Task
	if err := handle.Get(waitCtx, &task); err != nil {
		if timedOut(ctx, waitCtx) {
			return c.acceptedTask(ctx, workflowID, desired.Task)
		}
		return nil, mapError(err)
	}
	return &task, nil
}

// acceptedTask answers for a task whose workflow has not reported back inside
// the wait. The start and the update travel in one request, so an expired wait
// says nothing about whether the task was created: the execution is asked for
// directly, and the task is reported Pending only if it is really there.
//
// The latest run has to be open. A closed run under the same ID is the task
// that was deleted before, and says nothing about whether this start landed.
func (c *Client) acceptedTask(
	ctx context.Context, workflowID string, task *v1alpha1.Task,
) (*v1alpha1.Task, error) {
	describeCtx, cancel := context.WithTimeout(ctx, c.queryWait)
	defer cancel()

	described, err := c.client.DescribeWorkflowExecution(describeCtx, workflowID, "")
	if err != nil {
		return nil, fmt.Errorf("%w: task %s was not accepted: %s",
			orchestration.ErrTaskUnavailable, workflowID, err)
	}
	if described.GetWorkflowExecutionInfo().GetStatus() !=
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil, fmt.Errorf("%w: task %s was not accepted: its workflow is not running",
			orchestration.ErrTaskUnavailable, workflowID)
	}
	out, ok := proto.Clone(task).(*v1alpha1.Task)
	if !ok {
		return task, nil
	}
	out.ApiVersion = v1alpha1.APIVersion
	out.Kind = v1alpha1.KindTask
	out.Status = &v1alpha1.TaskStatus{Phase: v1alpha1.PhasePending}
	if out.Metadata != nil {
		// The workflow owns the creation time and has not answered yet, so
		// there is nothing truthful to put here.
		out.Metadata.CreationTimestamp = nil
	}
	return out, nil
}

// timedOut reports whether the bounded wait expired while the caller is still
// waiting, as opposed to the caller giving up.
func timedOut(ctx, waitCtx context.Context) bool {
	return ctx.Err() == nil && waitCtx.Err() != nil
}

// Get returns one task as its workflow sees it. Only a worker can answer for a
// task, so the wait is bounded and a task nothing answers for is reported
// unavailable rather than left hanging.
//
// A workflow answers queries after it has closed, for as long as its history
// is retained. A task exists only while its workflow runs, whatever the run
// closed as, so the query is sent with a reject condition and a rejection is
// the task being gone.
func (c *Client) Get(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	workflowID := workflows.TaskWorkflowID(atespaceOf(atespace), name)
	queryCtx, cancel := context.WithTimeout(ctx, c.queryWait)
	defer cancel()

	request := &sdkclient.QueryWorkflowWithOptionsRequest{
		WorkflowID:           workflowID,
		QueryType:            workflows.QueryTask,
		QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
	}
	resp, err := c.client.QueryWorkflowWithOptions(queryCtx, request)
	if err != nil {
		if timedOut(ctx, queryCtx) {
			return nil, fmt.Errorf("%w: no worker answered for task %s",
				orchestration.ErrTaskUnavailable, workflowID)
		}
		return nil, mapError(err)
	}
	if resp.QueryRejected != nil {
		return nil, fmt.Errorf("%w: %s", orchestration.ErrTaskNotFound, workflowID)
	}
	var task v1alpha1.Task
	if err := resp.QueryResult.Get(&task); err != nil {
		return nil, fmt.Errorf("decoding task %s/%s: %w", atespace, name, err)
	}
	return &task, nil
}

// List returns the running task workflows of an atespace. A listing must not
// cost a round trip to every task, so it reads the identities from visibility
// and leaves the per-task status to GetTask.
func (c *Client) List(
	ctx context.Context,
	atespace string,
	limit, offset int64,
) ([]*v1alpha1.Task, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	executions, err := c.listExecutions(ctx, atespace, limit+offset)
	if err != nil {
		return nil, err
	}
	if offset >= int64(len(executions)) {
		return []*v1alpha1.Task{}, nil
	}
	executions = executions[offset:]
	if int64(len(executions)) > limit {
		executions = executions[:limit]
	}

	tasks := make([]*v1alpha1.Task, 0, len(executions))
	for _, execution := range executions {
		tasks = append(tasks, listedTask(execution))
	}
	return tasks, nil
}

// listedTask rebuilds a task's identity from its workflow ID. A listing does
// not carry the task's phase; GetTask has it.
func listedTask(execution *workflowpb.WorkflowExecutionInfo) *v1alpha1.Task {
	atespace, name := splitWorkflowID(execution.GetExecution().GetWorkflowId())
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:              name,
			Atespace:          atespace,
			CreationTimestamp: execution.GetStartTime(),
		},
		// An actor carries the name of the task that owns it.
		Status: &v1alpha1.TaskStatus{Actor: name},
	}
}

// splitWorkflowID takes a task's atespace and name back out of its workflow
// ID. Neither may contain a slash, and the separator is the last one so that
// an ID from somewhere else does not come apart in a surprising way.
func splitWorkflowID(workflowID string) (atespace, name string) {
	slash := strings.LastIndex(workflowID, "/")
	if slash < 0 {
		return v1alpha1.DefaultAtespace, workflowID
	}
	return workflowID[:slash], workflowID[slash+1:]
}

// listQuery selects the running task workflows. The atespace is matched from
// the workflow ID after listing, so the query uses only the standard attributes
// every namespace has.
func listQuery() string {
	return fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'",
		workflows.TaskWorkflowType)
}

// listExecutions walks visibility until it has at least want running task
// workflows, keeping only the ones in the atespace when one is named.
func (c *Client) listExecutions(
	ctx context.Context,
	atespace string,
	want int64,
) ([]*workflowpb.WorkflowExecutionInfo, error) {
	query := listQuery()
	keep := func(*workflowpb.WorkflowExecutionInfo) bool { return true }
	if atespace != "" && atespace != "*" {
		ns := atespaceOf(atespace)
		keep = func(e *workflowpb.WorkflowExecutionInfo) bool {
			got, _ := splitWorkflowID(e.GetExecution().GetWorkflowId())
			return got == ns
		}
	}

	var (
		out   []*workflowpb.WorkflowExecutionInfo
		token []byte
	)
	for {
		resp, err := c.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      listPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tasks: %w", err)
		}
		for _, e := range resp.GetExecutions() {
			if keep(e) {
				out = append(out, e)
			}
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 || int64(len(out)) >= want {
			return out, nil
		}
	}
}

// Suspend checkpoints a task's sandbox and stops it.
func (c *Client) Suspend(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	return c.change(ctx, atespace, name, workflows.UpdateSuspend)
}

// Resume puts a suspended sandbox back on a worker.
func (c *Client) Resume(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	return c.change(ctx, atespace, name, workflows.UpdateResume)
}

// change sends an update that answers with the task. The wait is bounded, and a
// task whose worker does not answer in that time is reported unavailable rather
// than left hanging.
func (c *Client) change(
	ctx context.Context,
	atespace, name, update string,
) (*v1alpha1.Task, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.changeWait)
	defer cancel()

	handle, err := c.client.UpdateWorkflow(waitCtx, sdkclient.UpdateWorkflowOptions{
		WorkflowID: workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName: update,
		// The answer is the point of the call, so wait for it rather than for
		// acceptance and then a second round trip.
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, c.changeError(ctx, waitCtx, atespace, name, err)
	}

	var task v1alpha1.Task
	if err := handle.Get(waitCtx, &task); err != nil {
		return nil, c.changeError(ctx, waitCtx, atespace, name, err)
	}
	return &task, nil
}

func (c *Client) changeError(ctx, waitCtx context.Context, atespace, name string, err error) error {
	if timedOut(ctx, waitCtx) {
		return fmt.Errorf("%w: no worker answered for task %s/%s", orchestration.ErrTaskUnavailable,
			atespaceOf(atespace), name)
	}
	return mapError(err)
}

// Delete tears a task's sandbox down and returns once it is gone. Acceptance
// is waited for under the bounded wait, so a task nobody answers for is
// reported unavailable; the teardown itself then runs for as long as it takes,
// which is what the synchronous control plane does too. A caller that gives up
// does not stop it: the workflow finishes the teardown on its own.
func (c *Client) Delete(ctx context.Context, atespace, name string) error {
	waitCtx, cancel := context.WithTimeout(ctx, c.changeWait)
	defer cancel()

	handle, err := c.client.UpdateWorkflow(waitCtx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName:   workflows.UpdateDelete,
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
	if err != nil {
		return c.changeError(ctx, waitCtx, atespace, name, err)
	}
	if err := handle.Get(ctx, nil); err != nil {
		return mapError(err)
	}
	return nil
}

func atespaceOf(atespace string) string {
	if atespace == "" {
		return v1alpha1.DefaultAtespace
	}
	return atespace
}

// mapError turns what Temporal reports into the errors the API server answers
// with. A task that has no workflow, and a task whose workflow has torn its
// sandbox down, are both gone.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %s", orchestration.ErrTaskNotFound, err)
	}
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &started) {
		return fmt.Errorf("%w: %s", orchestration.ErrTaskExists, err)
	}
	// Only a worker can answer for a task. When none does, the task exists but
	// nothing can speak for it, which is a different thing from it being gone.
	// A task queue with no pollers is reported as a failed precondition, which
	// is the shape of "worker may be down".
	var unavailable *serviceerror.Unavailable
	var deadline *serviceerror.DeadlineExceeded
	var notReady *serviceerror.WorkflowNotReady
	var precondition *serviceerror.FailedPrecondition
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &unavailable) ||
		errors.As(err, &deadline) ||
		errors.As(err, &notReady) ||
		errors.As(err, &precondition) {
		return fmt.Errorf("%w: %s", orchestration.ErrTaskUnavailable, err)
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case workflows.ErrTypeTaskDeleted:
			return fmt.Errorf("%w: %s", orchestration.ErrTaskNotFound, appErr.Message())
		case workflows.ErrTypeTaskExists:
			return fmt.Errorf("%w: %s", orchestration.ErrTaskExists, appErr.Message())
		case workflows.ErrTypeTaskTerminating:
			return fmt.Errorf("%w: %s", orchestration.ErrTaskTerminating, appErr.Message())
		case workflows.ErrTypeInvalidTask:
			return fmt.Errorf("%w: %s", orchestration.ErrInvalidTask, appErr.Message())
		}
	}
	return err
}
