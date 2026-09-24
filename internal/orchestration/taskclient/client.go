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
// changes are updates, and listing goes through visibility.
package taskclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
}

// New returns a client that starts task workflows on the given task queue.
func New(c sdkclient.Client, taskQueue string) *Client {
	return &Client{client: c, taskQueue: taskQueue}
}

// Apply creates or updates a task. Creating and updating are the same call: the
// update is sent together with a start, so a task that does not exist yet is
// started and a task that is already running takes the update instead.
func (c *Client) Apply(ctx context.Context, desired *workflows.TaskDesiredState) (*v1alpha1.Task, error) {
	if desired == nil || desired.Task.GetMetadata().GetName() == "" {
		return nil, fmt.Errorf("%w: task name is required", orchestration.ErrInvalidTask)
	}
	atespace := atespaceOf(desired.Task.GetMetadata().GetAtespace())
	desired.Task.GetMetadata().Atespace = atespace

	start := c.client.NewWithStartWorkflowOperation(sdkclient.StartWorkflowOptions{
		ID:        workflows.TaskWorkflowID(atespace, desired.Task.GetMetadata().GetName()),
		TaskQueue: c.taskQueue,
		// A task that is still running takes the update. A task whose workflow has
		// ended, because it was deleted, is created again under the same name.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		TypedSearchAttributes:    temporal.NewSearchAttributes(workflows.AtespaceKey.ValueSet(atespace)),
	}, workflows.TaskWorkflow, workflows.TaskWorkflowInput{Desired: desired})

	// An update is only accepted by a worker, so the wait is bounded: the task
	// has been created either way, and a control plane with no workers must not
	// hold the caller.
	waitCtx, cancel := context.WithTimeout(ctx, changeWait)
	defer cancel()

	handle, err := c.client.UpdateWithStartWorkflow(waitCtx, sdkclient.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: sdkclient.UpdateWorkflowOptions{
			UpdateName:   workflows.UpdateApply,
			Args:         []any{desired},
			WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
		},
	})
	if err != nil {
		if timedOut(ctx, waitCtx) {
			return acceptedTask(desired.Task), nil
		}
		return nil, mapError(err)
	}

	var task v1alpha1.Task
	if err := handle.Get(waitCtx, &task); err != nil {
		if timedOut(ctx, waitCtx) {
			return acceptedTask(desired.Task), nil
		}
		return nil, mapError(err)
	}
	return &task, nil
}

// acceptedTask is what a caller is told about a task that has been created but
// whose workflow has not reported back yet.
func acceptedTask(task *v1alpha1.Task) *v1alpha1.Task {
	out, ok := proto.Clone(task).(*v1alpha1.Task)
	if !ok {
		return task
	}
	out.ApiVersion = v1alpha1.APIVersion
	out.Kind = v1alpha1.KindTask
	out.Status = &v1alpha1.TaskStatus{Phase: v1alpha1.PhasePending}
	return out
}

// timedOut reports whether the bounded wait expired while the caller is still
// waiting, as opposed to the caller giving up.
func timedOut(ctx, waitCtx context.Context) bool {
	return ctx.Err() == nil && waitCtx.Err() != nil
}

// Get returns one task as its workflow sees it. Only a worker can answer for a
// task, so the wait is bounded and a task nothing answers for is reported
// unavailable rather than left hanging.
func (c *Client) Get(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	queryCtx, cancel := context.WithTimeout(ctx, queryWait)
	defer cancel()

	value, err := c.client.QueryWorkflow(queryCtx, workflows.TaskWorkflowID(atespaceOf(atespace), name), "", workflows.QueryTask)
	if err != nil {
		if timedOut(ctx, queryCtx) {
			return nil, fmt.Errorf("%w: no worker answered for task %s/%s", orchestration.ErrTaskUnavailable, atespaceOf(atespace), name)
		}
		return nil, mapError(err)
	}
	var task v1alpha1.Task
	if err := value.Get(&task); err != nil {
		return nil, fmt.Errorf("decoding task %s/%s: %w", atespace, name, err)
	}
	return &task, nil
}

// List returns the tasks of an atespace. Visibility answers which tasks exist,
// and each one is then asked for its own state.
func (c *Client) List(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
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

	tasks := make([]*v1alpha1.Task, 0, len(executions))
	for _, execution := range executions {
		task, ok := c.describe(ctx, execution)
		if !ok {
			continue
		}
		tasks = append(tasks, task)
		if int64(len(tasks)) == limit {
			break
		}
	}
	return tasks, nil
}

// describe asks one listed task for its state. A task that has ended in the
// meantime is left out; a task whose worker does not answer is listed with what
// visibility knows about it, so a listing stays useful when workers are down.
func (c *Client) describe(ctx context.Context, execution *workflowpb.WorkflowExecutionInfo) (*v1alpha1.Task, bool) {
	id := execution.GetExecution().GetWorkflowId()

	queryCtx, cancel := context.WithTimeout(ctx, queryWait)
	defer cancel()

	value, err := c.client.QueryWorkflow(queryCtx, id, "", workflows.QueryTask)
	if err != nil {
		if errors.Is(mapError(err), orchestration.ErrTaskNotFound) {
			return nil, false
		}
		slog.Warn("could not read task state", "workflow", id, "error", err)
		return listedTask(id), true
	}
	var task v1alpha1.Task
	if err := value.Get(&task); err != nil {
		slog.Warn("could not decode task state", "workflow", id, "error", err)
		return listedTask(id), true
	}
	return &task, true
}

// listedTask is what a listing shows for a task that exists but cannot speak
// for itself. Its workflow ID is its atespace and name.
func listedTask(workflowID string) *v1alpha1.Task {
	atespace, name, found := strings.Cut(workflowID, "/")
	if !found {
		atespace, name = v1alpha1.DefaultAtespace, workflowID
	}
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: name, Atespace: atespace},
		Status:     &v1alpha1.TaskStatus{},
	}
}

// listExecutions walks visibility until it has at least want executions.
func (c *Client) listExecutions(ctx context.Context, atespace string, want int64) ([]*workflowpb.WorkflowExecutionInfo, error) {
	query := fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'", workflows.TaskWorkflowType)
	if atespace != "" && atespace != "*" {
		query += fmt.Sprintf(" AND %s = '%s'", workflows.AtespaceSearchAttribute, atespaceOf(atespace))
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
		out = append(out, resp.GetExecutions()...)
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
func (c *Client) change(ctx context.Context, atespace, name, update string) (*v1alpha1.Task, error) {
	waitCtx, cancel := context.WithTimeout(ctx, changeWait)
	defer cancel()

	handle, err := c.client.UpdateWorkflow(waitCtx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName:   update,
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
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
		return fmt.Errorf("%w: no worker answered for task %s/%s", orchestration.ErrTaskUnavailable, atespaceOf(atespace), name)
	}
	return mapError(err)
}

// Delete asks for a task's sandbox to be torn down. It waits only for the
// request to be accepted: tearing a sandbox down can take a while, and callers
// watch the task until it disappears.
func (c *Client) Delete(ctx context.Context, atespace, name string) error {
	waitCtx, cancel := context.WithTimeout(ctx, changeWait)
	defer cancel()

	_, err := c.client.UpdateWorkflow(waitCtx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName:   workflows.UpdateDelete,
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
	if err != nil {
		return c.changeError(ctx, waitCtx, atespace, name, err)
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
// sandbox down, are both simply gone.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %s", orchestration.ErrTaskNotFound, err)
	}
	// Only a worker can answer for a task. When none does, the task exists but
	// nothing can speak for it, which is a different thing from it being gone.
	var unavailable *serviceerror.Unavailable
	var deadline *serviceerror.DeadlineExceeded
	var notReady *serviceerror.WorkflowNotReady
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &unavailable) ||
		errors.As(err, &deadline) ||
		errors.As(err, &notReady) {
		return fmt.Errorf("%w: %s", orchestration.ErrTaskUnavailable, err)
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case workflows.ErrTypeTaskDeleted:
			return fmt.Errorf("%w: %s", orchestration.ErrTaskNotFound, appErr.Message())
		case workflows.ErrTypeTaskTerminating:
			return fmt.Errorf("%w: %s", orchestration.ErrTaskTerminating, appErr.Message())
		case workflows.ErrTypeInvalidTask:
			return fmt.Errorf("%w: %s", orchestration.ErrInvalidTask, appErr.Message())
		}
	}
	return err
}
