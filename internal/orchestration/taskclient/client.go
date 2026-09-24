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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// listPageSize is how many executions one visibility page carries.
const listPageSize = 100

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

	handle, err := c.client.UpdateWithStartWorkflow(ctx, sdkclient.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: sdkclient.UpdateWorkflowOptions{
			UpdateName:   workflows.UpdateApply,
			Args:         []any{desired},
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, mapError(err)
	}
	var task v1alpha1.Task
	if err := handle.Get(ctx, &task); err != nil {
		return nil, mapError(err)
	}
	return &task, nil
}

// Get returns one task as its workflow sees it.
func (c *Client) Get(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	value, err := c.client.QueryWorkflow(ctx, workflows.TaskWorkflowID(atespaceOf(atespace), name), "", workflows.QueryTask)
	if err != nil {
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
		id := execution.GetExecution().GetWorkflowId()
		value, err := c.client.QueryWorkflow(ctx, id, "", workflows.QueryTask)
		if err != nil {
			// A task that ended between the listing and the query is simply gone.
			if !errors.Is(mapError(err), orchestration.ErrTaskNotFound) {
				slog.Warn("could not read task state", "workflow", id, "error", err)
			}
			continue
		}
		var task v1alpha1.Task
		if err := value.Get(&task); err != nil {
			slog.Warn("could not decode task state", "workflow", id, "error", err)
			continue
		}
		tasks = append(tasks, &task)
		if int64(len(tasks)) == limit {
			break
		}
	}
	return tasks, nil
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

// change sends an update that answers with the task and waits for the answer.
func (c *Client) change(ctx context.Context, atespace, name, update string) (*v1alpha1.Task, error) {
	handle, err := c.client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName:   update,
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, mapError(err)
	}
	var task v1alpha1.Task
	if err := handle.Get(ctx, &task); err != nil {
		return nil, mapError(err)
	}
	return &task, nil
}

// Delete asks for a task's sandbox to be torn down. It waits only for the
// request to be accepted: tearing a sandbox down can take a while, and callers
// watch the task until it disappears.
func (c *Client) Delete(ctx context.Context, atespace, name string) error {
	_, err := c.client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID(atespaceOf(atespace), name),
		UpdateName:   workflows.UpdateDelete,
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
	return mapError(err)
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
