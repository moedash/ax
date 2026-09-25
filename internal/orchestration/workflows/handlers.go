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

package workflows

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// registerHandlers wires the task's API surface. Queries answer questions about
// the task, updates change it. Both are registered before anything is
// provisioned, so a caller can always reach a task that exists.
func (r *taskRun) registerHandlers(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QueryTask, r.queryTask); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, QueryStatus, r.queryStatus); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateApply, r.handleApply,
		workflow.UpdateHandlerOptions{Validator: r.validateApply}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSuspend, r.handleSuspend,
		workflow.UpdateHandlerOptions{Validator: r.validateRunning}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateResume, r.handleResume,
		workflow.UpdateHandlerOptions{Validator: r.validateRunning}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateComplete, r.handleComplete,
		workflow.UpdateHandlerOptions{Validator: r.validateComplete}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateDelete, r.handleDelete,
		workflow.UpdateHandlerOptions{Validator: r.validateDelete}); err != nil {
		return err
	}

	// The runner reports a command exit as an update so that it learns the report
	// landed. When it is shutting down it cannot wait for that, so the same
	// payload is accepted as a signal and handled here.
	r.completions = workflow.GetSignalChannel(ctx, SignalComplete)
	workflow.Go(ctx, func(gctx workflow.Context) {
		for {
			var in CompleteInput
			if !r.completions.Receive(gctx, &in) {
				return
			}
			r.recordCompletion(gctx, in)
		}
	})
	return nil
}

// queryTask returns the whole task. A task that has been torn down still
// answers with the Terminating phase it ended on; the API server asks for the
// query to be rejected once the execution has closed, so nothing here has to
// say the task is gone.
func (r *taskRun) queryTask() (*v1alpha1.Task, error) {
	return r.taskSnapshot(), nil
}

func (r *taskRun) queryStatus() (*v1alpha1.TaskStatus, error) {
	return r.statusSnapshot(), nil
}

// handleApply replaces the task's desired state and answers with the task as it
// stands once the change has been driven into Substrate. Creating a task is the
// same operation, sent together with the workflow start.
func (r *taskRun) handleApply(ctx workflow.Context, desired *TaskDesiredState) (*v1alpha1.Task, error) {
	if err := r.adopt(ctx, desired); err != nil {
		return nil, err
	}
	if err := r.awaitHandled(ctx, r.request()); err != nil {
		return nil, err
	}
	return r.taskSnapshot(), nil
}

// validateApply rejects a spec the workflow could not act on, before it is
// written to history. Validators only read state.
func (r *taskRun) validateApply(ctx workflow.Context, desired *TaskDesiredState) error {
	if r.deleting {
		return taskTerminating(r.key())
	}
	if desired == nil || desired.Task == nil {
		return temporal.NewApplicationError("task is required", ErrTypeInvalidTask)
	}
	// A task's name is its identity. Once it has one, nothing else may be
	// applied over it; before then, the first apply is what gives it one.
	name := desired.Task.GetMetadata().GetName()
	if current := r.desired.GetTask().GetMetadata().GetName(); current != "" && name != current {
		return temporal.NewApplicationError(
			fmt.Sprintf("task %q cannot be applied to %s", name, r.key()), ErrTypeInvalidTask)
	}
	if err := v1alpha1.ValidateTask(desired.Task); err != nil {
		return temporal.NewApplicationError(err.Error(), ErrTypeInvalidTask)
	}
	return nil
}

// handleSuspend checkpoints the sandbox and stops it. Suspending a suspended
// task is accepted and changes nothing.
func (r *taskRun) handleSuspend(ctx workflow.Context) (*v1alpha1.Task, error) {
	r.desired.Task.Spec.Suspend = true
	if err := r.awaitHandled(ctx, r.request()); err != nil {
		return nil, err
	}
	return r.taskSnapshot(), nil
}

// handleResume puts a suspended sandbox back on a worker.
func (r *taskRun) handleResume(ctx workflow.Context) (*v1alpha1.Task, error) {
	r.desired.Task.Spec.Suspend = false
	if err := r.awaitHandled(ctx, r.request()); err != nil {
		return nil, err
	}
	return r.taskSnapshot(), nil
}

// validateRunning rejects changes to a task that is going away.
func (r *taskRun) validateRunning(ctx workflow.Context) error {
	if r.deleting {
		return taskTerminating(r.key())
	}
	return nil
}

// handleComplete records how the task command exited. Nothing has to be driven
// into Substrate, so the report is answered as soon as it is recorded.
func (r *taskRun) handleComplete(ctx workflow.Context, in CompleteInput) (*v1alpha1.TaskStatus, error) {
	r.recordCompletion(ctx, in)
	return r.statusSnapshot(), nil
}

// validateComplete rejects a report the task cannot use: one for a task that is
// going away, and one from a sandbox the task has since replaced.
func (r *taskRun) validateComplete(ctx workflow.Context, in CompleteInput) error {
	if r.deleting {
		return taskTerminating(r.key())
	}
	if in.Generation != r.generation {
		return temporal.NewApplicationError(
			fmt.Sprintf("task %s replaced sandbox generation %d with %d",
				r.key(), in.Generation, r.generation), ErrTypeStaleReport)
	}
	return nil
}

// handleDelete tears the sandbox down and ends the task. It returns once the
// record is gone, so a caller that waits for the update sees a task that no
// longer exists. A teardown that cannot finish answers with what was left
// behind, and the task stays.
func (r *taskRun) handleDelete(ctx workflow.Context) error {
	failures := r.teardownFailures
	r.deleting = true
	r.teardownFailed = false
	r.request()
	r.syncPhase(ctx)

	if err := workflow.Await(ctx, func() bool {
		return r.deleted || r.teardownFailures > failures
	}); err != nil {
		return err
	}
	if !r.deleted {
		return temporal.NewApplicationError(r.teardownMessage, ErrTypeTeardownFailed)
	}
	return nil
}

// validateDelete rejects a delete for a task that has already gone.
func (r *taskRun) validateDelete(ctx workflow.Context) error {
	if r.deleted {
		return temporal.NewApplicationError(
			fmt.Sprintf("task %s no longer exists", r.key()), ErrTypeTaskDeleted)
	}
	return nil
}

// request records that something asked for a change and returns the number the
// caller has to wait for.
func (r *taskRun) request() int {
	r.requests++
	return r.requests
}

// awaitHandled blocks until the main loop has driven the caller's request into
// Substrate, or until the task starts going away.
func (r *taskRun) awaitHandled(ctx workflow.Context, target int) error {
	return workflow.Await(ctx, func() bool { return r.handled >= target || r.deleting })
}

func taskTerminating(key string) error {
	return temporal.NewApplicationError(fmt.Sprintf("task %s is being deleted", key), ErrTypeTaskTerminating)
}
