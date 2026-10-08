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

// Package orchestration defines the Temporal path of the AX control plane: the
// contract the API server uses to reach the workflow that owns a task when it
// runs with --orchestrator=temporal.
//
// The implementation lives in the taskclient subpackage; the workflow and its
// activities live in workflows and activities.
package orchestration

import (
	"context"
	"errors"

	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Errors the API server turns into status codes for its callers.
var (
	// ErrTaskNotFound reports a task the control plane does not have. A task
	// exists from the moment it is created until its sandbox has been torn down.
	ErrTaskNotFound = errors.New("task not found")
	// ErrTaskExists reports a create for a name that is already taken. Tasks
	// are immutable, so there is nothing a second create could mean.
	ErrTaskExists = errors.New("task already exists")
	// ErrTaskTerminating reports a change asked of a task that is going away.
	ErrTaskTerminating = errors.New("task is being deleted")
	// ErrInvalidTask reports a task spec the control plane refused.
	ErrInvalidTask = errors.New("invalid task")
	// ErrTaskUnavailable reports a task whose worker did not answer in time.
	// The task exists; nothing is running to speak for it right now.
	ErrTaskUnavailable = errors.New("task is unavailable")
	// ErrTaskChangePending reports a change a task's worker accepted but has
	// not finished within the wait. The workflow keeps working on it, so there
	// is nothing to ask again.
	ErrTaskChangePending = errors.New("task change is pending")
)

// Tasks reaches the workflow that owns each task. Tasks are addressed by their
// atespace and name, which is also the workflow's ID, so no mapping is kept
// anywhere.
type Tasks interface {
	// Create starts a task. A task is created suspended, so this answers with
	// the task once its sandbox has been built and checkpointed, or, when no
	// worker answers within the client's wait, with the task it accepted in the
	// Pending phase. A name that is already taken is ErrTaskExists.
	Create(ctx context.Context, desired *workflows.TaskDesiredState) (*v1alpha1.Task, error)
	// Get returns one task, or ErrTaskNotFound.
	Get(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
	// List returns the running task workflows of an atespace, or of every
	// atespace when it is empty. The per-task phase is not carried; Get has it.
	List(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error)
	// Suspend checkpoints a task's sandbox and stops it.
	Suspend(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
	// Resume puts a suspended sandbox back on a worker.
	Resume(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
	// Delete tears a task's sandbox down and returns once the task is gone. A
	// teardown that could not finish is reported as an error, and the task
	// stays, reported Failed, until a later delete succeeds.
	Delete(ctx context.Context, atespace, name string) error
}
