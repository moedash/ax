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

// Package workflows holds the Temporal orchestration of an AX task: one
// workflow per task, for as long as the task exists.
//
// The workflow owns the task's lifecycle and its status. The API server starts
// it, sends it updates, and queries it; nothing else writes a task's state.
package workflows

import (
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// TaskWorkflowType is the registered name of TaskWorkflow. Visibility queries
// use it to find tasks.
const TaskWorkflowType = "TaskWorkflow"

// Update names. Every change to a task arrives as an update, so the caller
// learns whether the change was accepted and what it produced.
const (
	// UpdateApply sets the task's desired state. It travels with the workflow
	// start; tasks are immutable, so a later apply is refused.
	UpdateApply = "apply"
	// UpdateSuspend checkpoints the sandbox and stops it.
	UpdateSuspend = "suspend"
	// UpdateResume puts a suspended sandbox back on a worker.
	UpdateResume = "resume"
	// UpdateDelete tears the sandbox down and ends the task.
	UpdateDelete = "delete"
)

// Query names.
const (
	// QueryTask returns the whole task: metadata, spec, and status.
	QueryTask = "task"
	// QueryStatus returns the task's status on its own.
	QueryStatus = "status"
)

// Error types the workflow returns to its callers.
const (
	// ErrTypeTaskTerminating rejects updates to a task that is going away.
	ErrTypeTaskTerminating = "TaskTerminating"
	// ErrTypeTaskDeleted rejects a change to a task that has already gone.
	ErrTypeTaskDeleted = "TaskDeleted"
	// ErrTypeTaskExists rejects an apply to a task that already has its spec.
	ErrTypeTaskExists = "TaskExists"
	// ErrTypeTeardownFailed answers a delete whose sandbox could not be
	// released. The task is still there and still deletable.
	ErrTypeTeardownFailed = "TeardownFailed"
	// ErrTypeInvalidTask rejects an update whose task spec cannot be applied.
	ErrTypeInvalidTask = "InvalidTask"
)

// TaskWorkflowID returns the workflow ID of a task. It is the task's business
// key, so the API server can address a task without keeping a mapping and two
// callers cannot create the same task twice.
func TaskWorkflowID(atespace, name string) string {
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return atespace + "/" + name
}

// TaskDesiredState is everything the workflow needs to provision a task. The
// API server resolves the workspaces the task binds and passes them in, so the
// worker never reads the configuration store.
type TaskDesiredState struct {
	Task       *v1alpha1.Task
	Workspaces []*v1alpha1.Workspace
}

// TaskWorkflowInput starts a task workflow.
type TaskWorkflowInput struct {
	Desired *TaskDesiredState
	// Status carries the task's status across a continue-as-new boundary. It is
	// empty when a task is first created.
	Status *v1alpha1.TaskStatus
	// Suspended carries whether the task is meant to be stopped across a
	// continue-as-new boundary. A task is created suspended, and the suspend
	// and resume updates are the only things that change it.
	Suspended bool
	// Generation is the sandbox generation the task is on, carried across a
	// continue-as-new boundary so the next run names the same template and
	// accepts reports from the same sandbox.
	Generation int
	// Failed, TeardownFailed, and TeardownMessage carry what the status alone
	// cannot say across a continue-as-new boundary: why the task reports Failed,
	// and what a teardown that could not finish left behind.
	Failed          bool
	TeardownFailed  bool
	TeardownMessage string
}
