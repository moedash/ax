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

// Package workflows holds the orchestration of an AX task: one workflow per
// task, for as long as the task exists.
//
// The workflow owns the task's lifecycle and its status. The API server starts
// it, sends it updates, and queries it; nothing else writes a task's state.
package workflows

import (
	"go.temporal.io/sdk/temporal"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// TaskWorkflowType is the registered name of TaskWorkflow. Visibility queries
// use it to find tasks.
const TaskWorkflowType = "TaskWorkflow"

// Update names. Every change to a task arrives as an update, so the caller
// learns whether the change was accepted and what it produced.
const (
	// UpdateApply sets the task's desired state, creating the task when it is
	// sent together with the workflow start.
	UpdateApply = "apply"
	// UpdateSuspend checkpoints the sandbox and stops it.
	UpdateSuspend = "suspend"
	// UpdateResume puts a suspended sandbox back on a worker.
	UpdateResume = "resume"
	// UpdateComplete records how the task command exited. The runner inside the
	// sandbox sends it.
	UpdateComplete = "complete"
	// UpdateDelete tears the sandbox down and ends the task.
	UpdateDelete = "delete"
)

// SignalComplete carries the same payload as UpdateComplete. The runner falls
// back to it when it is shutting down and cannot wait for an update to be
// accepted.
const SignalComplete = "complete"

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
	// ErrTypeTeardownFailed answers a delete whose sandbox could not be
	// released. The task is still there and still deletable.
	ErrTypeTeardownFailed = "TeardownFailed"
	// ErrTypeInvalidTask rejects an update whose task spec cannot be applied.
	ErrTypeInvalidTask = "InvalidTask"
	// ErrTypeUnsupportedVersion is returned when a task's recorded provisioning
	// version is no longer supported by this worker.
	ErrTypeUnsupportedVersion = "UnsupportedProvisioningVersion"
)

// Search attributes carry enough of a task in visibility to list tasks, and to
// find the tasks that bind a piece of configuration, without asking each task
// in turn. They have to exist in the namespace before a worker starts:
//
//	temporal operator search-attribute create \
//	  --name AxAtespace  --type Keyword \
//	  --name AxPhase     --type Keyword \
//	  --name AxGateway   --type Keyword \
//	  --name AxWorkspaces --type KeywordList
const (
	// AtespaceSearchAttribute carries a task's atespace.
	AtespaceSearchAttribute = "AxAtespace"
	// PhaseSearchAttribute carries status.phase, so a listing reads a task's
	// state from visibility rather than from the task itself.
	PhaseSearchAttribute = "AxPhase"
	// GatewaySearchAttribute carries the gateway a task binds, if any.
	GatewaySearchAttribute = "AxGateway"
	// WorkspacesSearchAttribute carries the workspaces a task binds.
	WorkspacesSearchAttribute = "AxWorkspaces"
)

// Typed handles for the search attributes above.
var (
	AtespaceKey   = temporal.NewSearchAttributeKeyKeyword(AtespaceSearchAttribute)
	PhaseKey      = temporal.NewSearchAttributeKeyKeyword(PhaseSearchAttribute)
	GatewayKey    = temporal.NewSearchAttributeKeyKeyword(GatewaySearchAttribute)
	WorkspacesKey = temporal.NewSearchAttributeKeyKeywordList(WorkspacesSearchAttribute)
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
// API server resolves the gateway and the workspaces the task binds and passes
// them in, so the worker never reads the configuration store.
type TaskDesiredState struct {
	Task       *v1alpha1.Task
	Gateway    *v1alpha1.Gateway
	Workspaces []*v1alpha1.Workspace
}

// TaskWorkflowInput starts a task workflow.
type TaskWorkflowInput struct {
	Desired *TaskDesiredState
	// Status carries the task's status across a continue-as-new boundary. It is
	// empty when a task is first created.
	Status *v1alpha1.TaskStatus
}

// CompleteInput reports how the task command finished.
type CompleteInput struct {
	// ExitCode is the command's exit status, or -1 when it was killed.
	ExitCode int32
	// Message is an optional note from the runner, such as why the command was
	// stopped.
	Message string
}
