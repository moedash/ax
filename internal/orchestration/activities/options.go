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

package activities

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Every timeout and retry setting the task workflow uses lives here, so the
// budget of a task is readable in one place.
const (
	// controlCallTimeout bounds one Substrate control call. The slowest of them,
	// creating an actor while a previous one finishes deleting, waits about ten
	// seconds inside the client.
	controlCallTimeout = time.Minute

	// provisionBudget bounds how long provisioning may keep retrying before the
	// task is rolled back and reported Failed. A cluster that cannot serve a
	// task within a quarter of an hour needs a human, not more retries.
	provisionBudget = 15 * time.Minute

	// resumeTimeout bounds placing an actor on a worker, which includes
	// restoring its snapshot.
	resumeTimeout = 5 * time.Minute

	// WorkspaceReadyTimeout bounds waiting for workspace setup inside a sandbox.
	// A maiden run clones repositories and installs skills, so it is generous;
	// expiry leaves the task running with WorkspaceReady False rather than
	// failing it.
	WorkspaceReadyTimeout = 10 * time.Minute

	// teardownBudget bounds how long deleting a task's Substrate resources may
	// keep retrying. Template deletion is rejected while the actor still exists,
	// so teardown has to outlast an actor that is slow to disappear.
	teardownBudget = 10 * time.Minute

	// heartbeatTimeout is the gap allowed between heartbeats of a long call. It
	// is well above heartbeatInterval so that a busy worker is not killed for
	// being late.
	heartbeatTimeout = time.Minute
)

// retryPolicy is shared by every activity: retry transient Substrate failures
// with exponential backoff, and give up immediately on the error types that no
// retry can fix.
func retryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:        time.Second,
		BackoffCoefficient:     2,
		MaximumInterval:        30 * time.Second,
		NonRetryableErrorTypes: []string{ErrTypeInvalidSpec, ErrTypePermanent},
	}
}

// ProvisionOptions configures the short control-plane calls that provision a
// task: the atespace, the actor template, the actor, and its egress policy.
func ProvisionOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    controlCallTimeout,
		ScheduleToCloseTimeout: provisionBudget,
		HeartbeatTimeout:       heartbeatTimeout,
		RetryPolicy:            retryPolicy(),
	}
}

// ResumeOptions configures placing an actor on a worker. The call blocks while
// Substrate restores the sandbox, so it reports liveness and can be cancelled.
func ResumeOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    resumeTimeout,
		ScheduleToCloseTimeout: provisionBudget,
		HeartbeatTimeout:       heartbeatTimeout,
		RetryPolicy:            retryPolicy(),
	}
}

// WorkspaceReadyOptions configures the readiness poll. StartToClose leaves room
// beyond the poll's own budget so that the activity reports not-ready itself
// instead of being timed out and restarted.
func WorkspaceReadyOptions(pollTimeout time.Duration) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: pollTimeout + controlCallTimeout,
		HeartbeatTimeout:    heartbeatTimeout,
		RetryPolicy:         retryPolicy(),
	}
}

// TeardownOptions configures removing a task's Substrate resources. Teardown
// also runs from a disconnected context after cancellation, where the sandbox
// has to be released whatever the workflow's own fate.
func TeardownOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    controlCallTimeout,
		ScheduleToCloseTimeout: teardownBudget,
		HeartbeatTimeout:       heartbeatTimeout,
		RetryPolicy:            retryPolicy(),
	}
}
