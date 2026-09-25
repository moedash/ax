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

package main

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

// Reporting a command exit must never take the sandbox down with it, however
// unreachable the control plane is.
func TestReportCommandExitSurvivesAnUnreachableControlPlane(t *testing.T) {
	t.Setenv(v1alpha1.EnvTemporalAddress, "127.0.0.1:1")
	t.Setenv(v1alpha1.EnvWorkflowID, "default/job")
	t.Setenv(v1alpha1.EnvTemporalNamespace, "default")

	done := make(chan struct{})
	go func() {
		defer close(done)
		reportCommandExit(runner.CommandExit{Pid: 42, ExitCode: 1})
	}()

	select {
	case <-done:
	case <-time.After(updateWait + signalWait + 5*time.Second):
		t.Fatal("reporting the command exit did not give up")
	}
}

// Without a workflow to report to, the exit is only logged.
func TestReportCommandExitWithoutAWorkflow(t *testing.T) {
	t.Setenv(v1alpha1.EnvTemporalAddress, "")
	t.Setenv(v1alpha1.EnvWorkflowID, "")

	reportCommandExit(runner.CommandExit{Pid: 42, ExitCode: 0})
}

// fakeControlPlane stands in for a Temporal frontend that is reachable but has
// no worker behind it: an update waits for its whole deadline, a signal lands.
// Embedding the client interface leaves everything else unimplemented on
// purpose; a test that reaches for it should say so by panicking.
type fakeControlPlane struct {
	client.Client

	updateErr error
	updates   int
	signals   []workflows.CompleteInput
	// signalCtxErr records the state of the signal's context when it arrived.
	signalCtxErr error
}

func (f *fakeControlPlane) UpdateWorkflow(
	ctx context.Context, options client.UpdateWorkflowOptions,
) (client.WorkflowUpdateHandle, error) {
	f.updates++
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeControlPlane) SignalWorkflow(
	ctx context.Context, workflowID, runID, signalName string, arg any,
) error {
	f.signalCtxErr = ctx.Err()
	in, _ := arg.(workflows.CompleteInput)
	f.signals = append(f.signals, in)
	return nil
}

// With the service reachable and no worker polling, the update holds the call
// until its deadline. The signal is what records the exit then, so it needs a
// deadline of its own.
func TestReportFallsBackToASignalWithItsOwnDeadline(t *testing.T) {
	fake := &fakeControlPlane{}
	r := exitReporter{
		client:     fake,
		workflowID: "default/job",
		updateWait: 20 * time.Millisecond,
		signalWait: time.Second,
	}
	in := workflows.CompleteInput{ExitCode: 2, Message: "exit status 2", Generation: 4}

	if err := r.report(in); err != nil {
		t.Fatalf("report failed: %v", err)
	}
	if fake.updates != 1 {
		t.Errorf("expected the update to be tried first, got %d updates", fake.updates)
	}
	if len(fake.signals) != 1 || fake.signals[0] != in {
		t.Fatalf("expected the same report as a signal, got %v", fake.signals)
	}
	if fake.signalCtxErr != nil {
		t.Errorf("expected the signal to be sent under a live deadline, got %v", fake.signalCtxErr)
	}
}

// A refused update is the workflow's answer, not a delivery problem, so no
// signal follows it.
func TestReportDoesNotSignalARefusedUpdate(t *testing.T) {
	fake := &fakeControlPlane{
		updateErr: temporal.NewApplicationError("replaced", workflows.ErrTypeStaleReport),
	}
	r := exitReporter{client: fake, workflowID: "default/job", updateWait: time.Second, signalWait: time.Second}

	if err := r.report(workflows.CompleteInput{ExitCode: 0}); err == nil {
		t.Fatal("expected the refusal to be reported")
	}
	if len(fake.signals) != 0 {
		t.Errorf("expected no signal after a refusal, got %v", fake.signals)
	}
}

// An exit the runner caused by stopping the command is not a completion: the
// sandbox is being suspended or stopped, and the task's work is not done.
func TestReportCommandExitSkipsAStoppedCommand(t *testing.T) {
	t.Setenv(v1alpha1.EnvTemporalAddress, "127.0.0.1:1")
	t.Setenv(v1alpha1.EnvWorkflowID, "default/job")

	start := time.Now()
	reportCommandExit(runner.CommandExit{Pid: 42, ExitCode: -1, Stopped: true})
	if time.Since(start) > time.Second {
		t.Error("a stopped command's exit must not be reported at all")
	}
}
