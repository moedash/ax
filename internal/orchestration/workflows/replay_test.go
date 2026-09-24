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

package workflows_test

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/internal/orchestration/taskclient"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/substrate/substratetest"
)

// historyFile holds a recorded execution of a task's whole life: provisioning,
// a suspend, a resume, a reported command exit, and a delete.
var historyFile = filepath.Join("testdata", "task_workflow_history.json")

// TestReplayRecordedHistory replays that recording against the current
// workflow code. It fails when a change to the workflow would break the tasks
// that are already running.
func TestReplayRecordedHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(workflows.DefaultConfig()),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, historyFile); err != nil {
		t.Fatalf("replaying %s: %v", historyFile, err)
	}
}

// TestRecordHistory re-records the history the replay test reads. It needs a
// Temporal server, so it only runs when one is offered:
//
//	temporal server start-dev --port 7466 --ui-port 8466 --headless
//	AX_RECORD_HISTORY_ADDRESS=localhost:7466 go test ./internal/orchestration/workflows/ -run TestRecordHistory
//
// The task is driven against a fake Agent Substrate, so the recording is of the
// orchestration and nothing else.
func TestRecordHistory(t *testing.T) {
	address := os.Getenv("AX_RECORD_HISTORY_ADDRESS")
	if address == "" {
		t.Skip("set AX_RECORD_HISTORY_ADDRESS to a Temporal server to re-record the replay history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A sandbox that reports its workspace ready right away, so the recording
	// does not wait out the readiness poll.
	sandbox := httpSandbox(t)

	control := substratetest.NewControlServer()
	control.WorkerIP = sandbox
	substrateClient, stop, err := substratetest.Start(control)
	if err != nil {
		t.Fatalf("starting the substrate fake: %v", err)
	}
	defer stop()

	temporalClient, err := sdkclient.Dial(sdkclient.Options{HostPort: address})
	if err != nil {
		t.Fatalf("connecting to %s: %v", address, err)
	}
	defer temporalClient.Close()
	ensureSearchAttributes(ctx, t, temporalClient)

	taskQueue := "ax-tasks-replay"
	w := worker.New(temporalClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(workflows.DefaultConfig()),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)
	w.RegisterActivity(&activities.Activities{
		Substrate:         substrateClient,
		ReportCompletion:  true,
		TemporalAddress:   address,
		TemporalNamespace: "default",
	})
	if err := w.Start(); err != nil {
		t.Fatalf("starting the worker: %v", err)
	}
	defer w.Stop()

	tasks := taskclient.New(temporalClient, taskQueue)
	in := testInput()
	name := in.Desired.Task.GetMetadata().GetName()

	if _, err := tasks.Apply(ctx, in.Desired); err != nil {
		t.Fatalf("applying the task: %v", err)
	}
	if _, err := tasks.Suspend(ctx, "default", name); err != nil {
		t.Fatalf("suspending the task: %v", err)
	}
	if _, err := tasks.Resume(ctx, "default", name); err != nil {
		t.Fatalf("resuming the task: %v", err)
	}

	// The runner inside the sandbox reports the command's exit.
	handle, err := temporalClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflows.TaskWorkflowID("default", name),
		UpdateName:   workflows.UpdateComplete,
		Args:         []any{workflows.CompleteInput{ExitCode: 0, Message: "agent finished"}},
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		t.Fatalf("reporting the command exit: %v", err)
	}
	if err := handle.Get(ctx, nil); err != nil {
		t.Fatalf("reporting the command exit: %v", err)
	}

	if err := tasks.Delete(ctx, "default", name); err != nil {
		t.Fatalf("deleting the task: %v", err)
	}
	run := temporalClient.GetWorkflow(ctx, workflows.TaskWorkflowID("default", name), "")
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("waiting for the task to finish: %v", err)
	}

	writeHistory(ctx, t, temporalClient, workflows.TaskWorkflowID("default", name), run.GetRunID())
}

// httpSandbox serves the readiness endpoint a sandbox would serve and returns
// its address.
func httpSandbox(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for the fake sandbox: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return listener.Addr().String()
}

// ensureSearchAttributes creates the attributes a task publishes about itself.
// A namespace that already has them is left alone.
func ensureSearchAttributes(ctx context.Context, t *testing.T, c sdkclient.Client) {
	t.Helper()
	_, err := c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: "default",
		SearchAttributes: map[string]enumspb.IndexedValueType{
			workflows.AtespaceSearchAttribute:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			workflows.PhaseSearchAttribute:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			workflows.GatewaySearchAttribute:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			workflows.WorkspacesSearchAttribute: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	})
	if err != nil {
		t.Logf("search attributes: %v", err)
	}
}

// writeHistory stores the execution's history in the form the replayer reads.
func writeHistory(ctx context.Context, t *testing.T, c sdkclient.Client, workflowID, runID string) {
	t.Helper()

	history := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("reading the workflow history: %v", err)
		}
		history.Events = append(history.Events, event)
	}

	data, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(history)
	if err != nil {
		t.Fatalf("encoding the workflow history: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(historyFile), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(historyFile), err)
	}
	if err := os.WriteFile(historyFile, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("writing %s: %v", historyFile, err)
	}
	t.Logf("recorded %d events to %s", len(history.Events), historyFile)
}
