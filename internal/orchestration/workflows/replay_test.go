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
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/internal/substrate/substratetest"
)

// Recorded executions the replay tests read.
var (
	// historyFile holds a task's whole life: creation, a resume, a suspend,
	// another resume, a reported command exit, and a delete.
	historyFile = filepath.Join("testdata", "task_workflow_history.json")
	// resyncHistoryFile holds a task that sat through resync cycles until the
	// service suggested continuing as new, which is how every run but the last
	// of a long-lived task ends.
	resyncHistoryFile = filepath.Join("testdata", "task_workflow_resync_history.json")
)

// TestReplayRecordedHistory replays the recordings against the current
// workflow code. It fails when a change to the workflow would break the tasks
// that are already running.
//
// The replay runs under the recorded workflow ID. The workflow checks every
// spec against its own ID, and the replayer's stand-in ID would make it refuse
// the very apply the recording starts with.
func TestReplayRecordedHistory(t *testing.T) {
	for _, file := range []string{historyFile, resyncHistoryFile} {
		t.Run(filepath.Base(file), func(t *testing.T) {
			history := readHistory(t, file)
			started := history.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()

			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(
				workflows.NewTaskWorkflow(workflows.DefaultConfig()),
				workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
			)
			err := replayer.ReplayWorkflowHistoryWithOptions(nil, history,
				worker.ReplayWorkflowHistoryOptions{
					OriginalExecution: workflow.Execution{
						ID:    started.GetWorkflowId(),
						RunID: started.GetOriginalExecutionRunId(),
					},
				})
			if err != nil {
				t.Fatalf("replaying %s: %v", file, err)
			}
		})
	}
}

// readHistory loads a recording in the form the replayer reads.
func readHistory(t *testing.T, file string) *historypb.History {
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("opening %s: %v", file, err)
	}
	defer func() { _ = f.Close() }()
	history, err := sdkclient.HistoryFromJSON(f, sdkclient.HistoryJSONOptions{})
	if err != nil {
		t.Fatalf("decoding %s: %v", file, err)
	}
	if len(history.GetEvents()) == 0 {
		t.Fatalf("%s holds no events", file)
	}
	return history
}

// TestRecordHistory re-records the histories the replay tests read. It needs a
// Temporal server, so it only runs when one is offered. The server's
// continue-as-new suggestion is lowered so the second recording reaches it in
// seconds rather than days:
//
//	temporal server start-dev --port 7466 --ui-port 8466 --headless \
//	  --dynamic-config-value limit.historyCount.suggestContinueAsNew=150
//	AX_RECORD_HISTORY_ADDRESS=localhost:7466 \
//	  go test ./internal/orchestration/workflows/ -run TestRecordHistory
//
// The tasks are driven against a fake Agent Substrate, so the recordings are
// of the orchestration and nothing else.
func TestRecordHistory(t *testing.T) {
	address := os.Getenv("AX_RECORD_HISTORY_ADDRESS")
	if address == "" {
		t.Skip("set AX_RECORD_HISTORY_ADDRESS to a Temporal server to re-record the histories")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// A sandbox that reports its workspace ready right away, so the recordings
	// do not wait out the readiness poll.
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

	rec := recorder{
		ctx:       ctx,
		temporal:  temporalClient,
		substrate: substrateClient,
		address:   address,
	}
	t.Run("life", rec.recordLife)
	t.Run("resync", rec.recordResync)
}

// recorder drives one task against a dev server and writes its history down.
type recorder struct {
	ctx       context.Context
	temporal  sdkclient.Client
	substrate *substrate.Client
	address   string
}

// startWorker runs a worker on its own task queue for one recording.
func (r recorder) startWorker(t *testing.T, taskQueue string, cfg workflows.Config) {
	t.Helper()
	w := worker.New(r.temporal, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(cfg),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)
	w.RegisterActivity(&activities.Activities{
		Substrate:         r.substrate,
		ReportCompletion:  true,
		TemporalAddress:   r.address,
		TemporalNamespace: "default",
	})
	if err := w.Start(); err != nil {
		t.Fatalf("starting the worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// recordLife takes a task through creation, resume, suspend, another resume, a
// reported command exit, and delete. A task is created suspended, so the first
// resume is what puts it to work.
func (r recorder) recordLife(t *testing.T) {
	taskQueue := "ax-tasks-replay"
	r.startWorker(t, taskQueue, workflows.DefaultConfig())
	tasks := taskclient.New(r.temporal, taskQueue)
	in := testInput()
	name := in.Desired.Task.GetMetadata().GetName()
	workflowID := workflows.TaskWorkflowID("default", name)

	if _, err := tasks.Create(r.ctx, in.Desired); err != nil {
		t.Fatalf("creating the task: %v", err)
	}
	if _, err := tasks.Resume(r.ctx, "default", name); err != nil {
		t.Fatalf("resuming the task: %v", err)
	}
	if _, err := tasks.Suspend(r.ctx, "default", name); err != nil {
		t.Fatalf("suspending the task: %v", err)
	}
	if _, err := tasks.Resume(r.ctx, "default", name); err != nil {
		t.Fatalf("resuming the task again: %v", err)
	}

	// The runner inside the sandbox reports the command's exit.
	handle, err := r.temporal.UpdateWorkflow(r.ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateName:   workflows.UpdateComplete,
		Args:         []any{workflows.CompleteInput{ExitCode: 0, Message: "agent finished"}},
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		t.Fatalf("reporting the command exit: %v", err)
	}
	if err := handle.Get(r.ctx, nil); err != nil {
		t.Fatalf("reporting the command exit: %v", err)
	}

	if err := tasks.Delete(r.ctx, "default", name); err != nil {
		t.Fatalf("deleting the task: %v", err)
	}
	run := r.temporal.GetWorkflow(r.ctx, workflowID, "")
	if err := run.Get(r.ctx, nil); err != nil {
		t.Fatalf("waiting for the task to finish: %v", err)
	}
	writeHistory(r.ctx, t, r.temporal, workflowID, run.GetRunID(), historyFile)
}

// recordResync leaves a running task alone until its resync cycles have grown
// the history enough for the service to suggest continuing as new, then
// deletes it. The recording is of the first run, which ends in the
// continue-as-new.
func (r recorder) recordResync(t *testing.T) {
	taskQueue := "ax-tasks-replay-resync"
	r.startWorker(t, taskQueue, workflows.Config{ResyncInterval: 2 * time.Second})
	tasks := taskclient.New(r.temporal, taskQueue)
	in := testInput()
	in.Desired.Task.Metadata.Name = "resync-task"
	name := in.Desired.Task.GetMetadata().GetName()
	workflowID := workflows.TaskWorkflowID("default", name)

	if _, err := tasks.Create(r.ctx, in.Desired); err != nil {
		t.Fatalf("creating the task: %v", err)
	}
	if _, err := tasks.Resume(r.ctx, "default", name); err != nil {
		t.Fatalf("resuming the task: %v", err)
	}
	described, err := r.temporal.DescribeWorkflowExecution(r.ctx, workflowID, "")
	if err != nil {
		t.Fatalf("describing the task: %v", err)
	}
	firstRun := described.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// Wait for the first run to hand over to the next one.
	for {
		described, err := r.temporal.DescribeWorkflowExecution(r.ctx, workflowID, firstRun)
		if err != nil {
			t.Fatalf("describing the first run: %v", err)
		}
		status := described.GetWorkflowExecutionInfo().GetStatus()
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
			break
		}
		if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			t.Fatalf("the first run ended as %s rather than continuing as new; "+
				"is the server's limit.historyCount.suggestContinueAsNew lowered?", status)
		}
		select {
		case <-r.ctx.Done():
			t.Fatalf("the first run did not continue as new in time; " +
				"is the server's limit.historyCount.suggestContinueAsNew lowered?")
		case <-time.After(time.Second):
		}
	}

	if err := tasks.Delete(r.ctx, "default", name); err != nil {
		t.Fatalf("deleting the task: %v", err)
	}
	if err := r.temporal.GetWorkflow(r.ctx, workflowID, "").Get(r.ctx, nil); err != nil {
		t.Fatalf("waiting for the task to finish: %v", err)
	}
	writeHistory(r.ctx, t, r.temporal, workflowID, firstRun, resyncHistoryFile)
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
	_, err := c.OperatorService().
		AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
			Namespace: "default",
			SearchAttributes: map[string]enumspb.IndexedValueType{
				workflows.AtespaceSearchAttribute:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				workflows.PhaseSearchAttribute:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				workflows.WorkspacesSearchAttribute: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			},
		})
	if err != nil {
		t.Logf("search attributes: %v", err)
	}
}

// writeHistory stores one run's history in the form the replayer reads.
func writeHistory(
	ctx context.Context, t *testing.T, c sdkclient.Client, workflowID, runID, file string,
) {
	t.Helper()

	history := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(file), err)
	}
	if err := os.WriteFile(file, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("writing %s: %v", file, err)
	}
	t.Logf("recorded %d events to %s", len(history.Events), file)
}
