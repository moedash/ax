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

package taskclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeTemporal answers the handful of calls the task client makes. Embedding
// the client interface leaves everything else unimplemented on purpose: a test
// that reaches for it should say so by panicking.
type fakeTemporal struct {
	sdkclient.Client

	// startOptions and updateOptions record what the client asked for.
	startOptions  sdkclient.StartWorkflowOptions
	startWorkflow any
	updateOptions sdkclient.UpdateWorkflowOptions

	// updateResult is what a completed update answers with.
	updateResult any
	// blockUpdate holds the update open until the caller's context expires.
	blockUpdate bool

	// queryResult and queryErr answer QueryWorkflow.
	queryResult *v1alpha1.Task
	queryErr    error
	queries     int

	// describeStatus and describeErr answer DescribeWorkflowExecution.
	describeStatus enumspb.WorkflowExecutionStatus
	describeErr    error
	describes      int

	// executions is what ListWorkflow answers with, and listQueries records the
	// queries it was asked.
	executions  []string
	listQueries []string
}

func (f *fakeTemporal) NewWithStartWorkflowOperation(options sdkclient.StartWorkflowOptions, workflow any, args ...any) sdkclient.WithStartWorkflowOperation {
	f.startOptions = options
	f.startWorkflow = workflow
	return nil
}

func (f *fakeTemporal) UpdateWithStartWorkflow(ctx context.Context, options sdkclient.UpdateWithStartWorkflowOptions) (sdkclient.WorkflowUpdateHandle, error) {
	f.updateOptions = options.UpdateOptions
	return f.answerUpdate(ctx)
}

func (f *fakeTemporal) UpdateWorkflow(ctx context.Context, options sdkclient.UpdateWorkflowOptions) (sdkclient.WorkflowUpdateHandle, error) {
	f.updateOptions = options
	return f.answerUpdate(ctx)
}

func (f *fakeTemporal) answerUpdate(ctx context.Context) (sdkclient.WorkflowUpdateHandle, error) {
	if f.blockUpdate {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &fakeUpdateHandle{result: f.updateResult}, nil
}

func (f *fakeTemporal) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error) {
	f.queries++
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return encodedTask{task: f.queryResult}, nil
}

func (f *fakeTemporal) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.describes++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: f.describeStatus},
	}, nil
}

func (f *fakeTemporal) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.listQueries = append(f.listQueries, request.GetQuery())
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, id := range f.executions {
		resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		})
	}
	return resp, nil
}

type fakeUpdateHandle struct {
	result any
}

func (h *fakeUpdateHandle) WorkflowID() string { return "" }
func (h *fakeUpdateHandle) RunID() string      { return "" }
func (h *fakeUpdateHandle) UpdateID() string   { return "" }

func (h *fakeUpdateHandle) Get(ctx context.Context, valuePtr any) error {
	if valuePtr == nil || h.result == nil {
		return nil
	}
	task, ok := h.result.(*v1alpha1.Task)
	if !ok {
		return errors.New("unexpected update result")
	}
	return assignTask(valuePtr, task)
}

// encodedTask stands in for the encoded value a query answers with.
type encodedTask struct {
	task *v1alpha1.Task
}

func (e encodedTask) HasValue() bool { return e.task != nil }

func (e encodedTask) Get(valuePtr any) error {
	return assignTask(valuePtr, e.task)
}

func assignTask(valuePtr any, task *v1alpha1.Task) error {
	out, ok := valuePtr.(*v1alpha1.Task)
	if !ok {
		return errors.New("expected a task")
	}
	if task == nil {
		return nil
	}
	proto.Reset(out)
	proto.Merge(out, task)
	return nil
}

func newTestClient(fake *fakeTemporal) *Client {
	c := New(fake, "ax-tasks")
	// The waits are what the tests are about, not how long they take.
	c.changeWait = 50 * time.Millisecond
	c.queryWait = 50 * time.Millisecond
	return c
}

func testDesired() *workflows.TaskDesiredState {
	return &workflows.TaskDesiredState{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/example/agent"},
		},
	}
}

func runningTask() *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team-a"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/example/agent"},
		Status:   &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning, WorkerIp: "10.0.0.1"},
	}
}

// Creating and updating a task are the same call, and the policies on it are
// what decide which of the two happens.
func TestApplySendsTheUpdateWithAStart(t *testing.T) {
	fake := &fakeTemporal{updateResult: runningTask()}
	client := newTestClient(fake)

	task, err := client.Apply(context.Background(), testDesired())
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseRunning {
		t.Errorf("expected the task the update answered with, got %v", task.GetStatus())
	}

	if got := fake.startOptions.ID; got != "team-a/job" {
		t.Errorf("expected the workflow ID to be the task's key, got %q", got)
	}
	if got := fake.startOptions.TaskQueue; got != "ax-tasks" {
		t.Errorf("expected task queue ax-tasks, got %q", got)
	}
	if got := fake.startOptions.WorkflowIDConflictPolicy; got != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		t.Errorf("a running task has to take the update, got conflict policy %v", got)
	}
	if got := fake.startOptions.WorkflowIDReusePolicy; got != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		t.Errorf("a deleted task has to be creatable again, got reuse policy %v", got)
	}
	if got := fake.startWorkflow; got != workflows.TaskWorkflowType {
		t.Errorf("expected the workflow type, got %v", got)
	}
	atespace, ok := fake.startOptions.TypedSearchAttributes.GetKeyword(workflows.AtespaceKey)
	if !ok || atespace != "team-a" {
		t.Errorf("expected the atespace search attribute, got %q (set=%v)", atespace, ok)
	}
	if fake.updateOptions.UpdateName != workflows.UpdateApply {
		t.Errorf("expected the apply update, got %q", fake.updateOptions.UpdateName)
	}
	if fake.updateOptions.WaitForStage != sdkclient.WorkflowUpdateStageAccepted {
		t.Errorf("expected to wait for acceptance, got %v", fake.updateOptions.WaitForStage)
	}
}

// Only a worker applies a change, so a control plane with no workers answers
// with the task it accepted rather than holding the caller.
func TestApplyAnswersPendingWhenNoWorkerAccepts(t *testing.T) {
	fake := &fakeTemporal{
		blockUpdate:    true,
		describeStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	client := newTestClient(fake)

	task, err := client.Apply(context.Background(), testDesired())
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhasePending {
		t.Errorf("expected a Pending task, got %v", task.GetStatus())
	}
	if task.GetMetadata().GetName() != "job" {
		t.Errorf("expected the task that was accepted, got %v", task.GetMetadata())
	}
	if fake.describes != 1 {
		t.Errorf("expected the execution to be checked once, got %d", fake.describes)
	}
}

// An expired wait says nothing about whether the task was created, so it is
// only reported as accepted when the execution is really there.
func TestApplyReportsUnavailableWhenNothingWasStarted(t *testing.T) {
	fake := &fakeTemporal{
		blockUpdate: true,
		describeErr: serviceerror.NewNotFound("no such workflow"),
	}
	client := newTestClient(fake)

	_, err := client.Apply(context.Background(), testDesired())
	if !errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskUnavailable, err)
	}
}

func TestApplyRejectsATaskWithNoName(t *testing.T) {
	client := newTestClient(&fakeTemporal{})
	desired := testDesired()
	desired.Task.Metadata.Name = ""

	_, err := client.Apply(context.Background(), desired)
	if !errors.Is(err, orchestration.ErrInvalidTask) {
		t.Fatalf("expected %v, got %v", orchestration.ErrInvalidTask, err)
	}
}

func TestGetReturnsARunningTask(t *testing.T) {
	fake := &fakeTemporal{queryResult: runningTask()}
	client := newTestClient(fake)

	task, err := client.Get(context.Background(), "team-a", "job")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseRunning {
		t.Errorf("expected a running task, got %v", task.GetStatus())
	}
	if fake.describes != 0 {
		t.Error("a task that is not terminating needs no second call")
	}
}

// A workflow answers queries after it has closed, so a task that reports
// terminating is only gone once its execution has ended.
func TestGetMapsAFinishedTaskToNotFound(t *testing.T) {
	terminating := runningTask()
	terminating.Status.Phase = v1alpha1.PhaseTerminating

	fake := &fakeTemporal{
		queryResult:    terminating,
		describeStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}
	client := newTestClient(fake)

	_, err := client.Get(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskNotFound) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskNotFound, err)
	}

	// The same task while its sandbox is still being torn down is still there.
	fake.describeStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	task, err := client.Get(context.Background(), "team-a", "job")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Errorf("expected a terminating task, got %v", task.GetStatus())
	}
}

func TestGetMapsAMissingWorkflowToNotFound(t *testing.T) {
	client := newTestClient(&fakeTemporal{queryErr: serviceerror.NewNotFound("no such workflow")})

	_, err := client.Get(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskNotFound) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskNotFound, err)
	}
}

// A task queue with no pollers is reported as a failed precondition, which is
// the shape of "worker may be down".
func TestGetMapsNoPollerToUnavailable(t *testing.T) {
	client := newTestClient(&fakeTemporal{
		queryErr: serviceerror.NewFailedPrecondition("no poller seen for task queue recently, worker may be down"),
	})

	_, err := client.Get(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskUnavailable, err)
	}
}

func TestSuspendReportsUnavailableWhenNoWorkerAnswers(t *testing.T) {
	client := newTestClient(&fakeTemporal{blockUpdate: true})

	_, err := client.Suspend(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskUnavailable, err)
	}
}

func TestSuspendAnswersWithTheTask(t *testing.T) {
	suspended := runningTask()
	suspended.Status.Phase = v1alpha1.PhaseSuspended
	fake := &fakeTemporal{updateResult: suspended}
	client := newTestClient(fake)

	task, err := client.Suspend(context.Background(), "team-a", "job")
	if err != nil {
		t.Fatalf("Suspend failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseSuspended {
		t.Errorf("expected a suspended task, got %v", task.GetStatus())
	}
	if fake.updateOptions.WorkflowID != "team-a/job" {
		t.Errorf("expected the task's workflow, got %q", fake.updateOptions.WorkflowID)
	}
	if fake.updateOptions.UpdateName != workflows.UpdateSuspend {
		t.Errorf("expected the suspend update, got %q", fake.updateOptions.UpdateName)
	}
}

// The listing query is built rather than interpolated, so a value cannot steer
// it whatever validation upstream does.
func TestListQuery(t *testing.T) {
	all := listQuery("")
	if !strings.Contains(all, "WorkflowType = 'TaskWorkflow'") || !strings.Contains(all, "ExecutionStatus = 'Running'") {
		t.Errorf("unexpected query for every atespace: %q", all)
	}
	if strings.Contains(all, workflows.AtespaceSearchAttribute) {
		t.Errorf("expected no atespace filter, got %q", all)
	}

	one := listQuery("team-a")
	if !strings.Contains(one, "AxAtespace = 'team-a'") {
		t.Errorf("expected an atespace filter, got %q", one)
	}

	quoted := listQuery("team' OR '1'='1")
	if !strings.Contains(quoted, "AxAtespace = 'team'' OR ''1''=''1'") {
		t.Errorf("expected the value to be quoted, got %q", quoted)
	}
}

// A task nothing answers for is still listed, with what visibility knows.
func TestListFallsBackToVisibility(t *testing.T) {
	fake := &fakeTemporal{
		executions: []string{"team-a/job"},
		queryErr:   serviceerror.NewFailedPrecondition("no poller seen for task queue recently, worker may be down"),
	}
	client := newTestClient(fake)

	tasks, err := client.List(context.Background(), "team-a", 50, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].GetMetadata().GetName() != "job" || tasks[0].GetMetadata().GetAtespace() != "team-a" {
		t.Errorf("expected the name and atespace from the workflow ID, got %v", tasks[0].GetMetadata())
	}
	if tasks[0].GetStatus().GetPhase() != "" {
		t.Errorf("expected no status for a task that could not be asked, got %q", tasks[0].GetStatus().GetPhase())
	}
}

// A task that ended between the listing and the query is left out.
func TestListLeavesOutATaskThatIsGone(t *testing.T) {
	fake := &fakeTemporal{
		executions: []string{"team-a/job"},
		queryErr:   serviceerror.NewNotFound("no such workflow"),
	}
	client := newTestClient(fake)

	tasks, err := client.List(context.Background(), "team-a", 50, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no tasks, got %d", len(tasks))
	}
}
