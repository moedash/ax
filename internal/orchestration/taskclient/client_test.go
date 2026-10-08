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
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	// updateResult is what a completed update answers with, and updateErr is
	// what a refused one fails with.
	updateResult any
	updateErr    error
	// blockUpdate holds the update open until the caller's context expires.
	blockUpdate bool
	// blockResult accepts the update but holds its answer until the caller's
	// context expires, which is a worker still carrying the change out.
	blockResult bool

	// queryResult and queryErr answer QueryWorkflowWithOptions. queryRejected
	// stands in for the service rejecting the query because the run is closed,
	// and rejectCondition records what the client asked for.
	queryResult     *v1alpha1.Task
	queryErr        error
	queryRejected   bool
	rejectCondition enumspb.QueryRejectCondition
	queries         int

	// describeStatus and describeErr answer DescribeWorkflowExecution.
	describeStatus enumspb.WorkflowExecutionStatus
	describeErr    error
	describes      int

	// executions is what ListWorkflow answers with, and listQueries records the
	// queries it was asked.
	executions  []listedExecution
	listQueries []string
}

func (f *fakeTemporal) NewWithStartWorkflowOperation(
	options sdkclient.StartWorkflowOptions,
	workflow any,
	args ...any,
) sdkclient.WithStartWorkflowOperation {
	f.startOptions = options
	f.startWorkflow = workflow
	return nil
}

func (f *fakeTemporal) UpdateWithStartWorkflow(
	ctx context.Context,
	options sdkclient.UpdateWithStartWorkflowOptions,
) (sdkclient.WorkflowUpdateHandle, error) {
	f.updateOptions = options.UpdateOptions
	return f.answerUpdate(ctx)
}

func (f *fakeTemporal) UpdateWorkflow(
	ctx context.Context,
	options sdkclient.UpdateWorkflowOptions,
) (sdkclient.WorkflowUpdateHandle, error) {
	f.updateOptions = options
	return f.answerUpdate(ctx)
}

func (f *fakeTemporal) answerUpdate(ctx context.Context) (sdkclient.WorkflowUpdateHandle, error) {
	if f.blockUpdate {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return &fakeUpdateHandle{result: f.updateResult, block: f.blockResult}, nil
}

func (f *fakeTemporal) QueryWorkflowWithOptions(
	ctx context.Context, request *sdkclient.QueryWorkflowWithOptionsRequest,
) (*sdkclient.QueryWorkflowWithOptionsResponse, error) {
	f.queries++
	f.rejectCondition = request.QueryRejectCondition
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if f.queryRejected {
		return &sdkclient.QueryWorkflowWithOptionsResponse{
			QueryRejected: &querypb.QueryRejected{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		}, nil
	}
	return &sdkclient.QueryWorkflowWithOptionsResponse{
		QueryResult: encodedTask{task: f.queryResult},
	}, nil
}

func (f *fakeTemporal) DescribeWorkflowExecution(
	ctx context.Context,
	workflowID, runID string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.describes++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: f.describeStatus},
	}, nil
}

func (f *fakeTemporal) ListWorkflow(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.listQueries = append(f.listQueries, request.GetQuery())
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, execution := range f.executions {
		resp.Executions = append(resp.Executions, execution.info())
	}
	return resp, nil
}

// listedExecution is one row of visibility. A listing reads a task's identity
// from its workflow ID, so that is all a row needs to carry.
type listedExecution struct {
	id      string
	started time.Time
}

func (e listedExecution) info() *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: e.id},
		StartTime: timestamppb.New(e.started),
	}
}

type fakeUpdateHandle struct {
	result any
	block  bool
}

func (h *fakeUpdateHandle) WorkflowID() string { return "" }
func (h *fakeUpdateHandle) RunID() string      { return "" }
func (h *fakeUpdateHandle) UpdateID() string   { return "" }

func (h *fakeUpdateHandle) Get(ctx context.Context, valuePtr any) error {
	if h.block {
		<-ctx.Done()
		return ctx.Err()
	}
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

// The spec travels as an update sent together with the start, and the policies
// on the start are what make a taken name a refusal and a deleted task
// creatable again.
func TestCreateSendsTheUpdateWithAStart(t *testing.T) {
	suspended := runningTask()
	suspended.Status = &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseSuspended}
	fake := &fakeTemporal{updateResult: suspended}
	client := newTestClient(fake)

	task, err := client.Create(context.Background(), testDesired())
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseSuspended {
		t.Errorf("expected the task the update answered with, got %v", task.GetStatus())
	}

	if got := fake.startOptions.ID; got != "team-a/job" {
		t.Errorf("expected the workflow ID to be the task's key, got %q", got)
	}
	if got := fake.startOptions.TaskQueue; got != "ax-tasks" {
		t.Errorf("expected task queue ax-tasks, got %q", got)
	}
	conflict := fake.startOptions.WorkflowIDConflictPolicy
	if conflict != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
		t.Errorf("a name that is taken has to be refused, got conflict policy %v", conflict)
	}
	reuse := fake.startOptions.WorkflowIDReusePolicy
	if reuse != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		t.Errorf("a deleted task has to be creatable again, got reuse policy %v", reuse)
	}
	if got := fake.startWorkflow; got != workflows.TaskWorkflowType {
		t.Errorf("expected the workflow type, got %v", got)
	}
	if fake.updateOptions.UpdateName != workflows.UpdateApply {
		t.Errorf("expected the apply update, got %q", fake.updateOptions.UpdateName)
	}
	if fake.updateOptions.WaitForStage != sdkclient.WorkflowUpdateStageCompleted {
		t.Errorf("expected to wait for the result, got %v", fake.updateOptions.WaitForStage)
	}
}

// Tasks are immutable, so a name that is already running is refused. The
// service says so when the start is turned away, and the workflow says so when
// a spec reaches a task that already has one.
func TestCreateReportsATakenName(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "the start is refused",
			err: serviceerror.NewWorkflowExecutionAlreadyStarted("already started",
				"req-1", "run-1"),
		},
		{
			name: "the apply is refused",
			err: temporal.NewApplicationError("task team-a/job already exists and is immutable",
				workflows.ErrTypeTaskExists),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(&fakeTemporal{updateErr: tc.err})

			_, err := client.Create(context.Background(), testDesired())
			if !errors.Is(err, orchestration.ErrTaskExists) {
				t.Fatalf("expected %v, got %v", orchestration.ErrTaskExists, err)
			}
		})
	}
}

// Only a worker applies a change, so a control plane with no workers answers
// with the task it accepted rather than holding the caller.
func TestCreateAnswersPendingWhenNoWorkerAccepts(t *testing.T) {
	fake := &fakeTemporal{
		blockUpdate:    true,
		describeStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	client := newTestClient(fake)

	task, err := client.Create(context.Background(), testDesired())
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhasePending {
		t.Errorf("expected a Pending task, got %v", task.GetStatus())
	}
	if task.GetMetadata().GetName() != "job" {
		t.Errorf("expected the task that was accepted, got %v", task.GetMetadata())
	}
	if task.GetMetadata().GetCreationTimestamp() != nil {
		t.Error("the workflow owns the creation time and has not answered yet")
	}
	if fake.describes != 1 {
		t.Errorf("expected the execution to be checked once, got %d", fake.describes)
	}
}

// An expired wait says nothing about whether the task was created, so it is
// only reported as accepted when the execution is really there.
func TestCreateReportsUnavailableWhenNothingWasStarted(t *testing.T) {
	fake := &fakeTemporal{
		blockUpdate: true,
		describeErr: serviceerror.NewNotFound("no such workflow"),
	}
	client := newTestClient(fake)

	_, err := client.Create(context.Background(), testDesired())
	if !errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskUnavailable, err)
	}
}

// A task that was deleted leaves a closed run under the same ID. Finding it
// says nothing about whether a new start landed, so it is not accepted either.
func TestCreateDoesNotTakeAClosedRunAsAccepted(t *testing.T) {
	fake := &fakeTemporal{
		blockUpdate:    true,
		describeStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}
	client := newTestClient(fake)

	_, err := client.Create(context.Background(), testDesired())
	if !errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskUnavailable, err)
	}
}

func TestCreateRejectsATaskWithNoName(t *testing.T) {
	client := newTestClient(&fakeTemporal{})
	desired := testDesired()
	desired.Task.Metadata.Name = ""

	_, err := client.Create(context.Background(), desired)
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
	if fake.rejectCondition != enumspb.QUERY_REJECT_CONDITION_NOT_OPEN {
		t.Errorf("expected the query to be rejected for a closed run, got %v", fake.rejectCondition)
	}
	if fake.describes != 0 {
		t.Error("one query answers for a task; no second call is needed")
	}
}

// A workflow answers queries after it has closed, so the client asks for the
// query to be rejected on a closed run. A task whose run closed is gone,
// whatever phase it reported last and whatever closed it.
func TestGetMapsAClosedRunToNotFound(t *testing.T) {
	fake := &fakeTemporal{queryResult: runningTask(), queryRejected: true}
	client := newTestClient(fake)

	_, err := client.Get(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskNotFound) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskNotFound, err)
	}
	if fake.describes != 0 {
		t.Error("the rejection says the run is closed; nothing else has to be asked")
	}
}

// A task whose sandbox is still being torn down is still there.
func TestGetReturnsATerminatingTask(t *testing.T) {
	terminating := runningTask()
	terminating.Status.Phase = v1alpha1.PhaseTerminating
	client := newTestClient(&fakeTemporal{queryResult: terminating})

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
		queryErr: serviceerror.NewFailedPrecondition(
			"no poller seen for task queue recently, worker may be down"),
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

// A worker that took the change and is still carrying it out is a different
// answer from no worker at all. The change keeps going, so it is pending, not
// unavailable.
func TestSuspendReportsPendingWhenTheChangeIsStillRunning(t *testing.T) {
	client := newTestClient(&fakeTemporal{blockResult: true})

	_, err := client.Suspend(context.Background(), "team-a", "job")
	if !errors.Is(err, orchestration.ErrTaskChangePending) {
		t.Fatalf("expected %v, got %v", orchestration.ErrTaskChangePending, err)
	}
	if errors.Is(err, orchestration.ErrTaskUnavailable) {
		t.Errorf("a change the worker accepted is not unavailable: %v", err)
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
	if fake.updateOptions.WaitForStage != sdkclient.WorkflowUpdateStageAccepted {
		t.Errorf("expected to wait for acceptance first, got %v", fake.updateOptions.WaitForStage)
	}
}

// The listing query is built rather than interpolated, so a value cannot steer
// it. The atespace is matched from the workflow ID, not from the query.
func TestListQuery(t *testing.T) {
	query := listQuery()
	if !strings.Contains(query, "WorkflowType = 'TaskWorkflow'") ||
		!strings.Contains(query, "ExecutionStatus = 'Running'") {
		t.Errorf("unexpected listing query: %q", query)
	}
	if strings.Contains(query, "AxAtespace") {
		t.Errorf("expected no custom search attribute, got %q", query)
	}
}

// A listing returns the running task workflows without asking each one for its
// state.
func TestListReturnsTasksWithoutAskingEachTask(t *testing.T) {
	started := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	fake := &fakeTemporal{
		executions: []listedExecution{
			{id: "team-a/job", started: started},
			{id: "team-a/other", started: started},
		},
	}
	client := newTestClient(fake)

	tasks, err := client.List(context.Background(), "team-a", 50, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected two tasks, got %d", len(tasks))
	}
	if fake.queries != 0 {
		t.Errorf("expected no task to be asked, got %d queries", fake.queries)
	}

	first := tasks[0]
	if first.GetMetadata().GetName() != "job" || first.GetMetadata().GetAtespace() != "team-a" {
		t.Errorf("expected the task's identity, got %v", first.GetMetadata())
	}
	if first.GetMetadata().GetCreationTimestamp().AsTime() != started {
		t.Errorf("expected the start time as the creation time, got %v",
			first.GetMetadata().GetCreationTimestamp())
	}
	if first.GetStatus().GetActor() != "job" {
		t.Errorf("an actor carries the task's name, got %q", first.GetStatus().GetActor())
	}
}

// Only the tasks in the named atespace are listed. The atespace is matched from
// the workflow ID.
func TestListFiltersByAtespace(t *testing.T) {
	fake := &fakeTemporal{executions: []listedExecution{
		{id: "team-a/job"},
		{id: "team-b/job"},
	}}
	client := newTestClient(fake)

	tasks, err := client.List(context.Background(), "team-a", 50, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(tasks) != 1 || tasks[0].GetMetadata().GetAtespace() != "team-a" {
		t.Fatalf("expected only the team-a task, got %v", tasks)
	}
}

// Paging happens over what visibility returned.
func TestListPages(t *testing.T) {
	fake := &fakeTemporal{executions: []listedExecution{
		{id: "team-a/one"},
		{id: "team-a/two"},
		{id: "team-a/three"},
	}}
	client := newTestClient(fake)

	tasks, err := client.List(context.Background(), "team-a", 1, 1)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(tasks) != 1 || tasks[0].GetMetadata().GetName() != "two" {
		t.Fatalf("expected the second task alone, got %v", tasks)
	}
}

// A workflow ID that is not a task's key still yields something sensible.
func TestSplitWorkflowID(t *testing.T) {
	atespace, name := splitWorkflowID("team-a/job")
	if atespace != "team-a" || name != "job" {
		t.Errorf("expected team-a and job, got %q and %q", atespace, name)
	}
	atespace, name = splitWorkflowID("job")
	if atespace != v1alpha1.DefaultAtespace || name != "job" {
		t.Errorf("expected the default atespace and job, got %q and %q", atespace, name)
	}
	// The separator is the last slash, so a name is never cut short.
	atespace, name = splitWorkflowID("a/b/c")
	if atespace != "a/b" || name != "c" {
		t.Errorf("expected a/b and c, got %q and %q", atespace, name)
	}
}
