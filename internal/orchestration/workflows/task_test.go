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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// workerIP is where the fake places a resumed actor.
const workerIP = "10.244.1.42"

// calls records what the workflow asked Substrate to do. The test environment
// runs activities on their own goroutines, so it is guarded.
type calls struct {
	mu sync.Mutex

	atespaces  []string
	templates  []string
	actors     []string
	suspends   []string
	resumes    []string
	reverts    []string
	probes     []string
	observes   []string
	delActors  []string
	delTmpl    []string
	delTmplOne []string
	templateIn []activities.TemplateInput

	// actorExists and actorTemplate stand in for Substrate binding an actor to
	// the template it was created from, actorState for what it is doing, and
	// workerIP for where. A test changes them behind the workflow's back to
	// stand in for a sandbox that crashed, moved, or vanished.
	actorExists   bool
	actorTemplate string
	actorState    string
	workerIP      string
	// madeTemplates records which templates this fake has created, so a second
	// pass over the same spec reports finding one rather than making it.
	madeTemplates map[string]bool

	// failures let a test make one step fail.
	templateErr    error
	actorErr       error
	resumeErr      error
	revertErr      error
	deleteActorErr error
	// workspaceReady is what the readiness poll reports, unless probeAnswers
	// has an answer queued for this call.
	workspaceReady bool
	probeAnswers   []bool

	// probeGate holds the readiness poll open so a test can replace the sandbox
	// while a probe is still talking to the one before it.
	probeGate chan struct{}
}

func (c *calls) add(list *[]string, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*list = append(*list, name)
}

func (c *calls) get(list *[]string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), *list...)
}

// set changes the fake under its lock, for a test that alters the sandbox
// while the workflow is running.
func (c *calls) set(change func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	change()
}

func (c *calls) lastTemplateInput() (activities.TemplateInput, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.templateIn) == 0 {
		return activities.TemplateInput{}, false
	}
	return c.templateIn[len(c.templateIn)-1], true
}

type taskWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env   *testsuite.TestWorkflowEnvironment
	calls *calls
}

func TestTaskWorkflow(t *testing.T) {
	suite.Run(t, new(taskWorkflowSuite))
}

func (s *taskWorkflowSuite) SetupTest() {
	s.setUp(workflows.DefaultConfig())
}

// setUp builds the environment a test runs the workflow in. A test that needs
// the resync to come around sooner calls it again with its own settings.
func (s *taskWorkflowSuite) setUp(cfg workflows.Config) {
	s.env = s.NewTestWorkflowEnvironment()
	// A task workflow is addressed by its business key, and the workflow ID ends
	// up inside the sandbox, so the tests run under a realistic one.
	s.env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: workflows.TaskWorkflowID("default", "test-task"),
	})
	s.calls = &calls{
		workspaceReady: true,
		workerIP:       workerIP,
		madeTemplates:  map[string]bool{},
	}
	s.mockActivities()
	s.env.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(cfg),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)
}

// updateTask sends an update and captures the task its handler answered with.
// Reading state through an update is deterministic: the handler only returns
// once the change has been driven into Substrate.
func (s *taskWorkflowSuite) updateTask(
	at time.Duration,
	name, id string,
	got **v1alpha1.Task,
	args ...any,
) {
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(name, id, &testsuite.TestUpdateCallback{
			OnReject: func(err error) { s.Failf("update rejected", "%s: %v", name, err) },
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
				task, ok := result.(*v1alpha1.Task)
				s.Require().True(ok, "unexpected %s result %T", name, result)
				*got = task
			},
		}, args...)
	}, at)
}

// updateStatus is updateTask for the handlers that answer with a status.
func (s *taskWorkflowSuite) updateStatus(
	at time.Duration,
	name, id string,
	got **v1alpha1.TaskStatus,
	args ...any,
) {
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(name, id, &testsuite.TestUpdateCallback{
			OnReject: func(err error) { s.Failf("update rejected", "%s: %v", name, err) },
			OnAccept: func() {},
			OnComplete: func(result any, err error) {
				s.Require().NoError(err)
				status, ok := result.(*v1alpha1.TaskStatus)
				s.Require().True(ok, "unexpected %s result %T", name, result)
				*got = status
			},
		}, args...)
	}, at)
}

// resume puts the task to work. A task is created suspended, so a test that
// wants it running starts with this.
func (s *taskWorkflowSuite) resume(at time.Duration) {
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflowNoRejection(workflows.UpdateResume, "resume-"+at.String(), s.T())
	}, at)
}

// refuse sends an update that has to be rejected and records the rejection.
// The environment hands the update to the workflow on its next turn, so the
// rejection is there to read once the workflow has finished.
func (s *taskWorkflowSuite) refuse(name, id string, got *error, args ...any) {
	s.env.UpdateWorkflow(name, id, &testsuite.TestUpdateCallback{
		OnReject:   func(err error) { *got = err },
		OnAccept:   func() { s.Failf("update accepted", "%s should have been rejected", name) },
		OnComplete: func(any, error) {},
	}, args...)
}

// errorType returns the type of the application error behind a rejection.
func errorType(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Type()
	}
	return ""
}

// mockActivities stands in for Agent Substrate. Every activity records its call
// so a test can assert the order and the rollback.
func (s *taskWorkflowSuite) mockActivities() {
	var a *activities.Activities
	c := s.calls
	s.env.RegisterActivity(&activities.Activities{})

	s.env.OnActivity(a.EnsureAtespace, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.AtespaceInput) error {
			c.add(&c.atespaces, in.Atespace)
			return nil
		}).Maybe()

	s.env.OnActivity(a.EnsureActorTemplate, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplateInput) (activities.TemplateProvision, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.templates = append(c.templates, in.Template.Name)
			c.templateIn = append(c.templateIn, in)
			if c.templateErr != nil {
				return activities.TemplateProvision{}, c.templateErr
			}
			c.madeTemplates[in.Template.Name] = true
			return activities.TemplateProvision{Template: in.Template}, nil
		}).
		Maybe()

	s.env.OnActivity(a.EnsureActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorInput) (activities.ActorProvision, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.actors = append(c.actors, in.Actor.Name)
			if c.actorErr != nil {
				return activities.ActorProvision{}, c.actorErr
			}
			if c.actorExists {
				// Substrate keeps an actor on the template it was created from.
				return activities.ActorProvision{
					Template: activities.TemplateRef{
						Atespace: in.Actor.Atespace,
						Name:     c.actorTemplate,
					},
					State: c.actorState,
				}, nil
			}
			c.actorExists = true
			c.actorTemplate = in.Template.Name
			c.actorState = activities.ActorStateSuspended
			return activities.ActorProvision{
				Template: in.Template,
				State:    activities.ActorStateSuspended,
			}, nil
		}).Maybe()

	s.env.OnActivity(a.SuspendActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.suspends = append(c.suspends, in.Name)
			c.actorState = activities.ActorStateSuspended
			return nil
		}).Maybe()

	s.env.OnActivity(a.ResumeActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) (string, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.resumes = append(c.resumes, in.Name)
			if c.resumeErr != nil {
				return "", c.resumeErr
			}
			c.actorState = activities.ActorStateRunning
			return c.workerIP, nil
		}).Maybe()

	s.env.OnActivity(a.RevertActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.reverts = append(c.reverts, in.Name)
			if c.revertErr != nil {
				return c.revertErr
			}
			// A reverted actor is back on its last snapshot, stopped.
			c.actorState = activities.ActorStateSuspended
			return nil
		}).Maybe()

	s.env.OnActivity(a.ObserveActorTemplate, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplateRef) (activities.TemplateObservation, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			return activities.TemplateObservation{Exists: c.madeTemplates[in.Name]}, nil
		}).
		Maybe()

	s.env.OnActivity(a.AwaitWorkspaceReady, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.WorkspaceReadyInput) (bool, error) {
			c.mu.Lock()
			c.probes = append(c.probes, in.WorkerIP)
			ready := c.workspaceReady
			if len(c.probeAnswers) > 0 {
				ready, c.probeAnswers = c.probeAnswers[0], c.probeAnswers[1:]
			}
			gate := c.probeGate
			c.mu.Unlock()
			if gate != nil {
				<-gate
			}
			return ready, nil
		}).Maybe()

	s.env.OnActivity(a.ObserveActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) (activities.ActorObservation, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.observes = append(c.observes, in.Name)
			if !c.actorExists {
				return activities.ActorObservation{}, nil
			}
			observed := activities.ActorObservation{
				Exists:   true,
				State:    c.actorState,
				Template: activities.TemplateRef{Atespace: in.Atespace, Name: c.actorTemplate},
			}
			if c.actorState == activities.ActorStateRunning {
				observed.WorkerIP = c.workerIP
			}
			return observed, nil
		}).Maybe()

	s.env.OnActivity(a.DeleteActorIfExists, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.delActors = append(c.delActors, in.Name)
			if c.deleteActorErr != nil {
				return c.deleteActorErr
			}
			c.actorExists = false
			c.actorTemplate = ""
			c.actorState = ""
			return nil
		}).Maybe()

	s.env.OnActivity(a.DeleteActorTemplateIfExists, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplateRef) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.delTmplOne = append(c.delTmplOne, in.Name)
			delete(c.madeTemplates, in.Name)
			return nil
		}).Maybe()

	s.env.OnActivity(a.DeleteActorTemplates, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplatesInput) error {
			c.add(&c.delTmpl, in.TaskName)
			return nil
		}).Maybe()
}

// testInput is a task as it stands once it has been created: its spec is known
// and it is meant to be suspended, which is how every task starts out. A test
// that wants the task running resumes it.
func testInput() workflows.TaskWorkflowInput {
	return workflows.TaskWorkflowInput{
		Desired: &workflows.TaskDesiredState{
			Task: &v1alpha1.Task{
				ApiVersion: v1alpha1.APIVersion,
				Kind:       v1alpha1.KindTask,
				Metadata:   &v1alpha1.ObjectMeta{Name: "test-task", Atespace: "default"},
				Spec: &v1alpha1.TaskSpec{
					Image:   "ghcr.io/example/agent",
					Command: []string{"/bin/agent"},
				},
			},
			Workspaces: []*v1alpha1.Workspace{{
				Metadata: &v1alpha1.ObjectMeta{Name: "repo", Atespace: "default"},
				Spec: &v1alpha1.WorkspaceSpec{
					Git: []*v1alpha1.GitRepo{
						{Name: "repo", Repo: "https://github.com/example/repo"},
					},
				},
			}},
		},
		Suspended: true,
	}
}

// templateName is the template the test task's sandbox of a generation is
// built from.
func templateName(generation int) string {
	in := testInput()
	return activities.TaskTemplateName(in.Desired.Task, in.Desired.Workspaces, generation)
}

// permanent is a Substrate rejection no retry can fix, so a step fails at once.
func permanent(message string) error {
	return temporal.NewNonRetryableApplicationError(message, activities.ErrTypePermanent, nil)
}

// queryStatus reads the task status through the query handler.
func (s *taskWorkflowSuite) queryStatus() *v1alpha1.TaskStatus {
	value, err := s.env.QueryWorkflow(workflows.QueryStatus)
	s.Require().NoError(err)
	var status v1alpha1.TaskStatus
	s.Require().NoError(value.Get(&status))
	return &status
}

// queryTask reads the whole task through the query handler.
func (s *taskWorkflowSuite) queryTask() *v1alpha1.Task {
	value, err := s.env.QueryWorkflow(workflows.QueryTask)
	s.Require().NoError(err)
	var task v1alpha1.Task
	s.Require().NoError(value.Get(&task))
	return &task
}

// delete asks the task to go away, which is how a task workflow ends.
//
// The delays the tests use are workflow time, which the environment skips while
// the workflow is idle. They are kept well apart so that a callback cannot land
// in the middle of a step that is still running.
func (s *taskWorkflowSuite) delete(after time.Duration) {
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflowNoRejection(workflows.UpdateDelete, "delete-1", s.T())
	}, after)
}

// A task is created suspended: its sandbox is built and checkpointed, and
// nothing runs in it until the task is resumed.
func (s *taskWorkflowSuite) TestATaskIsCreatedSuspended() {
	var created *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { created = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"default"}, s.calls.get(&s.calls.atespaces))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.actors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends))
	s.Empty(s.calls.get(&s.calls.resumes))
	s.Empty(s.calls.get(&s.calls.probes), "a suspended sandbox is on no worker to probe")

	s.Require().NotNil(created)
	s.Equal(v1alpha1.PhaseSuspended, created.GetPhase())
	s.Equal("test-task", created.GetActor())
	s.Empty(created.GetWorkerIp())
	s.Nil(created.ExitCode)
	assertCondition(s.T(), created, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TaskSuspended")
}

func (s *taskWorkflowSuite) TestProvisionsAndRuns() {
	var resumed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateResume, "resume-1", &resumed)
	var running *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { running = s.queryStatus() }, 2*time.Second)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes))
	s.Equal([]string{workerIP}, s.calls.get(&s.calls.probes))
	// Resuming is not a spec change, so the sandbox is the one creation built.
	s.Len(s.calls.get(&s.calls.actors), 1)

	s.Require().NotNil(resumed)
	s.Equal(v1alpha1.PhaseRunning, resumed.GetStatus().GetPhase())
	s.Equal(workerIP, resumed.GetStatus().GetWorkerIp())

	s.Require().NotNil(running)
	s.Equal(v1alpha1.PhaseRunning, running.GetPhase())
	s.Equal("test-task", running.GetActor())
	s.Nil(running.ExitCode)
	assertCondition(s.T(), running, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning")
	assertCondition(s.T(), running, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")

	// Deleting the task releases the sandbox and then its templates.
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))
}

// The template name is derived from the task and workspace specs, so the same
// spec always addresses the same template.
func (s *taskWorkflowSuite) TestTemplateNameIsDerivedFromTheSpec() {
	s.delete(time.Second)
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	got, ok := s.calls.lastTemplateInput()
	s.Require().True(ok)
	s.Equal(templateName(0), got.Template.Name)
	s.Equal("default", got.Template.Atespace)
	s.Equal("default/test-task", got.WorkflowID)
	s.Equal(0, got.Generation)
}

func (s *taskWorkflowSuite) TestActorFailureRollsBackProvisioning() {
	s.calls.actorErr = permanent("template does not exist")

	// A resume asks for another pass, so the sandbox is provisioned again and
	// the answer carries the state the task settled in.
	var failed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateResume, "resume-1", &failed)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.NotEmpty(s.calls.get(&s.calls.actors))
	s.Empty(s.calls.get(&s.calls.resumes), "a task that failed to provision is never resumed")
	s.Contains(s.calls.get(&s.calls.delTmplOne), templateName(0),
		"the template the pass created is rolled back")
	s.Contains(s.calls.get(&s.calls.delTmpl), "test-task", "the teardown removes what is left")

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"ActorCreationFailed")
}

// A template that cannot be built leaves no actor behind. The atespace is
// shared with every other task in it, so it stays.
func (s *taskWorkflowSuite) TestTemplateFailureLeavesNoActor() {
	s.calls.templateErr = permanent("image is not allowed")

	var failed *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { failed = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"default"}, s.calls.get(&s.calls.atespaces))
	s.Empty(s.calls.get(&s.calls.actors))
	s.Empty(s.calls.get(&s.calls.resumes))

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetPhase())
	assertCondition(s.T(), failed, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TemplateCreationFailed")
}

// Activation is never compensated: a resume that fails leaves the task Failed
// with its sandbox intact, so the workspace inside survives and a later resume
// can still succeed.
func (s *taskWorkflowSuite) TestAResumeThatFailsKeepsTheSandbox() {
	s.calls.resumeErr = permanent("no worker has room")

	var failed, running *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateResume, "resume-1", &failed)
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.resumeErr = nil })
	}, 2*time.Second)
	s.updateTask(3*time.Second, workflows.UpdateResume, "resume-2", &running)
	s.delete(4 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"ActorResumeFailed")

	s.Require().NotNil(running)
	s.Equal(v1alpha1.PhaseRunning, running.GetStatus().GetPhase())
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes))
	s.Len(s.calls.get(&s.calls.actors), 1, "the sandbox is kept across the failure")
	s.Len(s.calls.get(&s.calls.delActors), 1, "only the teardown deletes the actor")
}

func (s *taskWorkflowSuite) TestSuspendAndResume() {
	var runningTask, suspendedTask, resumedTask *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateResume, "resume-1", &runningTask)
	s.updateTask(2*time.Second, workflows.UpdateSuspend, "suspend-1", &suspendedTask)
	s.updateTask(3*time.Second, workflows.UpdateResume, "resume-2", &resumedTask)
	s.delete(4 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	// One suspend when the task is created, one for the update.
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.suspends))
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes))
	// Suspending is not a spec change, so the sandbox is never rebuilt.
	s.Len(s.calls.get(&s.calls.actors), 1)
	s.Len(s.calls.get(&s.calls.templates), 1)

	s.Require().NotNil(runningTask)
	s.Equal(v1alpha1.PhaseRunning, runningTask.GetStatus().GetPhase())

	s.Require().NotNil(suspendedTask)
	suspended := suspendedTask.GetStatus()
	s.Equal(v1alpha1.PhaseSuspended, suspended.GetPhase())
	s.Empty(suspended.GetWorkerIp())
	assertCondition(s.T(), suspended, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TaskSuspended")
	// Workspace setup is a one-time step whose result outlives a suspend.
	assertCondition(s.T(), suspended, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")

	s.Require().NotNil(resumedTask)
	resumed := resumedTask.GetStatus()
	s.Equal(v1alpha1.PhaseRunning, resumed.GetPhase())
	s.Equal(workerIP, resumed.GetWorkerIp())
	assertCondition(s.T(), resumed, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning")
	// The workspace is not probed again after a resume.
	s.Len(s.calls.get(&s.calls.probes), 1)
}

// Suspending a task that is already suspended is accepted and changes nothing.
func (s *taskWorkflowSuite) TestSuspendingASuspendedTaskChangesNothing() {
	var suspended *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateSuspend, "suspend-1", &suspended)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends),
		"the sandbox is not suspended a second time")
	s.Require().NotNil(suspended)
	s.Equal(v1alpha1.PhaseSuspended, suspended.GetStatus().GetPhase())
}

func (s *taskWorkflowSuite) TestCompleteRecordsTheExitCode() {
	var completed *v1alpha1.TaskStatus
	var deletedWhileCompleted []string

	s.resume(time.Second)
	s.updateStatus(2*time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 3, Message: "agent exited with code 3"})
	s.env.RegisterDelayedCallback(func() {
		deletedWhileCompleted = s.calls.get(&s.calls.delActors)
	}, 3*time.Second)
	s.delete(4 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Require().NotNil(completed.ExitCode)
	s.Equal(int32(3), completed.GetExitCode())
	assertCondition(s.T(), completed, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"CommandExited")
	// The sandbox stays up after the command exits so its workspace can still be
	// inspected.
	s.Empty(deletedWhileCompleted)
}

// The runner falls back to a signal when it cannot wait for an update.
func (s *taskWorkflowSuite) TestCompletionSignalRecordsTheExitCode() {
	var completed *v1alpha1.TaskStatus

	s.resume(time.Second)
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(workflows.SignalComplete, workflows.CompleteInput{ExitCode: 0})
	}, 2*time.Second)
	s.env.RegisterDelayedCallback(func() { completed = s.queryStatus() }, 3*time.Second)
	s.delete(4 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Require().NotNil(completed.ExitCode)
	s.Equal(int32(0), completed.GetExitCode())
}

func (s *taskWorkflowSuite) TestDeleteTearsDownAndRejectsFurtherChanges() {
	// The actor is slow to go, so the task spends a while Terminating.
	s.calls.deleteActorErr = temporal.NewApplicationError("actor is still stopping",
		activities.ErrTypeSubstrate)
	s.delete(time.Second)
	var terminating *v1alpha1.TaskStatus
	var rejection error
	s.env.RegisterDelayedCallback(func() {
		terminating = s.queryStatus()
		// A task that is going away stops accepting changes.
		s.refuse(workflows.UpdateSuspend, "suspend-late", &rejection)
		s.calls.set(func() { s.calls.deleteActorErr = nil })
	}, time.Second+500*time.Millisecond)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Require().NotNil(terminating)
	s.Equal(v1alpha1.PhaseTerminating, terminating.GetPhase())
	s.Equal(workflows.ErrTypeTaskTerminating, errorType(rejection))
	s.Len(s.calls.get(&s.calls.delActors), 2, "the teardown retried until the actor went")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))

	// The task ends on the Terminating phase, and its execution has closed, which
	// is what tells the API server the task is gone.
	s.Equal(v1alpha1.PhaseTerminating, s.queryTask().GetStatus().GetPhase())
}

func (s *taskWorkflowSuite) TestCancellationReleasesTheSandbox() {
	s.env.RegisterDelayedCallback(func() {
		s.env.CancelWorkflow()
	}, time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError(), "a cancelled workflow reports the cancellation")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors),
		"cancellation releases the sandbox through a disconnected context")
}

// A resync notices a sandbox that crashed behind the workflow's back and puts
// it back on its last snapshot, which keeps the workspace. The actor comes back
// stopped, so the next pass resumes it again.
func (s *taskWorkflowSuite) TestResyncRevertsACrashedSandbox() {
	s.resume(time.Second)
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.actorState = activities.ActorStateCrashed })
	}, 2*time.Second)
	var recovered *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { recovered = s.queryStatus() }, 6*time.Minute)
	// The sandbox is the one the task had, so a report from it is still taken.
	var completed *v1alpha1.TaskStatus
	s.updateStatus(6*time.Minute+time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 0, Generation: 0})
	s.delete(6*time.Minute + 2*time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.reverts))
	s.Len(s.calls.get(&s.calls.actors), 1, "the sandbox is kept")
	s.Len(s.calls.get(&s.calls.templates), 1, "and so is its generation")
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes),
		"the reverted sandbox is resumed again")
	s.Len(s.calls.get(&s.calls.delActors), 1, "only the teardown deletes the actor")

	s.Require().NotNil(recovered)
	s.Equal(v1alpha1.PhaseRunning, recovered.GetPhase())
	s.Equal(workerIP, recovered.GetWorkerIp())
	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
}

// A crashed sandbox that cannot be reverted is given up on: the task moves to
// its next generation and builds a sandbox from scratch.
func (s *taskWorkflowSuite) TestResyncReplacesACrashedSandboxItCannotRevert() {
	s.resume(time.Second)
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() {
			s.calls.actorState = activities.ActorStateCrashed
			s.calls.revertErr = permanent("no snapshot to revert to")
		})
	}, 2*time.Second)
	var replaced *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { replaced = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.reverts), "the revert is tried first")
	s.Len(s.calls.get(&s.calls.actors), 2, "the crashed sandbox is provisioned again")
	templates := s.calls.get(&s.calls.templates)
	s.Require().Len(templates, 2)
	s.Equal(templateName(1), templates[1], "the replacement is the task's second sandbox")
	// One delete to replace the sandbox, one to tear the task down.
	s.Len(s.calls.get(&s.calls.delActors), 2)
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes))
	s.Len(s.calls.get(&s.calls.probes), 2, "the replacement sandbox is probed for itself")

	s.Require().NotNil(replaced)
	s.Equal(v1alpha1.PhaseRunning, replaced.GetPhase())
}

// A sandbox that was rescheduled onto another worker has its address corrected
// without being resumed again.
func (s *taskWorkflowSuite) TestResyncCorrectsAMovedSandbox() {
	s.resume(time.Second)
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.workerIP = "10.244.2.7" })
	}, 2*time.Second)
	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes))
	s.Len(s.calls.get(&s.calls.actors), 1)
	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseRunning, status.GetPhase())
	s.Equal("10.244.2.7", status.GetWorkerIp())
}

// A sandbox that was stopped behind the task's back, while the task is meant to
// run, is put back on a worker.
func (s *taskWorkflowSuite) TestResyncResumesASandboxSuspendedOutOfBand() {
	s.resume(time.Second)
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.actorState = activities.ActorStateSuspended })
	}, 2*time.Second)
	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes),
		"the resync resumes the sandbox again")
	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseRunning, status.GetPhase())
	s.Equal(workerIP, status.GetWorkerIp())
}

// A task carries its status and its sandbox generation across a continue-as-new
// boundary, so a long-lived task does not provision itself from scratch again
// and does not take its own sandbox for one that has to be replaced.
func (s *taskWorkflowSuite) TestContinuedRunKeepsTheSandbox() {
	in := testInput()
	in.Suspended = false
	in.Generation = 2
	in.Status = &v1alpha1.TaskStatus{
		Phase:    v1alpha1.PhaseRunning,
		Actor:    "test-task",
		Id:       "task-test-task-1",
		WorkerIp: workerIP,
		Conditions: []*v1alpha1.Condition{{
			Type:   v1alpha1.ConditionWorkspaceReady,
			Status: v1alpha1.ConditionTrue,
			Reason: "SetupComplete",
		}},
	}
	// The sandbox the previous run built is there, on the template of its
	// generation.
	template := templateName(2)
	s.calls.actorExists = true
	s.calls.actorTemplate = template
	s.calls.actorState = activities.ActorStateRunning
	s.calls.madeTemplates[template] = true

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	// Provisioning runs again because it is idempotent, but the workspace is not
	// probed a second time, the sandbox is not replaced, and the task keeps its
	// identity.
	s.Empty(s.calls.get(&s.calls.probes))
	s.Len(s.calls.get(&s.calls.delActors), 1, "only the teardown deletes the actor")
	s.Equal([]string{template}, s.calls.get(&s.calls.templates))
	s.Require().NotNil(status)
	s.Equal("task-test-task-1", status.GetId())
	s.Equal(v1alpha1.PhaseRunning, status.GetPhase())
}

// A continued run of a task that was never resumed keeps it stopped.
func (s *taskWorkflowSuite) TestContinuedRunKeepsATaskSuspended() {
	in := testInput()
	in.Status = &v1alpha1.TaskStatus{
		Phase: v1alpha1.PhaseSuspended,
		Actor: "test-task",
		Id:    "task-test-task-1",
	}
	template := templateName(0)
	s.calls.actorExists = true
	s.calls.actorTemplate = template
	s.calls.actorState = activities.ActorStateSuspended
	s.calls.madeTemplates[template] = true

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	s.Empty(s.calls.get(&s.calls.resumes))
	s.Len(s.calls.get(&s.calls.actors), 1)
	s.Require().NotNil(status)
	s.Equal("task-test-task-1", status.GetId())
	s.Equal(v1alpha1.PhaseSuspended, status.GetPhase())
}

func (s *taskWorkflowSuite) TestInvalidTaskIsRejected() {
	in := testInput()
	in.Desired.Task.Spec.Workspaces = []*v1alpha1.WorkspaceRef{
		{Name: "a", Path: "/same"},
		{Name: "b", Path: "/same"},
	}

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Require().Error(err)
	s.Equal(workflows.ErrTypeInvalidTask, errorType(err))
	s.Empty(s.calls.get(&s.calls.atespaces), "nothing is provisioned for a task that cannot run")
}

func assertCondition(
	t *testing.T,
	status *v1alpha1.TaskStatus,
	condType, wantStatus, wantReason string,
) {
	t.Helper()
	for _, c := range status.GetConditions() {
		if c.GetType() != condType {
			continue
		}
		require.Equal(t, wantStatus, c.GetStatus(), "condition %s status", condType)
		require.Equal(t, wantReason, c.GetReason(), "condition %s reason", condType)
		return
	}
	t.Errorf("expected a %s condition, got %v", condType, status.GetConditions())
}

// A maiden run that outlasts the readiness poll still reports ready: the resync
// looks again.
func (s *taskWorkflowSuite) TestResyncRechecksAWorkspaceThatWasStillInitializing() {
	s.calls.workspaceReady = false

	s.resume(time.Second)
	var initializing, ready *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		initializing = s.queryStatus()
		// The workspace finishes setting up while the task is running.
		s.calls.set(func() { s.calls.workspaceReady = true })
	}, 2*time.Second)
	s.env.RegisterDelayedCallback(func() { ready = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(initializing)
	assertCondition(s.T(), initializing, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse,
		"Initializing")
	assertCondition(s.T(), initializing, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"WorkspaceInitializing")

	s.Len(s.calls.get(&s.calls.probes), 2, "the resync probes the workspace again")
	s.Require().NotNil(ready)
	assertCondition(s.T(), ready, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")
	assertCondition(s.T(), ready, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning")
	// The sandbox itself was never rebuilt.
	s.Len(s.calls.get(&s.calls.actors), 1)
}

// Tasks are immutable. Substrate binds the sandbox to the template the first
// spec built, so a second apply is refused rather than replacing the sandbox
// behind the task's back.
func (s *taskWorkflowSuite) TestASecondApplyIsRejected() {
	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"

	var rejection error
	s.env.RegisterDelayedCallback(func() {
		s.refuse(workflows.UpdateApply, "apply-2", &rejection, changed.Desired)
	}, time.Second)
	var task *v1alpha1.Task
	s.env.RegisterDelayedCallback(func() { task = s.queryTask() }, 2*time.Second)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal(workflows.ErrTypeTaskExists, errorType(rejection))
	s.Equal([]string{templateName(0)}, s.calls.get(&s.calls.templates),
		"the sandbox is not rebuilt")
	s.Len(s.calls.get(&s.calls.delActors), 1, "only the teardown deletes the actor")
	s.Require().NotNil(task)
	s.Equal("ghcr.io/example/agent", task.GetSpec().GetImage(),
		"the task keeps the spec it was created with")
}

// Substrate binds an actor to the template it was created from. An actor found
// on any other template was left by a sandbox the workflow gave up on, or by a
// control plane that ran the task before this one, and is replaced.
func (s *taskWorkflowSuite) TestAnActorOnAnotherTemplateIsReplaced() {
	s.calls.actorExists = true
	s.calls.actorTemplate = "test-task-tmpl-deadbeef"
	s.calls.actorState = activities.ActorStateSuspended

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	// One delete to replace the sandbox, one to tear the task down.
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.actors))
	s.Equal([]string{templateName(0)}, s.calls.get(&s.calls.templates))
	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseSuspended, status.GetPhase())
}

// What a rollback may delete is settled by looking before the pass creates
// anything, not by what the create calls answer: a retried create finds what
// its first attempt made and answers "found".
func (s *taskWorkflowSuite) TestRollbackLeavesATemplateThatWasThereBeforeThePass() {
	template := templateName(0)
	s.calls.madeTemplates[template] = true
	s.calls.actorErr = permanent("template does not exist")

	s.delete(time.Second)
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{template}, s.calls.get(&s.calls.templates))
	s.Empty(s.calls.get(&s.calls.delTmplOne), "a template that was there before is not rolled back")
	s.NotEmpty(s.calls.get(&s.calls.delActors), "the actor the pass tried to make is")
}

// An answer from a sandbox that has since been replaced says nothing about its
// replacement.
func (s *taskWorkflowSuite) TestAProbeForAReplacedSandboxIsDropped() {
	// The resync is what replaces a sandbox, and a probe that is held open keeps
	// the clock at wall speed, so the resync has to come around quickly here.
	s.setUp(workflows.Config{ResyncInterval: 2 * time.Second})
	gate := make(chan struct{})
	s.calls.probeGate = gate
	// The first sandbox reports not ready, the replacement reports ready, and
	// both answers arrive after the replacement.
	s.calls.probeAnswers = []bool{false, true}

	s.resume(time.Second)
	// The sandbox vanishes while its probe is still open; the resync notices and
	// builds another.
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.actorExists = false })
	}, 2*time.Second)
	s.env.RegisterDelayedCallback(func() { close(gate) }, 4*time.Second)
	var settled *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { settled = s.queryStatus() }, 5*time.Second)
	s.delete(6 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Len(s.calls.get(&s.calls.actors), 2, "the vanished sandbox is provisioned again")
	s.Len(s.calls.get(&s.calls.probes), 2, "the replacement gets a probe of its own")
	s.Require().NotNil(settled)
	assertCondition(s.T(), settled, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")
}

// A task that continues as new answers the update it was given first, and hands
// what the update changed on to the next run.
func (s *taskWorkflowSuite) TestContinueAsNewFinishesAnAdmittedUpdate() {
	s.env.SetContinueAsNewSuggested(true)

	var resumed *v1alpha1.Task
	s.updateTask(0, workflows.UpdateResume, "resume-1", &resumed)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	var continued *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &continued)

	s.Require().NotNil(resumed, "the update is answered before the run ends")
	s.Equal(v1alpha1.PhaseRunning, resumed.GetStatus().GetPhase())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes), "and its effect landed")

	var next workflows.TaskWorkflowInput
	s.Require().NoError(converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	s.False(next.Suspended, "the next run has to know the task was resumed")
	s.Equal(v1alpha1.PhaseRunning, next.Status.GetPhase())
}

// A task that was never resumed continues as new still suspended.
func (s *taskWorkflowSuite) TestContinueAsNewKeepsATaskSuspended() {
	s.env.SetContinueAsNewSuggested(true)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	var continued *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &continued)

	var next workflows.TaskWorkflowInput
	s.Require().NoError(converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	s.True(next.Suspended)
	s.Equal(0, next.Generation)
	s.Equal(v1alpha1.PhaseSuspended, next.Status.GetPhase())
	s.Equal("test-task", next.Desired.GetTask().GetMetadata().GetName())
}

// A completion left in the channel goes into the next run instead of down with
// this one.
func (s *taskWorkflowSuite) TestContinueAsNewKeepsABufferedCompletion() {
	s.env.SetContinueAsNewSuggested(true)
	s.env.RegisterDelayedCallback(func() {
		// Buffered without running workflow code, so it is still in the channel
		// when the run is on its way out.
		s.env.SignalWorkflowSkippingWorkflowTask(workflows.SignalComplete,
			workflows.CompleteInput{ExitCode: 9})
		s.env.SignalWorkflow(workflows.SignalComplete, workflows.CompleteInput{ExitCode: 9})
	}, 0)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	var continued *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &continued)

	var next workflows.TaskWorkflowInput
	s.Require().NoError(converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	s.Require().NotNil(next.Status)
	s.Require().NotNil(next.Status.ExitCode)
	s.Equal(int32(9), next.Status.GetExitCode())
	s.Equal(v1alpha1.PhaseCompleted, next.Status.GetPhase())
}

// A task is only gone once its sandbox is. A teardown that cannot finish keeps
// the task answerable, and a later delete tries again.
func (s *taskWorkflowSuite) TestATeardownThatFailsKeepsTheTask() {
	s.calls.deleteActorErr = permanent("actor is wedged")

	var deleteErr error
	var stillThere *v1alpha1.TaskStatus

	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(workflows.UpdateDelete, "delete-1", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { s.Failf("delete rejected", "%v", err) },
			OnAccept: func() {},
			OnComplete: func(_ any, err error) {
				deleteErr = err
			},
		})
	}, time.Second)

	s.env.RegisterDelayedCallback(func() {
		stillThere = s.queryStatus()
		// Whatever wedged the sandbox clears, and the task can go.
		s.calls.set(func() { s.calls.deleteActorErr = nil })
		s.env.UpdateWorkflowNoRejection(workflows.UpdateDelete, "delete-2", s.T())
	}, 2*time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "the second delete released the task")

	s.Require().Error(deleteErr, "a delete that could not release the sandbox says so")
	s.Equal(workflows.ErrTypeTeardownFailed, errorType(deleteErr))
	s.Contains(deleteErr.Error(), "still there")

	s.Require().NotNil(stillThere)
	s.Equal(v1alpha1.PhaseFailed, stillThere.GetPhase(), "the task stays, and says it could not go")
	assertCondition(s.T(), stillThere, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TeardownFailed")

	s.Len(s.calls.get(&s.calls.delActors), 2, "the delete was tried again")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl), "templates go once the actor has")
}

// A task started together with the update that carries its spec waits for it
// rather than assuming one, and comes up suspended.
func (s *taskWorkflowSuite) TestAStartWithNoSpecWaitsForTheApply() {
	var applied *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &applied, testInput().Desired)
	s.delete(2 * time.Second)

	// The start carries nothing, which is what update-with-start sends.
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(applied)
	s.Equal("test-task", applied.GetMetadata().GetName())
	s.Equal(v1alpha1.PhaseSuspended, applied.GetStatus().GetPhase())
	s.Empty(applied.GetStatus().GetWorkerIp())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.actors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends))
	s.Empty(s.calls.get(&s.calls.resumes))
}

// A completion report names the sandbox it comes from. One from a sandbox the
// task has since replaced says how that sandbox's command ended, not how the
// current one is doing, and is refused on both paths it can arrive by.
func (s *taskWorkflowSuite) TestACompletionFromAReplacedSandboxIsRefused() {
	s.resume(time.Second)
	// The sandbox crashes and cannot be reverted, so the task replaces it.
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() {
			s.calls.actorState = activities.ActorStateCrashed
			s.calls.revertErr = permanent("no snapshot to revert to")
		})
	}, 2*time.Second)

	var rejection error
	s.env.RegisterDelayedCallback(func() {
		stale := workflows.CompleteInput{ExitCode: -1, Message: "stopped", Generation: 0}
		s.refuse(workflows.UpdateComplete, "complete-stale", &rejection, stale)
		// The signal path has no validator, so the same report is dropped there.
		s.env.SignalWorkflow(workflows.SignalComplete, stale)
	}, 6*time.Minute)
	var afterStale *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { afterStale = s.queryStatus() },
		6*time.Minute+time.Second)

	var completed *v1alpha1.TaskStatus
	s.updateStatus(6*time.Minute+2*time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 0, Generation: 1})
	s.delete(6*time.Minute + 3*time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Len(s.calls.get(&s.calls.actors), 2, "the sandbox was replaced")
	s.Equal(workflows.ErrTypeStaleReport, errorType(rejection),
		"a report from the replaced sandbox is refused")
	s.Require().NotNil(afterStale)
	s.Equal(v1alpha1.PhaseRunning, afterStale.GetPhase())
	s.Nil(afterStale.ExitCode)

	s.Require().NotNil(completed, "a report from the current sandbox is taken")
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Equal(int32(0), completed.GetExitCode())
}

// A replacement sandbox has not run its command yet, so how the command in the
// sandbox before it ended does not carry over.
func (s *taskWorkflowSuite) TestReplacingTheSandboxResetsCompletion() {
	s.resume(time.Second)
	var completed *v1alpha1.TaskStatus
	s.updateStatus(2*time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 3})
	// The sandbox vanishes; the resync notices and builds another.
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.actorExists = false })
	}, 3*time.Second)
	var replaced *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { replaced = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())

	s.Len(s.calls.get(&s.calls.actors), 2, "the vanished sandbox is provisioned again")
	s.Equal([]string{templateName(0), templateName(1)}, s.calls.get(&s.calls.templates))
	s.Require().NotNil(replaced)
	s.Equal(v1alpha1.PhaseRunning, replaced.GetPhase())
	s.Nil(replaced.ExitCode)
}

// A spec whose identity names another workflow cannot be applied here, whatever
// client sent it: the actor would land in one atespace while the ID said
// another.
func (s *taskWorkflowSuite) TestApplyRejectsASpecForAnotherTask() {
	other := testInput().Desired
	other.Task.Metadata.Atespace = "team-b"

	var rejection error
	s.env.RegisterDelayedCallback(func() {
		s.refuse(workflows.UpdateApply, "apply-other", &rejection, other)
	}, time.Second)
	// The task's own spec is still taken afterwards.
	var applied *v1alpha1.Task
	s.updateTask(2*time.Second, workflows.UpdateApply, "apply-1", &applied, testInput().Desired)
	s.delete(3 * time.Second)

	// The start carries nothing, so the refusal is about the spec's identity
	// rather than about the task already having one.
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal(workflows.ErrTypeInvalidTask, errorType(rejection),
		"a spec for another task is refused")
	s.Require().NotNil(applied)
	s.Equal("default", applied.GetMetadata().GetAtespace())
	s.Equal([]string{"default"}, s.calls.get(&s.calls.atespaces),
		"nothing is provisioned in the other atespace")
}

// Before the first apply there is no spec to change and no sandbox to report
// on, so those updates are refused rather than dereferencing nothing.
func (s *taskWorkflowSuite) TestChangesBeforeTheFirstApplyAreRejected() {
	var suspendErr, resumeErr, completeErr error
	s.env.RegisterDelayedCallback(func() {
		s.refuse(workflows.UpdateSuspend, "suspend-early", &suspendErr)
		s.refuse(workflows.UpdateResume, "resume-early", &resumeErr)
		s.refuse(workflows.UpdateComplete, "complete-early", &completeErr,
			workflows.CompleteInput{ExitCode: 0})
	}, time.Second)

	// The start carries nothing and the apply never comes, so the task gives up.
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})
	s.True(s.env.IsWorkflowCompleted())

	s.Equal(workflows.ErrTypeInvalidTask, errorType(suspendErr), "suspend before the first apply")
	s.Equal(workflows.ErrTypeInvalidTask, errorType(resumeErr), "resume before the first apply")
	s.Equal(workflows.ErrTypeInvalidTask, errorType(completeErr), "complete before the first apply")
}

// A delete does not wait for a pass that is still retrying its way to a
// sandbox: the teardown starts as soon as the delete lands.
func (s *taskWorkflowSuite) TestDeleteInterruptsARetryingProvision() {
	s.calls.actorErr = temporal.NewApplicationError("substrate unavailable",
		activities.ErrTypeSubstrate)
	start := s.env.Now()
	s.delete(time.Minute)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Less(s.env.Now().Sub(start), 5*time.Minute,
		"the delete must not wait out the provisioning budget")
	s.Contains(s.calls.get(&s.calls.delActors), "test-task")
	s.Contains(s.calls.get(&s.calls.delTmpl), "test-task")
}

// A task that could not be released stays Failed, and says why, across a
// continue-as-new boundary.
func (s *taskWorkflowSuite) TestContinuedRunKeepsATeardownFailure() {
	in := testInput()
	in.TeardownFailed = true
	in.TeardownMessage = "Actor default/test-task is still there: wedged"
	in.Status = &v1alpha1.TaskStatus{
		Phase: v1alpha1.PhaseFailed,
		Actor: "test-task",
		Id:    "task-test-task-1",
	}

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError(), "the delete releases the task")

	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseFailed, status.GetPhase())
	assertCondition(s.T(), status, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TeardownFailed")
}

// The run that continues as new hands a teardown failure on to the next one.
func (s *taskWorkflowSuite) TestContinueAsNewCarriesATeardownFailure() {
	s.env.SetContinueAsNewSuggested(true)
	s.calls.deleteActorErr = permanent("actor is wedged")

	var deleteErr error
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(workflows.UpdateDelete, "delete-1", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { s.Failf("delete rejected", "%v", err) },
			OnAccept:   func() {},
			OnComplete: func(_ any, err error) { deleteErr = err },
		})
	}, 0)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	var continued *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &continued)
	s.Require().Error(deleteErr, "the delete reports what was left behind")

	var next workflows.TaskWorkflowInput
	s.Require().NoError(converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	s.True(next.TeardownFailed, "the next run has to know the task could not be released")
	s.Contains(next.TeardownMessage, "still there")
	s.Equal(v1alpha1.PhaseFailed, next.Status.GetPhase())
}

// The router resumes a suspended actor when a request reaches it. The task was
// told to stay suspended, so the resync puts it back.
func (s *taskWorkflowSuite) TestResyncReSuspendsATaskResumedOutOfBand() {
	s.env.RegisterDelayedCallback(func() {
		s.calls.set(func() { s.calls.actorState = activities.ActorStateRunning })
	}, time.Second)

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.suspends),
		"the resync suspends the sandbox again")
	s.Empty(s.calls.get(&s.calls.resumes))
	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseSuspended, status.GetPhase())
	s.Empty(status.GetWorkerIp())
}

// A start that is never followed by its update does not sit there forever.
func (s *taskWorkflowSuite) TestAStartWithNoSpecGivesUp() {
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Require().Error(err)
	s.Equal(workflows.ErrTypeInvalidTask, errorType(err))
	s.Empty(s.calls.get(&s.calls.atespaces), "nothing is provisioned for a task that never arrived")
}
