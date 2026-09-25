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

// calls records what the workflow asked Substrate to do. The test environment
// runs activities on their own goroutines, so it is guarded.
type calls struct {
	mu sync.Mutex

	atespaces  []string
	templates  []string
	actors     []string
	policies   []string
	suspends   []string
	resumes    []string
	probes     []string
	observes   []string
	delActors  []string
	delPolicy  []string
	delTmpl    []string
	delTmplOne []string
	templateIn []activities.TemplateInput

	// actorExists and actorTemplate stand in for Substrate binding an actor to
	// the template it was created from, and actorState for what it is doing.
	actorExists   bool
	actorTemplate string
	actorState    string
	// madeTemplates records which templates this fake has created, so a second
	// pass over the same spec reports finding one rather than making it.
	madeTemplates map[string]bool

	// failures let a test make one step fail.
	templateErr    error
	actorErr       error
	policyErr      error
	resumeErr      error
	deleteActorErr error
	// workspaceReady is what the readiness poll reports, unless probeAnswers
	// has an answer queued for this call.
	workspaceReady bool
	probeAnswers   []bool
	// observation, when set, is what an observe sees in place of the fake's own
	// record of the actor. It only applies while the fake has an actor.
	observation *activities.ActorObservation

	// gates hold an activity open so a test can land an update in the middle of
	// a provisioning pass.
	templateGate chan struct{}
	probeGate    chan struct{}
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
	s.env = s.NewTestWorkflowEnvironment()
	// A task workflow is addressed by its business key, and the workflow ID ends
	// up inside the sandbox, so the tests run under a realistic one.
	s.env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: workflows.TaskWorkflowID("default", "test-task"),
	})
	s.calls = &calls{workspaceReady: true, madeTemplates: map[string]bool{}}
	s.mockActivities()
	s.env.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(workflows.DefaultConfig()),
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
			c.templates = append(c.templates, in.Template.Name)
			c.templateIn = append(c.templateIn, in)
			err := c.templateErr
			gate := c.templateGate
			created := !c.madeTemplates[in.Template.Name]
			c.madeTemplates[in.Template.Name] = true
			c.mu.Unlock()
			if gate != nil {
				<-gate
			}
			if err != nil {
				return activities.TemplateProvision{}, err
			}
			return activities.TemplateProvision{Created: created, Template: in.Template}, nil
		}).Maybe()

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
					Template: activities.TemplateRef{Atespace: in.Actor.Atespace, Name: c.actorTemplate},
					State:    c.actorState,
				}, nil
			}
			c.actorExists = true
			c.actorTemplate = in.Template.Name
			c.actorState = activities.ActorStateSuspended
			return activities.ActorProvision{
				Created:  true,
				Template: in.Template,
				State:    activities.ActorStateSuspended,
			}, nil
		}).Maybe()

	s.env.OnActivity(a.ApplyEgressPolicy, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.EgressInput) error {
			c.add(&c.policies, in.Actor.Name)
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.policyErr
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
			return "10.244.1.42", nil
		}).Maybe()

	s.env.OnActivity(a.ObserveActorTemplate, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplateRef) (activities.TemplateObservation, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			return activities.TemplateObservation{Exists: c.madeTemplates[in.Name]}, nil
		}).Maybe()

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
			observed := activities.ActorObservation{Exists: true, State: c.actorState}
			if c.actorState == activities.ActorStateRunning {
				observed.WorkerIP = "10.244.1.42"
			}
			if c.observation != nil {
				observed = *c.observation
			}
			observed.Template = activities.TemplateRef{Atespace: in.Atespace, Name: c.actorTemplate}
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

	s.env.OnActivity(a.DeleteEgressPolicyIfExists, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) error {
			c.add(&c.delPolicy, in.Name)
			return nil
		}).Maybe()

	s.env.OnActivity(a.DeleteActorTemplates, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.TemplatesInput) error {
			c.add(&c.delTmpl, in.TaskName)
			return nil
		}).Maybe()
}

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
					Gateway: &v1alpha1.GatewayRef{Name: "default-gateway"},
				},
			},
			Gateway: &v1alpha1.Gateway{
				Metadata: &v1alpha1.ObjectMeta{Name: "default-gateway", Atespace: "default"},
				Spec: &v1alpha1.GatewaySpec{
					Egress: &v1alpha1.EgressConfig{
						Allowlist: &v1alpha1.EgressAllowlist{
							Hosts: []*v1alpha1.HostRule{{Host: "github.com", Port: 443}},
						},
					},
				},
			},
			Workspaces: []*v1alpha1.Workspace{{
				Metadata: &v1alpha1.ObjectMeta{Name: "repo", Atespace: "default"},
				Spec: &v1alpha1.WorkspaceSpec{
					Git: []*v1alpha1.GitRepo{{Name: "repo", Repo: "https://github.com/example/repo"}},
				},
			}},
		},
	}
}

// queryStatus reads the task status through the query handler.
func (s *taskWorkflowSuite) queryStatus() *v1alpha1.TaskStatus {
	value, err := s.env.QueryWorkflow(workflows.QueryStatus)
	s.Require().NoError(err)
	var status v1alpha1.TaskStatus
	s.Require().NoError(value.Get(&status))
	return &status
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

func (s *taskWorkflowSuite) TestProvisionsAndRuns() {
	var running *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		running = s.queryStatus()
	}, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	s.Equal([]string{"default"}, s.calls.get(&s.calls.atespaces))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.actors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.policies))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes))
	s.Equal([]string{"10.244.1.42"}, s.calls.get(&s.calls.probes))
	s.Empty(s.calls.get(&s.calls.suspends))

	s.Require().NotNil(running)
	s.Equal(v1alpha1.PhaseRunning, running.GetPhase())
	s.Equal("test-task", running.GetActor())
	s.Equal("10.244.1.42", running.GetWorkerIp())
	s.Nil(running.ExitCode)
	assertCondition(s.T(), running, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning")
	assertCondition(s.T(), running, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")
	assertCondition(s.T(), running, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionTrue,
		"PoliciesApplied")

	// Deleting the task releases the sandbox and then its templates.
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))
}

// The template name is derived from the task and workspace specs, so the same
// spec always addresses the same template.
func (s *taskWorkflowSuite) TestTemplateNameIsDerivedFromTheSpec() {
	s.delete(time.Second)
	in := testInput()
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	got, ok := s.calls.lastTemplateInput()
	s.Require().True(ok)
	s.Equal(activities.TaskTemplateName(in.Desired.Task, in.Desired.Workspaces, 0), got.Template.Name)
	s.Equal("default", got.Template.Atespace)
	s.Equal("default/test-task", got.WorkflowID)
	s.Equal(0, got.Generation)
}

func (s *taskWorkflowSuite) TestActorFailureRollsBackProvisioning() {
	s.calls.actorErr = temporal.NewNonRetryableApplicationError(
		"template does not exist", activities.ErrTypePermanent, nil)

	// Re-applying the same spec retries provisioning, and the answer carries the
	// state the task settled in.
	var failed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &failed, testInput().Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.NotEmpty(s.calls.get(&s.calls.actors))
	s.Empty(s.calls.get(&s.calls.resumes), "a task that failed to provision is never resumed")
	s.Contains(s.calls.get(&s.calls.delTmplOne),
		activities.TaskTemplateName(testInput().Desired.Task, testInput().Desired.Workspaces, 0),
		"the template the pass created is rolled back")
	s.Contains(s.calls.get(&s.calls.delTmpl), "test-task", "the teardown removes what is left")

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"ActorCreationFailed")
}

// A task bound to a gateway must not run without the gateway's allowlist.
func (s *taskWorkflowSuite) TestEgressFailureRollsBackARestrictedTask() {
	s.calls.policyErr = temporal.NewNonRetryableApplicationError(
		"policy rejected", activities.ErrTypePermanent, nil)

	var failed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &failed, testInput().Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Contains(s.calls.get(&s.calls.delPolicy), "test-task")
	s.Contains(s.calls.get(&s.calls.delActors), "test-task")
	s.Empty(s.calls.get(&s.calls.resumes))

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionGatewayReady, v1alpha1.ConditionFalse,
		"PolicyApplyFailed")
}

// Without a gateway a task keeps unrestricted egress, so a failure to apply the
// default policy is reported but does not stop the task.
func (s *taskWorkflowSuite) TestEgressFailureIsToleratedWithoutAGateway() {
	s.calls.policyErr = temporal.NewNonRetryableApplicationError(
		"policy rejected", activities.ErrTypePermanent, nil)

	in := testInput()
	in.Desired.Gateway = nil
	in.Desired.Task.Spec.Gateway = nil

	var applied *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &applied, in.Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes))
	s.Require().NotNil(applied)
	s.Equal(v1alpha1.PhaseRunning, applied.GetStatus().GetPhase())
	assertCondition(s.T(), applied.GetStatus(), v1alpha1.ConditionGatewayReady,
		v1alpha1.ConditionFalse, "PolicyApplyFailed")
}

func (s *taskWorkflowSuite) TestSuspendAndResume() {
	var suspendedTask, resumedTask *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateSuspend, "suspend-1", &suspendedTask)
	s.updateTask(2*time.Second, workflows.UpdateResume, "resume-1", &resumedTask)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends))
	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.resumes))
	// Suspending is not a spec change, so the sandbox is never rebuilt.
	s.Len(s.calls.get(&s.calls.actors), 1)
	s.Len(s.calls.get(&s.calls.templates), 1)

	s.Require().NotNil(suspendedTask)
	suspended := suspendedTask.GetStatus()
	s.Equal(v1alpha1.PhaseSuspended, suspended.GetPhase())
	s.Empty(suspended.GetWorkerIp())
	s.True(suspendedTask.GetSpec().GetSuspend())
	assertCondition(s.T(), suspended, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TaskSuspended")
	// Workspace setup is a one-time step whose result outlives a suspend.
	assertCondition(s.T(), suspended, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")

	s.Require().NotNil(resumedTask)
	resumed := resumedTask.GetStatus()
	s.Equal(v1alpha1.PhaseRunning, resumed.GetPhase())
	s.Equal("10.244.1.42", resumed.GetWorkerIp())
	s.False(resumedTask.GetSpec().GetSuspend())
	// The workspace is not probed again after a resume.
	s.Len(s.calls.get(&s.calls.probes), 1)
}

func (s *taskWorkflowSuite) TestCompleteRecordsTheExitCode() {
	var completed *v1alpha1.TaskStatus
	var deletedWhileCompleted []string

	s.updateStatus(time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 3, Message: "agent exited with code 3"})
	s.env.RegisterDelayedCallback(func() {
		deletedWhileCompleted = s.calls.get(&s.calls.delActors)
	}, 2*time.Second)
	s.delete(3 * time.Second)

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

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(workflows.SignalComplete, workflows.CompleteInput{ExitCode: 0})
	}, time.Second)
	s.env.RegisterDelayedCallback(func() {
		completed = s.queryStatus()
	}, 2*time.Second)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Require().NotNil(completed.ExitCode)
	s.Equal(int32(0), completed.GetExitCode())
}

func (s *taskWorkflowSuite) TestDeleteTearsDownAndRejectsFurtherChanges() {
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflowNoRejection(workflows.UpdateDelete, "delete-1", s.T())
	}, time.Second)
	s.env.RegisterDelayedCallback(func() {
		// A task that is going away stops accepting changes.
		rejected := false
		s.env.UpdateWorkflow(workflows.UpdateSuspend, "suspend-late", &testsuite.TestUpdateCallback{
			OnReject:   func(error) { rejected = true },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		})
		s.True(rejected, "suspend should be rejected while the task is terminating")
	}, 2*time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))

	// The task ends on the Terminating phase, and its execution has closed, which
	// is what tells the API server the task is gone.
	value, err := s.env.QueryWorkflow(workflows.QueryTask)
	s.Require().NoError(err)
	var task v1alpha1.Task
	s.Require().NoError(value.Get(&task))
	s.Equal(v1alpha1.PhaseTerminating, task.GetStatus().GetPhase())
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

// A resync notices a sandbox that crashed behind the workflow's back and builds
// a new one.
func (s *taskWorkflowSuite) TestResyncReplacesACrashedSandbox() {
	s.calls.observation = &activities.ActorObservation{
		Exists: true,
		State:  activities.ActorStateCrashed,
	}
	var replaced *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		replaced = s.queryStatus()
	}, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.NotEmpty(s.calls.get(&s.calls.observes))
	s.GreaterOrEqual(len(s.calls.get(&s.calls.actors)), 2, "the crashed sandbox is provisioned again")
	s.GreaterOrEqual(len(s.calls.get(&s.calls.resumes)), 2)
	s.Require().NotNil(replaced)
	s.Equal(v1alpha1.PhaseRunning, replaced.GetPhase())
}

// A task carries its status and its sandbox generation across a continue-as-new
// boundary, so a long-lived task does not provision itself from scratch again
// and does not take its own sandbox for one that has to be replaced.
func (s *taskWorkflowSuite) TestContinuedRunKeepsTheSandbox() {
	in := testInput()
	in.Generation = 2
	in.Status = &v1alpha1.TaskStatus{
		Phase:    v1alpha1.PhaseRunning,
		Actor:    "test-task",
		Id:       "task-test-task-1",
		WorkerIp: "10.244.1.42",
		Conditions: []*v1alpha1.Condition{{
			Type:   v1alpha1.ConditionWorkspaceReady,
			Status: v1alpha1.ConditionTrue,
			Reason: "SetupComplete",
		}},
	}
	// The sandbox the previous run built is there, on the template of its
	// generation.
	template := activities.TaskTemplateName(in.Desired.Task, in.Desired.Workspaces, 2)
	s.calls.actorExists = true
	s.calls.actorTemplate = template
	s.calls.actorState = activities.ActorStateRunning
	s.calls.madeTemplates[template] = true

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		status = s.queryStatus()
	}, time.Second)
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
	var appErr *temporal.ApplicationError
	s.Require().ErrorAs(err, &appErr)
	s.Equal(workflows.ErrTypeInvalidTask, appErr.Type())
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
	s.calls.observation = &activities.ActorObservation{
		Exists:   true,
		State:    activities.ActorStateRunning,
		WorkerIP: "10.244.1.42",
	}

	var initializing, ready *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		initializing = s.queryStatus()
		// The workspace finishes setting up while the task is running.
		s.calls.mu.Lock()
		s.calls.workspaceReady = true
		s.calls.mu.Unlock()
	}, time.Second)
	s.env.RegisterDelayedCallback(func() {
		ready = s.queryStatus()
	}, 6*time.Minute)
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

// Substrate binds an actor to the template it was created from, so applying a
// new spec has to replace the sandbox for the change to take effect.
func (s *taskWorkflowSuite) TestApplyReplacesTheSandboxForANewSpec() {
	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"

	var replaced *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &replaced, changed.Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	templates := s.calls.get(&s.calls.templates)
	s.Require().GreaterOrEqual(len(templates), 2)
	s.NotEqual(templates[0], templates[len(templates)-1], "a new image is a new template")
	// The replacement is the task's second sandbox, and its template says so.
	s.Equal(
		activities.TaskTemplateName(changed.Desired.Task, changed.Desired.Workspaces, 1),
		templates[len(templates)-1],
	)
	// One delete to replace the sandbox, one to tear the task down.
	s.Len(s.calls.get(&s.calls.delActors), 2)
	s.Len(s.calls.get(&s.calls.probes), 2, "the replacement sandbox is probed for itself")

	s.Require().NotNil(replaced)
	s.Equal("ghcr.io/example/agent:v2", replaced.GetSpec().GetImage())
	s.Equal(v1alpha1.PhaseRunning, replaced.GetStatus().GetPhase())
}

// A pass that finds a sandbox it did not create and cannot restrict stops it.
// Deleting it would throw away the workspace; leaving it running would let the
// task run without the allowlist it was given.
func (s *taskWorkflowSuite) TestReprovisionStopsASandboxItCannotRestrict() {
	// A different gateway changes what has to be applied to the sandbox, not
	// what the sandbox is made of, so the actor and its template stay.
	changed := testInput()
	changed.Desired.Gateway.Spec.Egress.Allowlist.Hosts = []*v1alpha1.HostRule{
		{Host: "example.com", Port: 443},
	}

	s.env.RegisterDelayedCallback(func() {
		s.calls.mu.Lock()
		s.calls.policyErr = temporal.NewNonRetryableApplicationError(
			"policy rejected", activities.ErrTypePermanent, nil)
		s.calls.mu.Unlock()
	}, time.Second)
	var failed *v1alpha1.Task
	s.updateTask(2*time.Second, workflows.UpdateApply, "apply-1", &failed, changed.Desired)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(),
		v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "PolicyApplyFailed")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends),
		"the sandbox is stopped rather than left running without its allowlist")
	s.Len(s.calls.get(&s.calls.delActors), 1, "only the teardown deletes the actor")
	s.Empty(s.calls.get(&s.calls.delTmplOne), "a template the pass found is left alone")
	s.Empty(s.calls.get(&s.calls.delPolicy), "the policy of a sandbox the pass found is left alone")
}

// What a rollback may delete is settled by looking before the pass creates
// anything, not by what the create calls answer: a retried create finds what
// its first attempt made and answers "found".
func (s *taskWorkflowSuite) TestRollbackLeavesATemplateThatWasThereBeforeThePass() {
	in := testInput()
	template := activities.TaskTemplateName(in.Desired.Task, in.Desired.Workspaces, 0)
	s.calls.madeTemplates[template] = true
	s.calls.actorErr = temporal.NewNonRetryableApplicationError(
		"template does not exist", activities.ErrTypePermanent, nil)

	s.delete(time.Second)
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{template}, s.calls.get(&s.calls.templates))
	s.Empty(s.calls.get(&s.calls.delTmplOne), "a template that was there before is not rolled back")
	s.NotEmpty(s.calls.get(&s.calls.delActors), "the actor the pass tried to make is")
}

// A spec that arrives while a pass is running belongs to the next pass, not to
// half of this one.
func (s *taskWorkflowSuite) TestApplyDuringProvisioningIsNotHalfApplied() {
	gate := make(chan struct{})
	s.calls.templateGate = gate

	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"

	var applied *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &applied, changed.Desired)
	s.env.RegisterDelayedCallback(func() { close(gate) }, 2*time.Second)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	last, ok := s.calls.lastTemplateInput()
	s.Require().True(ok)
	s.Equal("ghcr.io/example/agent:v2", last.Image, "the new spec is built by a pass of its own")
	s.Equal(
		activities.TaskTemplateName(changed.Desired.Task, changed.Desired.Workspaces, 1),
		last.Template.Name,
	)
	s.Require().NotNil(applied)
	s.Equal("ghcr.io/example/agent:v2", applied.GetSpec().GetImage())
}

// An answer from a sandbox that has since been replaced says nothing about its
// replacement.
func (s *taskWorkflowSuite) TestAProbeForAReplacedSandboxIsDropped() {
	gate := make(chan struct{})
	s.calls.probeGate = gate
	// The first sandbox reports not ready, the replacement reports ready, and
	// both answers arrive after the replacement.
	s.calls.probeAnswers = []bool{false, true}

	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"

	var replaced *v1alpha1.Task
	var settled *v1alpha1.TaskStatus
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &replaced, changed.Desired)
	s.env.RegisterDelayedCallback(func() { close(gate) }, 2*time.Second)
	s.env.RegisterDelayedCallback(func() { settled = s.queryStatus() }, 3*time.Second)
	s.delete(4 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Len(s.calls.get(&s.calls.probes), 2, "the replacement gets a probe of its own")
	s.Require().NotNil(settled)
	assertCondition(s.T(), settled, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue,
		"SetupComplete")
}

// A task that continues as new answers the update it was given first.
func (s *taskWorkflowSuite) TestContinueAsNewFinishesAnAdmittedUpdate() {
	s.env.SetContinueAsNewSuggested(true)

	var suspended *v1alpha1.Task
	s.updateTask(0, workflows.UpdateSuspend, "suspend-1", &suspended)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	var continued *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &continued)

	s.Require().NotNil(suspended, "the update is answered before the run ends")
	s.Equal(v1alpha1.PhaseSuspended, suspended.GetStatus().GetPhase())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.suspends), "and its effect landed")
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
	s.calls.deleteActorErr = temporal.NewNonRetryableApplicationError(
		"actor is wedged", activities.ErrTypePermanent, nil)

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
		s.calls.mu.Lock()
		s.calls.deleteActorErr = nil
		s.calls.mu.Unlock()
		s.env.UpdateWorkflowNoRejection(workflows.UpdateDelete, "delete-2", s.T())
	}, 2*time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "the second delete released the task")

	s.Require().Error(deleteErr, "a delete that could not release the sandbox says so")
	s.Contains(deleteErr.Error(), "still there")

	s.Require().NotNil(stillThere)
	s.Equal(v1alpha1.PhaseFailed, stillThere.GetPhase(), "the task stays, and says it could not go")
	assertCondition(s.T(), stillThere, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
		"TeardownFailed")

	s.Len(s.calls.get(&s.calls.delActors), 2, "the delete was tried again")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl), "templates go once the actor has")
}

// A task started together with the update that carries its spec waits for it
// rather than assuming one.
func (s *taskWorkflowSuite) TestAStartWithNoSpecWaitsForTheApply() {
	var applied *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &applied, testInput().Desired)
	s.delete(2 * time.Second)

	// The start carries nothing, which is what update-with-start sends.
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(applied)
	s.Equal("test-task", applied.GetMetadata().GetName())
	s.Equal(v1alpha1.PhaseRunning, applied.GetStatus().GetPhase())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.actors))
}

// A completion report names the sandbox it comes from. One from a sandbox the
// task has since replaced says how that sandbox's command ended, not how the
// current one is doing, and is refused on both paths it can arrive by.
func (s *taskWorkflowSuite) TestACompletionFromAReplacedSandboxIsRefused() {
	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"

	var replaced *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &replaced, changed.Desired)

	rejected := false
	s.env.RegisterDelayedCallback(func() {
		stale := workflows.CompleteInput{ExitCode: -1, Message: "stopped", Generation: 0}
		s.env.UpdateWorkflow(workflows.UpdateComplete, "complete-stale", &testsuite.TestUpdateCallback{
			OnReject:   func(error) { rejected = true },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		}, stale)
		// The signal path has no validator, so the same report is dropped there.
		s.env.SignalWorkflow(workflows.SignalComplete, stale)
	}, 2*time.Second)
	var afterStale *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { afterStale = s.queryStatus() }, 3*time.Second)

	var completed *v1alpha1.TaskStatus
	s.updateStatus(4*time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 0, Generation: 1})
	s.delete(5 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.True(rejected, "a report from the replaced sandbox is refused")
	s.Require().NotNil(afterStale)
	s.Equal(v1alpha1.PhaseRunning, afterStale.GetPhase())
	s.Nil(afterStale.ExitCode)

	s.Require().NotNil(completed, "a report from the current sandbox is taken")
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Equal(int32(0), completed.GetExitCode())
}

// A replacement sandbox has not run its command yet, so how the old one's
// command ended does not carry over to it.
func (s *taskWorkflowSuite) TestReplacingTheSandboxResetsCompletion() {
	var completed *v1alpha1.TaskStatus
	s.updateStatus(time.Second, workflows.UpdateComplete, "complete-1", &completed,
		workflows.CompleteInput{ExitCode: 3})

	changed := testInput()
	changed.Desired.Task.Spec.Image = "ghcr.io/example/agent:v2"
	var replaced *v1alpha1.Task
	s.updateTask(2*time.Second, workflows.UpdateApply, "apply-1", &replaced, changed.Desired)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Require().NotNil(replaced)
	s.Equal(v1alpha1.PhaseRunning, replaced.GetStatus().GetPhase())
	s.Nil(replaced.GetStatus().ExitCode)
}

// A spec whose identity names another workflow cannot be applied here, whatever
// client sent it: the actor would land in one atespace while the ID said
// another.
func (s *taskWorkflowSuite) TestApplyRejectsASpecForAnotherTask() {
	other := testInput().Desired
	other.Task.Metadata.Atespace = "team-b"

	rejected := false
	s.env.RegisterDelayedCallback(func() {
		s.env.UpdateWorkflow(workflows.UpdateApply, "apply-other", &testsuite.TestUpdateCallback{
			OnReject:   func(error) { rejected = true },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		}, other)
	}, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.True(rejected, "a spec for another task is refused")
	s.Equal([]string{"default"}, s.calls.get(&s.calls.atespaces),
		"nothing is provisioned in the other atespace")
}

// Before the first apply there is no spec to change and no sandbox to report
// on, so those updates are refused rather than dereferencing nothing.
func (s *taskWorkflowSuite) TestChangesBeforeTheFirstApplyAreRejected() {
	rejected := map[string]bool{}
	refuse := func(update string, args ...any) {
		s.env.UpdateWorkflow(update, update+"-early", &testsuite.TestUpdateCallback{
			OnReject:   func(error) { rejected[update] = true },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		}, args...)
	}
	s.env.RegisterDelayedCallback(func() {
		refuse(workflows.UpdateSuspend)
		refuse(workflows.UpdateResume)
		refuse(workflows.UpdateComplete, workflows.CompleteInput{ExitCode: 0})
	}, time.Second)

	// The start carries nothing and the apply never comes, so the task gives up.
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})
	s.True(s.env.IsWorkflowCompleted())

	early := []string{workflows.UpdateSuspend, workflows.UpdateResume, workflows.UpdateComplete}
	for _, update := range early {
		s.True(rejected[update], "%s before the first apply should be rejected", update)
	}
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
	assertCondition(s.T(), status, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TeardownFailed")
}

// The run that continues as new hands a teardown failure on to the next one.
func (s *taskWorkflowSuite) TestContinueAsNewCarriesATeardownFailure() {
	s.env.SetContinueAsNewSuggested(true)
	s.calls.deleteActorErr = temporal.NewNonRetryableApplicationError(
		"actor is wedged", activities.ErrTypePermanent, nil)

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
	in := testInput()
	in.Desired.Task.Spec.Suspend = true
	s.calls.observation = &activities.ActorObservation{
		Exists:   true,
		State:    activities.ActorStateRunning,
		WorkerIP: "10.244.1.42",
	}

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() { status = s.queryStatus() }, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, in)
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task", "test-task"}, s.calls.get(&s.calls.suspends),
		"the resync suspends the sandbox again")
	s.Require().NotNil(status)
	s.Equal(v1alpha1.PhaseSuspended, status.GetPhase())
}

// A start that is never followed by its update does not sit there forever.
func (s *taskWorkflowSuite) TestAStartWithNoSpecGivesUp() {
	s.env.ExecuteWorkflow(workflows.TaskWorkflowType, workflows.TaskWorkflowInput{})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Require().Error(err)
	var appErr *temporal.ApplicationError
	s.Require().ErrorAs(err, &appErr)
	s.Equal(workflows.ErrTypeInvalidTask, appErr.Type())
	s.Empty(s.calls.get(&s.calls.atespaces), "nothing is provisioned for a task that never arrived")
}
