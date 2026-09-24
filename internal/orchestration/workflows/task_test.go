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
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

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
	templateIn []activities.TemplateInput

	// failures let a test make one step fail.
	templateErr error
	actorErr    error
	policyErr   error
	resumeErr   error
	// workspaceReady is what the readiness poll reports.
	workspaceReady bool
	// observation is what a resync sees.
	observation activities.ActorObservation
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
	s.calls = &calls{workspaceReady: true}
	s.mockActivities()
}

// updateTask sends an update and captures the task its handler answered with.
// Reading state through an update is deterministic: the handler only returns
// once the change has been driven into Substrate.
func (s *taskWorkflowSuite) updateTask(at time.Duration, name, id string, got **v1alpha1.Task, args ...any) {
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
func (s *taskWorkflowSuite) updateStatus(at time.Duration, name, id string, got **v1alpha1.TaskStatus, args ...any) {
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
		func(ctx context.Context, in activities.TemplateInput) (activities.TemplateRef, error) {
			c.mu.Lock()
			c.templates = append(c.templates, in.Template.Name)
			c.templateIn = append(c.templateIn, in)
			err := c.templateErr
			c.mu.Unlock()
			if err != nil {
				return activities.TemplateRef{}, err
			}
			return in.Template, nil
		}).Maybe()

	s.env.OnActivity(a.EnsureActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorInput) error {
			c.add(&c.actors, in.Actor.Name)
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.actorErr
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
			c.add(&c.suspends, in.Name)
			return nil
		}).Maybe()

	s.env.OnActivity(a.ResumeActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) (string, error) {
			c.add(&c.resumes, in.Name)
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.resumeErr != nil {
				return "", c.resumeErr
			}
			return "10.244.1.42", nil
		}).Maybe()

	s.env.OnActivity(a.AwaitWorkspaceReady, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.WorkspaceReadyInput) (bool, error) {
			c.add(&c.probes, in.WorkerIP)
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.workspaceReady, nil
		}).Maybe()

	s.env.OnActivity(a.ObserveActor, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) (activities.ActorObservation, error) {
			c.add(&c.observes, in.Name)
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.observation, nil
		}).Maybe()

	s.env.OnActivity(a.DeleteActorIfExists, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in activities.ActorRef) error {
			c.add(&c.delActors, in.Name)
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())

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
	assertCondition(s.T(), running, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue, "SetupComplete")
	assertCondition(s.T(), running, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionTrue, "PoliciesApplied")

	// Deleting the task releases the sandbox and then its templates.
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))
}

// The template name is derived from the task and workspace specs, so the same
// spec always addresses the same template.
func (s *taskWorkflowSuite) TestTemplateNameIsDerivedFromTheSpec() {
	s.delete(time.Second)
	in := testInput()
	s.env.ExecuteWorkflow(workflows.TaskWorkflow, in)
	s.Require().NoError(s.env.GetWorkflowError())

	got, ok := s.calls.lastTemplateInput()
	s.Require().True(ok)
	s.Equal(activities.TaskTemplateName(in.Desired.Task, in.Desired.Workspaces), got.Template.Name)
	s.Equal("default", got.Template.Atespace)
	s.Equal("default/test-task", got.WorkflowID)
}

func (s *taskWorkflowSuite) TestActorFailureRollsBackProvisioning() {
	s.calls.actorErr = temporal.NewNonRetryableApplicationError(
		"template does not exist", activities.ErrTypePermanent, nil)

	// Re-applying the same spec retries provisioning, and the answer carries the
	// state the task settled in.
	var failed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &failed, testInput().Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.NotEmpty(s.calls.get(&s.calls.actors))
	s.Empty(s.calls.get(&s.calls.resumes), "a task that failed to provision is never resumed")
	s.Contains(s.calls.get(&s.calls.delTmpl), "test-task", "the actor template is rolled back")

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorCreationFailed")
}

// A task bound to a gateway must not run without the gateway's allowlist.
func (s *taskWorkflowSuite) TestEgressFailureRollsBackARestrictedTask() {
	s.calls.policyErr = temporal.NewNonRetryableApplicationError(
		"policy rejected", activities.ErrTypePermanent, nil)

	var failed *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateApply, "apply-1", &failed, testInput().Desired)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Contains(s.calls.get(&s.calls.delPolicy), "test-task")
	s.Contains(s.calls.get(&s.calls.delActors), "test-task")
	s.Empty(s.calls.get(&s.calls.resumes))

	s.Require().NotNil(failed)
	s.Equal(v1alpha1.PhaseFailed, failed.GetStatus().GetPhase())
	assertCondition(s.T(), failed.GetStatus(), v1alpha1.ConditionGatewayReady, v1alpha1.ConditionFalse, "PolicyApplyFailed")
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, in)
	s.Require().NoError(s.env.GetWorkflowError())

	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.resumes))
	s.Require().NotNil(applied)
	s.Equal(v1alpha1.PhaseRunning, applied.GetStatus().GetPhase())
	assertCondition(s.T(), applied.GetStatus(), v1alpha1.ConditionGatewayReady, v1alpha1.ConditionFalse, "PolicyApplyFailed")
}

func (s *taskWorkflowSuite) TestSuspendAndResume() {
	var suspendedTask, resumedTask *v1alpha1.Task
	s.updateTask(time.Second, workflows.UpdateSuspend, "suspend-1", &suspendedTask)
	s.updateTask(2*time.Second, workflows.UpdateResume, "resume-1", &resumedTask)
	s.delete(3 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
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
	assertCondition(s.T(), suspended, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TaskSuspended")
	// Workspace setup is a one-time step whose result outlives a suspend.
	assertCondition(s.T(), suspended, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue, "SetupComplete")

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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(completed)
	s.Equal(v1alpha1.PhaseCompleted, completed.GetPhase())
	s.Require().NotNil(completed.ExitCode)
	s.Equal(int32(3), completed.GetExitCode())
	assertCondition(s.T(), completed, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "CommandExited")
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors))
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delTmpl))

	// Once the task is gone the query reports it as such.
	_, err := s.env.QueryWorkflow(workflows.QueryTask)
	s.Require().Error(err)
	s.Contains(err.Error(), "no longer exists")
}

func (s *taskWorkflowSuite) TestCancellationReleasesTheSandbox() {
	s.env.RegisterDelayedCallback(func() {
		s.env.CancelWorkflow()
	}, time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError(), "a cancelled workflow reports the cancellation")
	s.Equal([]string{"test-task"}, s.calls.get(&s.calls.delActors),
		"cancellation releases the sandbox through a disconnected context")
}

// A resync notices a sandbox that crashed behind the workflow's back and builds
// a new one.
func (s *taskWorkflowSuite) TestResyncReplacesACrashedSandbox() {
	s.calls.observation = activities.ActorObservation{
		Exists: true,
		State:  activities.ActorStateCrashed,
	}
	var replaced *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		replaced = s.queryStatus()
	}, 6*time.Minute)
	s.delete(6*time.Minute + time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.NotEmpty(s.calls.get(&s.calls.observes))
	s.GreaterOrEqual(len(s.calls.get(&s.calls.actors)), 2, "the crashed sandbox is provisioned again")
	s.GreaterOrEqual(len(s.calls.get(&s.calls.resumes)), 2)
	s.Require().NotNil(replaced)
	s.Equal(v1alpha1.PhaseRunning, replaced.GetPhase())
}

// A task carries its status across a continue-as-new boundary, so a long-lived
// task does not provision itself from scratch again.
func (s *taskWorkflowSuite) TestContinuedRunKeepsTheSandbox() {
	in := testInput()
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

	var status *v1alpha1.TaskStatus
	s.env.RegisterDelayedCallback(func() {
		status = s.queryStatus()
	}, time.Second)
	s.delete(2 * time.Second)

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, in)
	s.Require().NoError(s.env.GetWorkflowError())

	// Provisioning runs again because it is idempotent, but the workspace is not
	// probed a second time and the task keeps its identity.
	s.Empty(s.calls.get(&s.calls.probes))
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, in)

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Require().Error(err)
	var appErr *temporal.ApplicationError
	s.Require().ErrorAs(err, &appErr)
	s.Equal(workflows.ErrTypeInvalidTask, appErr.Type())
	s.Empty(s.calls.get(&s.calls.atespaces), "nothing is provisioned for a task that cannot run")
}

func assertCondition(t *testing.T, status *v1alpha1.TaskStatus, condType, wantStatus, wantReason string) {
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
	s.calls.observation = activities.ActorObservation{
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

	s.env.ExecuteWorkflow(workflows.TaskWorkflow, testInput())
	s.Require().NoError(s.env.GetWorkflowError())

	s.Require().NotNil(initializing)
	assertCondition(s.T(), initializing, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "Initializing")
	assertCondition(s.T(), initializing, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "WorkspaceInitializing")

	s.Len(s.calls.get(&s.calls.probes), 2, "the resync probes the workspace again")
	s.Require().NotNil(ready)
	assertCondition(s.T(), ready, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue, "SetupComplete")
	assertCondition(s.T(), ready, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning")
	// The sandbox itself was never rebuilt.
	s.Len(s.calls.get(&s.calls.actors), 1)
}
