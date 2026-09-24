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

package workflows

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// provisioningChangeID names the provisioning sequence for versioning. The
	// marker is recorded on every run, so changing the sequence later is a matter
	// of raising provisioningVersion and branching on the recorded value.
	provisioningChangeID = "provisioning"
	provisioningVersion  = 1

	// defaultResyncInterval is how often a settled task checks that its sandbox
	// is still what the task says it should be. A sandbox can crash or be
	// rescheduled without anyone telling AX, and nothing else would notice.
	defaultResyncInterval = 5 * time.Minute
)

// Config is the worker-level tuning of task workflows. It is bound to the
// workflow when a worker registers it, so a change applies to the timers a
// running task starts from then on.
type Config struct {
	// ResyncInterval is how often a settled task checks its sandbox. Each
	// interval costs one read per task, so a large fleet wants a larger value.
	ResyncInterval time.Duration
}

// DefaultConfig returns the settings a worker uses unless it says otherwise.
func DefaultConfig() Config {
	return Config{ResyncInterval: defaultResyncInterval}
}

func (c Config) withDefaults() Config {
	if c.ResyncInterval <= 0 {
		c.ResyncInterval = defaultResyncInterval
	}
	return c
}

// NewTaskWorkflow returns the task workflow with a worker's settings bound to
// it. Register it under TaskWorkflowType so that every worker answers for the
// same workflow type whatever its settings are.
func NewTaskWorkflow(cfg Config) func(workflow.Context, TaskWorkflowInput) error {
	cfg = cfg.withDefaults()
	return func(ctx workflow.Context, in TaskWorkflowInput) error {
		return runTask(ctx, cfg, in)
	}
}

// sandboxState is what the workflow believes it has done to the sandbox. It
// starts unknown after provisioning, because a fresh actor has to be told to
// run even if the task was never suspended.
type sandboxState int

const (
	sandboxUnknown sandboxState = iota
	sandboxRunning
	sandboxSuspended
)

// acts names the activity methods the workflow calls. It is never dereferenced;
// the SDK reads the method names off it and resolves them on the worker.
var acts *activities.Activities

// runTask orchestrates one AX task for as long as the task exists.
//
// It provisions the sandbox, keeps it in the state the spec asks for, and
// answers questions about it. A task that has been provisioned stays in the
// loop until it is deleted, so suspending, resuming, reporting a command exit,
// and deleting are all handled by the same execution that created the sandbox.
func runTask(ctx workflow.Context, cfg Config, in TaskWorkflowInput) error {
	r, err := newTaskRun(ctx, in)
	if err != nil {
		return err
	}
	if err := r.registerHandlers(ctx); err != nil {
		return err
	}

	logger := workflow.GetLogger(ctx)
	logger.Info("task workflow started", "task", r.key(), "phase", r.status.GetPhase())

	if v := workflow.GetVersion(ctx, provisioningChangeID, provisioningVersion, provisioningVersion); v != provisioningVersion {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task was provisioned by version %d, which this worker does not support", v),
			ErrTypeUnsupportedVersion, nil)
	}

	// A cancelled task is a deleted task: the sandbox must not outlive the
	// workflow that owns it.
	defer r.releaseOnCancel(ctx)

	// The outer turn is the task's life: it ends only when the sandbox is
	// really gone. A teardown that cannot finish puts the task back in the
	// inner loop, where a later delete tries again.
	for !r.deleted {
		for !r.deleting {
			handling := r.requests
			r.converge(ctx)
			r.handled = handling

			if r.deleting {
				break
			}
			// Workspace setup inside the sandbox takes as long as a maiden run
			// takes, so it is waited for in the background: a caller asking for a
			// change gets an answer as soon as the sandbox is in the state it
			// asked for.
			r.settleWorkspace(ctx)
			if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
				r.drainCompletions(ctx)
				if r.pending() {
					continue
				}
				// Wait for handlers to finish, but not past one that is waiting
				// for the main loop: that one needs another pass, and waiting for
				// it here would leave both sides waiting for each other.
				if err := workflow.Await(ctx, func() bool {
					return workflow.AllHandlersFinished(ctx) || r.pending()
				}); err != nil {
					return err
				}
				if r.pending() {
					continue
				}
				// Anything signalled while we yielded above would go down with
				// this run, so the channel is taken again on the way out.
				r.drainCompletions(ctx)
				logger.Info("continuing task workflow as new", "task", r.key())
				return workflow.NewContinueAsNewError(ctx, TaskWorkflowType, r.continueInput())
			}

			requested, err := workflow.AwaitWithTimeout(ctx, cfg.ResyncInterval, r.pending)
			if err != nil {
				return err
			}
			if !requested {
				r.resync(ctx)
			}
		}

		if err := r.teardown(ctx); err != nil {
			logger.Error("task teardown failed; the task stays until it can be released",
				"task", r.key(), "error", err)
		}
	}

	if err := r.awaitHandlers(ctx); err != nil {
		return err
	}
	logger.Info("task deleted", "task", r.key())
	return nil
}

// taskRun is the state of one task workflow execution. Only the workflow's own
// coroutines touch it, so no locking is involved.
type taskRun struct {
	// desired is what the task should be. Updates replace it.
	desired *TaskDesiredState
	// provisioned is the desired state the current sandbox was built from. A
	// difference from desired is what triggers provisioning again.
	provisioned *TaskDesiredState
	status      *v1alpha1.TaskStatus

	// requests counts state changes asked for, handled counts the ones already
	// driven into Substrate. An update handler returns once its own request has
	// been handled, so the caller sees the result of its change.
	requests int
	handled  int

	sandbox         sandboxState
	workspaceProbed bool
	// probing is set while the background workspace poll is running, so only one
	// runs at a time.
	probing bool
	// generation counts the sandboxes this task has had. A probe carries the
	// generation it was started for, and its answer is dropped when the sandbox
	// it was talking to has since been replaced.
	generation int
	completed  bool
	failed     bool
	deleting   bool
	deleted    bool
	// teardownFailures counts the teardowns that could not finish, so the
	// caller waiting on a delete is released by its own attempt failing.
	teardownFailures int
	teardownMessage  string
	// teardownFailed keeps a task that could not be released reported as
	// Failed, rather than sliding back to Running as if nothing had happened.
	teardownFailed bool

	completions workflow.ReceiveChannel
}

// newTaskRun validates the input and puts the task into the shape the rest of
// the workflow expects.
func newTaskRun(ctx workflow.Context, in TaskWorkflowInput) (*taskRun, error) {
	if in.Desired == nil || in.Desired.Task == nil {
		return nil, temporal.NewNonRetryableApplicationError("task is required", ErrTypeInvalidTask, nil)
	}
	if in.Desired.Task.GetMetadata().GetName() == "" {
		return nil, temporal.NewNonRetryableApplicationError("task name is required", ErrTypeInvalidTask, nil)
	}
	if err := v1alpha1.ValidateTask(in.Desired.Task); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalidTask, nil)
	}

	desired := normalizeDesired(in.Desired)
	r := &taskRun{
		desired: desired,
		status:  in.Status,
	}
	if r.status == nil {
		r.status = &v1alpha1.TaskStatus{}
	}
	// The actor carries the task's name so that the two are interchangeable, for
	// example in the router's ate-target-actor header. Whatever a client put in
	// status.actor is overwritten.
	r.status.Actor = desired.Task.GetMetadata().GetName()
	if r.status.Id == "" {
		r.status.Id = fmt.Sprintf("task-%s-%d", desired.Task.GetMetadata().GetName(), workflow.Now(ctx).Unix())
	}
	if r.conditionTrue(v1alpha1.ConditionWorkspaceReady) {
		// Workspace setup happens once per task and its result outlives suspends,
		// actor failures, and continue-as-new.
		r.workspaceProbed = true
	}
	if r.status.GetPhase() == v1alpha1.PhaseCompleted || r.status.ExitCode != nil {
		r.completed = true
	}
	r.syncPhase()
	return r, nil
}

// normalizeDesired fills in the defaults a task manifest may leave out, so the
// rest of the workflow can read the spec without repeating them.
func normalizeDesired(in *TaskDesiredState) *TaskDesiredState {
	out := &TaskDesiredState{
		Task:       cloneTask(in.Task),
		Gateway:    in.Gateway,
		Workspaces: in.Workspaces,
	}
	if out.Task.Metadata == nil {
		out.Task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if out.Task.Metadata.Atespace == "" {
		out.Task.Metadata.Atespace = v1alpha1.DefaultAtespace
	}
	if out.Task.Spec == nil {
		out.Task.Spec = &v1alpha1.TaskSpec{}
	}
	if out.Task.Spec.Image == "" {
		out.Task.Spec.Image = v1alpha1.DefaultTaskImage
	}
	out.Task.Status = nil
	return out
}

// converge drives the sandbox towards the desired state. It is safe to call
// with nothing to do, which is what makes a resync cheap.
func (r *taskRun) converge(ctx workflow.Context) {
	if r.deleting {
		return
	}
	err := r.reconcile(ctx)
	r.failed = err != nil
	if err != nil {
		workflow.GetLogger(ctx).Error("task did not converge", "task", r.key(), "error", err)
	}
	r.syncPhase()
}

func (r *taskRun) reconcile(ctx workflow.Context) error {
	if !sameProvisioning(r.desired, r.provisioned) {
		if err := r.provision(ctx); err != nil {
			return err
		}
	}
	return r.activate(ctx)
}

// provision creates the Substrate resources a task needs, in order, and rolls
// back what it created if the sequence cannot be finished.
//
// Every step is idempotent and keyed by the task, so a retried or replayed step
// addresses the same resource. Compensations are registered before the step
// they undo, because a step whose side effect landed can still fail on the way
// back, and each one only undoes what this pass made: rolling back must never
// take a sandbox that was already there.
func (r *taskRun) provision(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	// One pass builds one sandbox. An apply that lands while provisioning is in
	// flight is left to the next pass instead of being half applied by this one.
	desired := cloneDesired(r.desired)
	actor := activities.ActorRef{
		Atespace: desired.Task.GetMetadata().GetAtespace(),
		Name:     desired.Task.GetMetadata().GetName(),
	}
	template := activities.TemplateRef{
		Atespace: actor.Atespace,
		Name:     activities.TaskTemplateName(desired.Task, desired.Workspaces),
	}
	provisionCtx := workflow.WithActivityOptions(ctx, activities.ProvisionOptions())

	// Only what this pass created may be rolled back by it, tracked per resource:
	// a pass over a task that already has a sandbox must leave it alone.
	templateCreated, actorCreated := false, false
	var saga compensations

	logger.Info("provisioning task sandbox", "task", r.key(), "image", desired.Task.GetSpec().GetImage())

	// The atespace is shared by every task in it, so it is never rolled back.
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureAtespace,
		activities.AtespaceInput{Atespace: actor.Atespace}).Get(provisionCtx, nil); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "AtespaceCreationFailed", err.Error())
		return err
	}

	saga.add("actor template", func(ctx workflow.Context) error {
		if !templateCreated {
			return nil
		}
		return workflow.ExecuteActivity(ctx, acts.DeleteActorTemplateIfExists, template).Get(ctx, nil)
	})
	var provisionedTemplate activities.TemplateProvision
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureActorTemplate, activities.TemplateInput{
		Template:   template,
		Image:      desired.Task.GetSpec().GetImage(),
		Task:       desired.Task,
		Workspaces: desired.Workspaces,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
	}).Get(provisionCtx, &provisionedTemplate); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TemplateCreationFailed", err.Error())
		return err
	}
	templateCreated = provisionedTemplate.Created
	resolved := provisionedTemplate.Template

	saga.add("actor", func(ctx workflow.Context) error {
		if !actorCreated {
			return nil
		}
		return workflow.ExecuteActivity(ctx, acts.DeleteActorIfExists, actor).Get(ctx, nil)
	})
	actorCtx := workflow.WithActivityOptions(ctx, activities.ActorOptions())
	var placed activities.ActorProvision
	if err := workflow.ExecuteActivity(actorCtx, acts.EnsureActor,
		activities.ActorInput{Actor: actor, Template: resolved}).Get(actorCtx, &placed); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorCreationFailed", err.Error())
		return err
	}
	actorCreated = placed.Created

	if placed.Template.Name != resolved.Name {
		// Substrate binds an actor to the template it was created from, so a
		// changed spec is only adopted by a new sandbox. The old one is replaced,
		// and whatever it had in its workspace goes with it.
		logger.Info("replacing the task sandbox to adopt a new spec",
			"task", r.key(), "from", placed.Template.Name, "to", resolved.Name)
		r.forgetSandbox(ctx, "SandboxReplaced", "Sandbox is being replaced to adopt a new task spec")

		teardownCtx := workflow.WithActivityOptions(ctx, activities.TeardownOptions())
		if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorIfExists, actor).Get(teardownCtx, nil); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorReplaceFailed", err.Error())
			return err
		}
		if err := workflow.ExecuteActivity(actorCtx, acts.EnsureActor,
			activities.ActorInput{Actor: actor, Template: resolved}).Get(actorCtx, &placed); err != nil {
			saga.run(ctx)
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorCreationFailed", err.Error())
			return err
		}
		actorCreated = actorCreated || placed.Created
		if placed.Template.Name != resolved.Name {
			err := fmt.Errorf("actor %s/%s is on template %q, not %q",
				actor.Atespace, actor.Name, placed.Template.Name, resolved.Name)
			saga.run(ctx)
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TemplateNotAdopted", err.Error())
			return err
		}
	}

	saga.add("egress policy", func(ctx workflow.Context) error {
		if !actorCreated {
			return nil
		}
		return workflow.ExecuteActivity(ctx, acts.DeleteEgressPolicyIfExists, actor).Get(ctx, nil)
	})
	allowlist, restricted := egressAllowlist(desired.Gateway)
	if err := workflow.ExecuteActivity(provisionCtx, acts.ApplyEgressPolicy,
		activities.EgressInput{Actor: actor, Allowlist: allowlist}).Get(provisionCtx, nil); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionFalse, "PolicyApplyFailed", err.Error())
		if restricted {
			// A task bound to a gateway must not run without the gateway's
			// allowlist, so a sandbox this pass created is rolled back instead.
			saga.run(ctx)
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "PolicyApplyFailed", err.Error())
			return err
		}
		logger.Warn("could not apply the default egress policy", "task", r.key(), "error", err)
	} else {
		r.setCondition(ctx, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionTrue, "PoliciesApplied", "Network policies active")
	}

	// Stamped from the snapshot, so a spec that arrived mid-pass is not recorded
	// as provisioned and the next pass picks it up.
	r.provisioned = desired
	r.sandbox = sandboxUnknown
	r.workspaceProbed = r.conditionTrue(v1alpha1.ConditionWorkspaceReady)
	r.status.Actor = actor.Name
	return nil
}

// activate brings the sandbox into the state the spec asks for: suspended and
// checkpointed, or running on a worker.
//
// Activation is retried but never compensated. Deleting a sandbox because a
// resume failed would throw away the workspace the task has been building, so a
// task that cannot be activated stays Failed with its sandbox intact and can be
// resumed again later.
func (r *taskRun) activate(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	actor := r.actorRef()

	if r.desired.Task.GetSpec().GetSuspend() {
		if r.sandbox == sandboxSuspended {
			return nil
		}
		suspendCtx := workflow.WithActivityOptions(ctx, activities.ActorOptions())
		if err := workflow.ExecuteActivity(suspendCtx, acts.SuspendActor, actor).Get(suspendCtx, nil); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorSuspendFailed", err.Error())
			return err
		}
		r.sandbox = sandboxSuspended
		r.status.WorkerIp = ""
		r.syncReady(ctx)
		logger.Info("task suspended", "task", r.key())
		return nil
	}

	if r.sandbox != sandboxRunning || r.status.GetWorkerIp() == "" {
		resumeCtx := workflow.WithActivityOptions(ctx, activities.ResumeOptions())
		var workerIP string
		if err := workflow.ExecuteActivity(resumeCtx, acts.ResumeActor, actor).Get(resumeCtx, &workerIP); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorResumeFailed", err.Error())
			return err
		}
		r.sandbox = sandboxRunning
		r.status.WorkerIp = workerIP
		logger.Info("task running", "task", r.key(), "workerIP", workerIP)
	}

	r.syncReady(ctx)
	return nil
}

// syncReady derives the Ready condition. A task is ready only when its sandbox
// is running and the workspace inside it has finished setting up.
func (r *taskRun) syncReady(ctx workflow.Context) {
	switch {
	case r.teardownFailed:
		// The sandbox may well be running, but the task was asked to go and is
		// still here, which is the thing worth reporting.
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TeardownFailed", r.teardownMessage)
	case r.sandbox == sandboxSuspended:
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TaskSuspended", "Task is suspended")
	case r.completed:
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "CommandExited",
			fmt.Sprintf("Task command exited with code %d", r.status.GetExitCode()))
	case r.conditionTrue(v1alpha1.ConditionWorkspaceReady):
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionTrue, "TaskRunning",
			"Task is running and its workspace is ready")
	default:
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "WorkspaceInitializing",
			"Waiting for workspace setup to complete")
	}
}

// settleWorkspace waits for the workspace inside the sandbox in its own
// coroutine and asks the main loop for a pass once it knows the answer, so the
// task's readiness catches up without anything blocking on the poll.
func (r *taskRun) settleWorkspace(ctx workflow.Context) {
	if r.workspaceProbed || r.probing || r.deleting || r.status.GetWorkerIp() == "" {
		return
	}
	r.probing = true
	generation := r.generation
	in := activities.WorkspaceReadyInput{
		Actor:    r.actorRef(),
		WorkerIP: r.status.GetWorkerIp(),
		Timeout:  activities.WorkspaceReadyTimeout,
	}
	workflow.Go(ctx, func(gctx workflow.Context) {
		probeCtx := workflow.WithActivityOptions(gctx, activities.WorkspaceReadyOptions(in.Timeout))
		var ready bool
		err := workflow.ExecuteActivity(probeCtx, acts.AwaitWorkspaceReady, in).Get(probeCtx, &ready)
		if generation != r.generation {
			// The sandbox this probe was talking to is gone. Its answer says
			// nothing about the one that replaced it.
			workflow.GetLogger(gctx).Info("dropping a workspace probe for a replaced sandbox", "task", r.key())
			return
		}
		r.probing = false
		switch {
		case err != nil:
			// A probe that cannot run leaves the task running and not ready. The
			// resync looks again, so nothing has to be retried here.
			r.workspaceProbed = true
			workflow.GetLogger(gctx).Error("workspace probe failed", "task", r.key(), "error", err)
			r.setCondition(gctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "ProbeFailed", err.Error())
		case ready:
			r.workspaceProbed = true
			r.setCondition(gctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue, "SetupComplete",
				fmt.Sprintf("Workspace setup completed at %s", in.WorkerIP))
		default:
			r.workspaceProbed = true
			r.setCondition(gctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "Initializing",
				fmt.Sprintf("Workspace is still initializing at %s", in.WorkerIP))
		}
		r.request()
	})
}

// resync checks the sandbox against what the workflow believes about it. A
// crashed or vanished actor is provisioned again, and a rescheduled one has its
// worker address corrected.
func (r *taskRun) resync(ctx workflow.Context) {
	if r.deleting || r.provisioned == nil {
		return
	}
	logger := workflow.GetLogger(ctx)
	observeCtx := workflow.WithActivityOptions(ctx, activities.ProvisionOptions())

	var observed activities.ActorObservation
	if err := workflow.ExecuteActivity(observeCtx, acts.ObserveActor, r.actorRef()).Get(observeCtx, &observed); err != nil {
		logger.Warn("could not observe the task sandbox", "task", r.key(), "error", err)
		return
	}

	switch {
	case !observed.Exists:
		logger.Info("task sandbox is gone; provisioning it again", "task", r.key())
		r.forgetSandbox(ctx, "SandboxGone", "Sandbox disappeared and is being provisioned again")
	case observed.State == activities.ActorStateCrashed:
		logger.Info("task sandbox crashed; replacing it", "task", r.key())
		r.forgetSandbox(ctx, "SandboxReplaced", "Sandbox crashed and is being replaced")
	case observed.State == activities.ActorStateSuspended && !r.desired.Task.GetSpec().GetSuspend():
		r.sandbox = sandboxSuspended
		r.status.WorkerIp = ""
	case observed.State == activities.ActorStateRunning && observed.WorkerIP != r.status.GetWorkerIp():
		logger.Info("task sandbox moved", "task", r.key(), "workerIP", observed.WorkerIP)
		r.status.WorkerIp = observed.WorkerIP
		r.sandbox = sandboxRunning
	default:
		// The sandbox is what the task says it should be. A workspace that was
		// still initializing gets another look, so a maiden run that outlasts the
		// poll still reports ready in the end.
		if r.status.GetWorkerIp() != "" && !r.conditionTrue(v1alpha1.ConditionWorkspaceReady) {
			r.workspaceProbed = false
		}
		r.syncPhase()
		return
	}
	r.converge(ctx)
}

// forgetSandbox drops what the workflow believes about the sandbox so the next
// pass builds a new one. The workspace inside a replacement sandbox is set up
// from scratch, so its readiness has to be established again too.
func (r *taskRun) forgetSandbox(ctx workflow.Context, reason, message string) {
	r.provisioned = nil
	r.sandbox = sandboxUnknown
	r.status.WorkerIp = ""
	r.workspaceProbed = false
	// A probe still talking to the old sandbox is now stale: its answer is
	// dropped, and the replacement gets a probe of its own.
	r.probing = false
	r.generation++
	r.setCondition(ctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, reason, message)
}

// teardown releases the task's Substrate resources. Templates can only go once
// the actor that references them is gone, so the order matters.
//
// A task is only reported gone once its sandbox really is. Teardown that fails
// leaves the task alive and Failed, naming what was left behind, so that the
// API keeps answering for it and a later delete tries again.
func (r *taskRun) teardown(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	teardownCtx := workflow.WithActivityOptions(ctx, activities.TeardownOptions())
	actor := r.actorRef()

	logger.Info("tearing down task sandbox", "task", r.key())
	if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorIfExists, actor).Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor", "task", r.key(), "error", err)
		r.failTeardown(ctx, fmt.Sprintf("Actor %s/%s is still there: %v", actor.Atespace, actor.Name, err))
		return err
	}
	if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorTemplates,
		activities.TemplatesInput{Atespace: actor.Atespace, TaskName: actor.Name}).Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor templates", "task", r.key(), "error", err)
		r.failTeardown(ctx, fmt.Sprintf("Actor templates of %s/%s are still there: %v", actor.Atespace, actor.Name, err))
		return err
	}

	r.deleted = true
	r.syncPhase()
	return nil
}

// failTeardown puts the task back in reach after a teardown that could not
// finish. The task stays deletable, and the condition names what Substrate
// still holds so an operator can find it.
func (r *taskRun) failTeardown(ctx workflow.Context, message string) {
	r.deleting = false
	r.teardownFailed = true
	r.teardownFailures++
	r.teardownMessage = message
	r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TeardownFailed", message)
	r.syncPhase()
}

// releaseOnCancel tears the sandbox down when the workflow is cancelled. The
// cleanup runs on a disconnected context, which is the only way activities can
// still be started after cancellation.
func (r *taskRun) releaseOnCancel(ctx workflow.Context) {
	if !errors.Is(ctx.Err(), workflow.ErrCanceled) || r.deleted {
		return
	}
	workflow.GetLogger(ctx).Info("task workflow cancelled; releasing the sandbox", "task", r.key())
	disconnected, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	_ = r.teardown(disconnected)
}

// awaitHandlers waits for update handlers that are still running, so a task
// never finishes while a caller is still waiting for its change to land.
func (r *taskRun) awaitHandlers(ctx workflow.Context) error {
	return workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}

// pending reports whether there is work the main loop has not handled yet.
func (r *taskRun) pending() bool {
	return r.requests != r.handled || r.deleting
}

// continueInput carries the task across a continue-as-new boundary: the desired
// state and the status, which is everything the next run needs to pick up
// without provisioning from scratch.
func (r *taskRun) continueInput() TaskWorkflowInput {
	return TaskWorkflowInput{
		Desired: cloneDesired(r.desired),
		Status:  r.statusSnapshot(),
	}
}

func (r *taskRun) actorRef() activities.ActorRef {
	return activities.ActorRef{
		Atespace: r.desired.Task.GetMetadata().GetAtespace(),
		Name:     r.desired.Task.GetMetadata().GetName(),
	}
}

func (r *taskRun) key() string {
	return TaskWorkflowID(r.desired.Task.GetMetadata().GetAtespace(), r.desired.Task.GetMetadata().GetName())
}

// syncPhase derives the reported phase from the run's state. Deriving it in one
// place is what keeps a task from reporting Running after its command exited,
// or Completed while its sandbox is suspended.
func (r *taskRun) syncPhase() {
	switch {
	case r.deleting || r.deleted:
		r.status.Phase = v1alpha1.PhaseTerminating
	case r.teardownFailed:
		// A task that was asked to go and could not is the operator's problem,
		// whatever its sandbox is doing.
		r.status.Phase = v1alpha1.PhaseFailed
	case r.completed:
		// How the command finished outlives what the sandbox is doing now, so a
		// task that has run to its end says so.
		r.status.Phase = v1alpha1.PhaseCompleted
	case r.failed:
		r.status.Phase = v1alpha1.PhaseFailed
	case r.sandbox == sandboxSuspended:
		r.status.Phase = v1alpha1.PhaseSuspended
	case r.status.GetWorkerIp() != "":
		r.status.Phase = v1alpha1.PhaseRunning
	default:
		r.status.Phase = v1alpha1.PhasePending
	}
}

// recordCompletion stores how the task command finished. The sandbox stays up
// so that its workspace can still be inspected.
func (r *taskRun) recordCompletion(ctx workflow.Context, in CompleteInput) {
	exitCode := in.ExitCode
	r.completed = true
	r.status.ExitCode = &exitCode
	message := in.Message
	if message == "" {
		message = fmt.Sprintf("Task command exited with code %d", exitCode)
	}
	r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "CommandExited", message)
	r.syncPhase()
	workflow.GetLogger(ctx).Info("task command exited", "task", r.key(), "exitCode", exitCode)
}

// drainCompletions takes anything left in the completion channel before the
// workflow continues as new, because a buffered signal would otherwise be lost
// with the run that was carrying it.
func (r *taskRun) drainCompletions(ctx workflow.Context) {
	for {
		var in CompleteInput
		if !r.completions.ReceiveAsync(&in) {
			return
		}
		r.recordCompletion(ctx, in)
	}
}

func (r *taskRun) statusSnapshot() *v1alpha1.TaskStatus {
	out, ok := proto.Clone(r.status).(*v1alpha1.TaskStatus)
	if !ok {
		return &v1alpha1.TaskStatus{}
	}
	return out
}

// taskSnapshot returns the task as the API serves it: the desired state the
// workflow is working towards, with the status it has reached.
func (r *taskRun) taskSnapshot() *v1alpha1.Task {
	task := cloneTask(r.desired.Task)
	task.ApiVersion = v1alpha1.APIVersion
	task.Kind = v1alpha1.KindTask
	task.Status = r.statusSnapshot()
	return task
}

func (r *taskRun) conditionTrue(condType string) bool {
	for _, c := range r.status.GetConditions() {
		if c.GetType() == condType {
			return c.GetStatus() == v1alpha1.ConditionTrue
		}
	}
	return false
}

// setCondition records a condition transition. The timestamp comes from the
// workflow clock so that it replays to the same value.
func (r *taskRun) setCondition(ctx workflow.Context, condType, status, reason, message string) {
	ts := timestamppb.New(workflow.Now(ctx))
	for _, c := range r.status.Conditions {
		if c.GetType() != condType {
			continue
		}
		if c.GetStatus() == status && c.GetReason() == reason && c.GetMessage() == message {
			return
		}
		c.Status = status
		c.Reason = reason
		c.Message = message
		c.LastTransitionTime = ts
		return
	}
	r.status.Conditions = append(r.status.Conditions, &v1alpha1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: ts,
	})
}

// egressAllowlist returns the rules to apply to a task's actor and whether the
// task is bound to a gateway that restricts its egress. Without a gateway a
// task keeps unrestricted egress, which is what it had before any policy was
// applied.
func egressAllowlist(gw *v1alpha1.Gateway) (*v1alpha1.EgressAllowlist, bool) {
	if list := gw.GetSpec().GetEgress().GetAllowlist(); list != nil && len(list.GetHosts()) > 0 {
		return list, true
	}
	return &v1alpha1.EgressAllowlist{
		Hosts: []*v1alpha1.HostRule{{Host: "*", Port: 443}},
	}, false
}

// sameProvisioning reports whether two desired states build the same sandbox.
// The suspend flag and the status are left out: they change what a sandbox is
// doing, not what it is made of, so flipping them must not strand a new actor
// template.
//
// proto.Equal compares any unknown fields with reflect.DeepEqual, which ranges
// over a map. The result does not depend on the iteration order, so the
// comparison is stable across a replay.
//
//workflowcheck:ignore
func sameProvisioning(a, b *TaskDesiredState) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if !proto.Equal(activities.SandboxSpec(a.Task), activities.SandboxSpec(b.Task)) {
		return false
	}
	if !proto.Equal(a.Gateway, b.Gateway) {
		return false
	}
	if len(a.Workspaces) != len(b.Workspaces) {
		return false
	}
	for i := range a.Workspaces {
		if !proto.Equal(a.Workspaces[i], b.Workspaces[i]) {
			return false
		}
	}
	return true
}

func cloneTask(task *v1alpha1.Task) *v1alpha1.Task {
	out, ok := proto.Clone(task).(*v1alpha1.Task)
	if !ok || out == nil {
		return &v1alpha1.Task{}
	}
	return out
}

func cloneDesired(in *TaskDesiredState) *TaskDesiredState {
	if in == nil {
		return nil
	}
	out := &TaskDesiredState{Task: cloneTask(in.Task)}
	if in.Gateway != nil {
		out.Gateway, _ = proto.Clone(in.Gateway).(*v1alpha1.Gateway)
	}
	for _, ws := range in.Workspaces {
		clone, _ := proto.Clone(ws).(*v1alpha1.Workspace)
		out.Workspaces = append(out.Workspaces, clone)
	}
	return out
}

// compensations undoes a partial provisioning run.
type compensations struct {
	steps []compensation
}

type compensation struct {
	what string
	undo func(ctx workflow.Context) error
}

// add registers a compensation. It is always called before the step it undoes.
func (c *compensations) add(what string, undo func(ctx workflow.Context) error) {
	c.steps = append(c.steps, compensation{what: what, undo: undo})
}

// run undoes the registered steps in reverse order on a disconnected context,
// so a provisioning run is rolled back even when the workflow itself is being
// cancelled. A compensation that fails is logged and the rest still run: the
// resources it leaves behind are named after the task, so the next attempt or
// the eventual delete picks them up.
func (c *compensations) run(ctx workflow.Context) {
	if len(c.steps) == 0 {
		return
	}
	logger := workflow.GetLogger(ctx)
	disconnected, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	undoCtx := workflow.WithActivityOptions(disconnected, activities.TeardownOptions())

	for i := len(c.steps) - 1; i >= 0; i-- {
		step := c.steps[i]
		logger.Info("rolling back", "step", step.what)
		if err := step.undo(undoCtx); err != nil {
			logger.Error("rollback failed", "step", step.what, "error", err)
		}
	}
	c.steps = nil
}
