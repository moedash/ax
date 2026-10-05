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

	// firstApplyTimeout bounds how long a task started by an update waits for
	// that update to bring its spec. Both travel in one request, so anything
	// longer than this means the update never arrived.
	firstApplyTimeout = time.Minute

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
	if r.desired == nil {
		// Started together with the update that carries the spec. Nothing about
		// this task is known until it lands.
		applied, err := workflow.AwaitWithTimeout(ctx, firstApplyTimeout,
			func() bool { return r.desired != nil })
		if err != nil {
			return err
		}
		if !applied {
			return temporal.NewNonRetryableApplicationError(
				"no task spec was applied", ErrTypeInvalidTask, nil)
		}
	}
	logger.Info("task workflow started", "task", r.key(), "phase", r.status.GetPhase())

	// Recorded on every run. There is one version of the provisioning sequence
	// so far, and this is the hook a later one branches on.
	_ = workflow.GetVersion(ctx, provisioningChangeID, workflow.DefaultVersion, provisioningVersion)

	// A cancelled task is a deleted task: the sandbox must not outlive the
	// workflow that owns it.
	defer r.releaseOnCancel(ctx)

	// The outer turn is the task's life: it ends only when the sandbox is
	// really gone. A teardown that cannot finish puts the task back in the
	// inner loop, where a later delete tries again.
	for !r.deleted {
		for !r.deleting {
			handling := r.requests
			r.interruptible(ctx, r.converge)
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
				r.interruptible(ctx, func(ctx workflow.Context) { r.resync(ctx, cfg) })
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
	// workflowID names the task even before a spec has been applied to it.
	workflowID string
	// desired is what the task should be. It is set once, by the apply that
	// starts the task.
	desired *TaskDesiredState
	// provisioned is the desired state the current sandbox was built from. It
	// is nil until a sandbox has been built, and again once the workflow has
	// given up on the one it had.
	provisioned *TaskDesiredState
	status      *v1alpha1.TaskStatus
	// suspended is whether the task is meant to be stopped. A task is created
	// suspended; the suspend and resume updates flip it.
	suspended bool

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
	// generation counts the sandboxes this task has had. It names the template
	// a sandbox is built from and is handed to the runner inside it, so a probe
	// or a completion report from a sandbox that has since been replaced can be
	// told from one about the current sandbox and dropped.
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

	// interrupt cancels the step the main loop is in, so a delete does not wait
	// for a pass that is still retrying. It is nil between steps.
	interrupt workflow.CancelFunc

	completions workflow.ReceiveChannel
}

// interruptible runs one step of the main loop under a context handleDelete
// can cancel. An activity the step is waiting on returns cancelled, the step
// fails, and the loop moves on to the teardown.
func (r *taskRun) interruptible(ctx workflow.Context, step func(workflow.Context)) {
	stepCtx, cancel := workflow.WithCancel(ctx)
	r.interrupt = cancel
	step(stepCtx)
	r.interrupt = nil
	cancel()
}

// newTaskRun validates the input and puts the task into the shape the rest of
// the workflow expects.
func newTaskRun(ctx workflow.Context, in TaskWorkflowInput) (*taskRun, error) {
	r := &taskRun{
		status:          in.Status,
		workflowID:      workflow.GetInfo(ctx).WorkflowExecution.ID,
		generation:      in.Generation,
		failed:          in.Failed,
		teardownFailed:  in.TeardownFailed,
		teardownMessage: in.TeardownMessage,
	}
	if r.status == nil {
		r.status = &v1alpha1.TaskStatus{}
	}
	if r.conditionTrue(v1alpha1.ConditionWorkspaceReady) {
		// Workspace setup happens once per task and its result outlives suspends,
		// actor failures, and continue-as-new.
		r.workspaceProbed = true
	}
	if r.status.GetPhase() == v1alpha1.PhaseCompleted || r.status.ExitCode != nil {
		r.completed = true
	}

	// A task started by an update-with-start carries no spec here: the update
	// is what brings it, and the task starts out suspended. A continued run
	// brings its own spec and whatever the task had been told since.
	if in.Desired == nil {
		r.suspended = true
		return r, nil
	}
	r.suspended = in.Suspended
	if err := r.adopt(ctx, in.Desired); err != nil {
		return nil, err
	}
	return r, nil
}

// adopt takes a desired state as the task's own, normalizing it and settling
// the identity that follows from it.
func (r *taskRun) adopt(ctx workflow.Context, desired *TaskDesiredState) error {
	if desired == nil || desired.Task == nil {
		return temporal.NewNonRetryableApplicationError("task is required", ErrTypeInvalidTask, nil)
	}
	if desired.Task.GetMetadata().GetName() == "" {
		return temporal.NewNonRetryableApplicationError("task name is required", ErrTypeInvalidTask, nil)
	}
	if err := r.ownsTask(desired.Task); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalidTask, nil)
	}
	if err := v1alpha1.ValidateTask(desired.Task); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalidTask, nil)
	}

	created := r.desired.GetTask().GetMetadata().GetCreationTimestamp()
	r.desired = normalizeDesired(desired)
	if created != nil {
		// A task is created once. Whatever a later apply carries, the creation
		// time is the one the task started with.
		r.desired.Task.Metadata.CreationTimestamp = created
	}

	// The actor carries the task's name so that the two are interchangeable, for
	// example in the router's ate-target-actor header. Whatever a client put in
	// status.actor is overwritten.
	r.status.Actor = r.desired.Task.GetMetadata().GetName()
	if r.status.Id == "" {
		r.status.Id = fmt.Sprintf("task-%s-%d", r.status.Actor, workflow.Now(ctx).Unix())
	}
	r.syncPhase(ctx)
	r.upsertSearchAttributes(ctx)
	return nil
}

// normalizeDesired fills in the defaults a task manifest may leave out, so the
// rest of the workflow can read the spec without repeating them.
func normalizeDesired(in *TaskDesiredState) *TaskDesiredState {
	out := &TaskDesiredState{
		Task:       cloneTask(in.Task),
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
	r.syncPhase(ctx)
}

// reconcile builds the sandbox if the task does not have one and then puts it
// in the state the spec asks for. The spec never changes, so a sandbox is only
// built again after the workflow has given up on the one it had.
func (r *taskRun) reconcile(ctx workflow.Context) error {
	if r.provisioned == nil {
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
//
// What was already there is settled by looking before anything is created. An
// activity's own answer cannot be trusted for that: a worker that dies after
// Substrate made the resource but before the result was recorded gets "found"
// on the retry.
func (r *taskRun) provision(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	desired := cloneDesired(r.desired)
	actor := activities.ActorRef{
		Atespace: desired.Task.GetMetadata().GetAtespace(),
		Name:     desired.Task.GetMetadata().GetName(),
	}
	provisionCtx := workflow.WithActivityOptions(ctx, activities.ProvisionOptions())
	actorCtx := workflow.WithActivityOptions(ctx, activities.ActorOptions())

	logger.Info("provisioning task sandbox",
		"task", r.key(), "image", desired.Task.GetSpec().GetImage())

	var observed activities.ActorObservation
	if err := workflow.ExecuteActivity(provisionCtx, acts.ObserveActor, actor).
		Get(provisionCtx, &observed); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"ActorObserveFailed", err.Error())
		return err
	}

	// Substrate binds an actor to the template it was created from. An actor on
	// any other template than the one this pass builds was not made by this
	// task's workflow: it is left over from a sandbox the workflow gave up on,
	// or from a control plane that ran the task before this one did. It is
	// replaced, and whatever it had in its workspace goes with it.
	template := r.templateRef(desired)
	replacing := observed.Exists && observed.Template.Name != template.Name
	if replacing {
		logger.Info("replacing a sandbox that is not on the task's template",
			"task", r.key(), "from", observed.Template.Name, "to", template.Name)
	}

	var templateObserved activities.TemplateObservation
	if err := workflow.ExecuteActivity(provisionCtx, acts.ObserveActorTemplate, template).
		Get(provisionCtx, &templateObserved); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"TemplateObserveFailed", err.Error())
		return err
	}

	// Only what this pass brings into being may be rolled back by it. An actor
	// this pass replaces is its own, because it is the one that deletes the old.
	templateOwned := !templateObserved.Exists
	actorOwned := !observed.Exists || replacing
	var saga compensations

	// The atespace is shared by every task in it, so it is never rolled back.
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureAtespace,
		activities.AtespaceInput{Atespace: actor.Atespace}).Get(provisionCtx, nil); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"AtespaceCreationFailed", err.Error())
		return err
	}

	saga.add("actor template", func(ctx workflow.Context) error {
		if !templateOwned {
			return nil
		}
		return workflow.ExecuteActivity(ctx, acts.DeleteActorTemplateIfExists, template).
			Get(ctx, nil)
	})
	templateIn := activities.TemplateInput{
		Template:   template,
		Image:      desired.Task.GetSpec().GetImage(),
		Task:       desired.Task,
		Workspaces: desired.Workspaces,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		Generation: r.generation,
	}
	var provisionedTemplate activities.TemplateProvision
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureActorTemplate, templateIn).
		Get(provisionCtx, &provisionedTemplate); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"TemplateCreationFailed", err.Error())
		return err
	}
	resolved := provisionedTemplate.Template

	saga.add("actor", func(ctx workflow.Context) error {
		if !actorOwned {
			return nil
		}
		return workflow.ExecuteActivity(ctx, acts.DeleteActorIfExists, actor).Get(ctx, nil)
	})
	if replacing {
		teardownCtx := workflow.WithActivityOptions(ctx, activities.TeardownOptions())
		if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorIfExists, actor).
			Get(teardownCtx, nil); err != nil {
			saga.run(ctx)
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
				"ActorReplaceFailed", err.Error())
			return err
		}
	}
	var placed activities.ActorProvision
	if err := workflow.ExecuteActivity(actorCtx, acts.EnsureActor,
		activities.ActorInput{Actor: actor, Template: resolved}).Get(actorCtx, &placed); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"ActorCreationFailed", err.Error())
		return err
	}
	if placed.Template.Name != resolved.Name {
		// An actor that appeared between the look and the create is on a template
		// this pass did not choose. The next pass sees it and replaces it.
		err := fmt.Errorf("actor %s/%s is on template %q, not %q",
			actor.Atespace, actor.Name, placed.Template.Name, resolved.Name)
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse,
			"TemplateNotAdopted", err.Error())
		return err
	}

	r.provisioned = desired
	r.sandbox = sandboxUnknown
	r.workspaceProbed = r.conditionTrue(v1alpha1.ConditionWorkspaceReady)
	r.status.Actor = actor.Name
	return nil
}

// templateRef names the ActorTemplate the task's current sandbox generation is
// built from.
func (r *taskRun) templateRef(desired *TaskDesiredState) activities.TemplateRef {
	return activities.TemplateRef{
		Atespace: desired.Task.GetMetadata().GetAtespace(),
		Name:     activities.TaskTemplateName(desired.Task, desired.Workspaces, r.generation),
	}
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

	if r.suspended {
		if r.sandbox == sandboxSuspended {
			return nil
		}
		suspendCtx := workflow.WithActivityOptions(ctx, activities.ActorOptions())
		if err := workflow.ExecuteActivity(suspendCtx, acts.SuspendActor, actor).
			Get(suspendCtx, nil); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorSuspendFailed",
				err.Error())
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
		if err := workflow.ExecuteActivity(resumeCtx, acts.ResumeActor, actor).
			Get(resumeCtx, &workerIP); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorResumeFailed",
				err.Error())
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
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TeardownFailed",
			r.teardownMessage)
	case r.sandbox == sandboxSuspended:
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TaskSuspended",
			"Task is suspended")
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
			workflow.GetLogger(gctx).Info("dropping a workspace probe for a replaced sandbox",
				"task", r.key())
			return
		}
		r.probing = false
		switch {
		case err != nil:
			// A probe that cannot run leaves the task running and not ready. The
			// resync looks again, so nothing has to be retried here.
			r.workspaceProbed = true
			workflow.GetLogger(gctx).Error("workspace probe failed", "task", r.key(), "error", err)
			r.setCondition(gctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "ProbeFailed",
				err.Error())
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
// vanished actor is provisioned again, a crashed one is put back on its last
// snapshot, and a rescheduled one has its worker address corrected.
func (r *taskRun) resync(ctx workflow.Context, cfg Config) {
	if r.deleting || r.provisioned == nil {
		return
	}
	logger := workflow.GetLogger(ctx)
	observeCtx := workflow.WithActivityOptions(ctx, activities.ObserveOptions(cfg.ResyncInterval))

	var observed activities.ActorObservation
	if err := workflow.ExecuteActivity(observeCtx, acts.ObserveActor, r.actorRef()).
		Get(observeCtx, &observed); err != nil {
		logger.Warn("could not observe the task sandbox", "task", r.key(), "error", err)
		return
	}

	switch {
	case !observed.Exists:
		logger.Info("task sandbox is gone; provisioning it again", "task", r.key())
		r.forgetSandbox(ctx, "SandboxGone", "Sandbox disappeared and is being provisioned again")
	case observed.State == activities.ActorStateCrashed:
		r.revertSandbox(ctx)
	case observed.State == activities.ActorStateSuspended && !r.suspended:
		r.sandbox = sandboxSuspended
		r.status.WorkerIp = ""
	case observed.State == activities.ActorStateRunning && r.suspended:
		// The router resumes a suspended actor when a request reaches it. The
		// task was told to stay suspended, so the next pass suspends it again.
		logger.Info("task sandbox was resumed behind the task's back", "task", r.key())
		r.sandbox = sandboxRunning
		r.status.WorkerIp = observed.WorkerIP
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
	}
	// Whatever changed here is driven in by the next turn of the loop.
	r.syncPhase(ctx)
}

// revertSandbox puts a crashed sandbox back on its last snapshot, which keeps
// the workspace the task has been building. The actor comes back suspended, so
// the next pass resumes it if the task is meant to run. A sandbox that cannot
// be reverted is given up on and built again.
func (r *taskRun) revertSandbox(ctx workflow.Context) {
	logger := workflow.GetLogger(ctx)
	logger.Info("task sandbox crashed; reverting it to its last snapshot", "task", r.key())
	revertCtx := workflow.WithActivityOptions(ctx, activities.ActorOptions())
	if err := workflow.ExecuteActivity(revertCtx, acts.RevertActor, r.actorRef()).
		Get(revertCtx, nil); err != nil {
		logger.Error("could not revert the crashed sandbox; replacing it",
			"task", r.key(), "error", err)
		r.forgetSandbox(ctx, "SandboxReplaced", "Sandbox crashed and is being replaced")
		return
	}
	r.sandbox = sandboxSuspended
	r.status.WorkerIp = ""
	r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "SandboxReverted",
		"Sandbox crashed and was reverted to its last snapshot")
}

// forgetSandbox drops what the workflow believes about the sandbox so the next
// pass builds a new one. The workspace inside a replacement sandbox is set up
// from scratch, so its readiness has to be established again too, and the
// command inside it has not run yet, so how the old one ended is dropped.
func (r *taskRun) forgetSandbox(ctx workflow.Context, reason, message string) {
	r.provisioned = nil
	r.sandbox = sandboxUnknown
	r.status.WorkerIp = ""
	r.workspaceProbed = false
	r.completed = false
	r.status.ExitCode = nil
	// A probe still talking to the old sandbox is now stale: its answer is
	// dropped, and the replacement gets a probe of its own. The same goes for a
	// completion report the old sandbox has yet to send.
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
	if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorIfExists, actor).
		Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor", "task", r.key(), "error", err)
		r.failTeardown(ctx,
			fmt.Sprintf("Actor %s/%s is still there: %v", actor.Atespace, actor.Name, err))
		return err
	}
	templates := activities.TemplatesInput{Atespace: actor.Atespace, TaskName: actor.Name}
	if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorTemplates, templates).
		Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor templates", "task", r.key(), "error", err)
		r.failTeardown(ctx,
			fmt.Sprintf("Actor templates of %s/%s are still there: %v", actor.Atespace, actor.Name, err))
		return err
	}

	r.deleted = true
	r.syncPhase(ctx)
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
	r.syncPhase(ctx)
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
		Desired:         cloneDesired(r.desired),
		Status:          r.statusSnapshot(),
		Suspended:       r.suspended,
		Generation:      r.generation,
		Failed:          r.failed,
		TeardownFailed:  r.teardownFailed,
		TeardownMessage: r.teardownMessage,
	}
}

// GetTask makes a desired state safe to read before one has been applied.
func (d *TaskDesiredState) GetTask() *v1alpha1.Task {
	if d == nil {
		return nil
	}
	return d.Task
}

func (r *taskRun) actorRef() activities.ActorRef {
	return activities.ActorRef{
		Atespace: r.desired.Task.GetMetadata().GetAtespace(),
		Name:     r.desired.Task.GetMetadata().GetName(),
	}
}

func (r *taskRun) key() string {
	if r.desired == nil {
		return r.workflowID
	}
	return TaskWorkflowID(r.desired.Task.GetMetadata().GetAtespace(),
		r.desired.Task.GetMetadata().GetName())
}

// syncPhase derives the reported phase from the run's state. Deriving it in one
// place is what keeps a task from reporting Running after its command exited,
// or Completed while its sandbox is suspended.
func (r *taskRun) syncPhase(ctx workflow.Context) {
	before := r.status.GetPhase()
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

	// Visibility carries the phase so that listing tasks does not mean asking
	// every one of them what it is doing.
	if r.status.GetPhase() != before {
		r.upsertSearchAttributes(ctx)
	}
}

// upsertSearchAttributes publishes the parts of a task that visibility answers
// for: where it lives, what it is doing, and the configuration it binds.
func (r *taskRun) upsertSearchAttributes(ctx workflow.Context) {
	if r.desired == nil {
		return
	}
	workspaces := make([]string, 0, len(r.desired.Workspaces))
	for _, ref := range r.desired.Task.GetSpec().WorkspaceRefs() {
		if ref.GetName() != "" {
			workspaces = append(workspaces, ref.GetName())
		}
	}
	// An attribute the namespace does not have fails the workflow task, not this
	// call, so there is nothing useful to do with the error here. The worker
	// checks the attributes exist before it starts, which is where a missing one
	// is reported.
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		AtespaceKey.ValueSet(r.desired.Task.GetMetadata().GetAtespace()),
		PhaseKey.ValueSet(r.status.GetPhase()),
		WorkspacesKey.ValueSet(workspaces),
	)
}

// recordCompletion stores how the task command finished. The sandbox stays up
// so that its workspace can still be inspected. A report from a sandbox the
// task has since replaced is dropped here as well as in the update validator,
// because the signal path has no validator.
func (r *taskRun) recordCompletion(ctx workflow.Context, in CompleteInput) {
	if in.Generation != r.generation {
		workflow.GetLogger(ctx).Info("dropping a completion report from a replaced sandbox",
			"task", r.key(), "reported", in.Generation, "current", r.generation)
		return
	}
	exitCode := in.ExitCode
	r.completed = true
	r.status.ExitCode = &exitCode
	message := in.Message
	if message == "" {
		message = fmt.Sprintf("Task command exited with code %d", exitCode)
	}
	r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "CommandExited", message)
	r.syncPhase(ctx)
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
	task := cloneTask(r.desired.GetTask())
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
