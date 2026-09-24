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

	// resyncInterval is how often a settled task checks that its sandbox is
	// still what the task says it should be. A sandbox can crash or be
	// rescheduled without anyone telling AX, and nothing else would notice.
	resyncInterval = 5 * time.Minute
)

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

// TaskWorkflow orchestrates one AX task for as long as the task exists.
//
// It provisions the sandbox, keeps it in the state the spec asks for, and
// answers questions about it. A task that has been provisioned stays in the
// loop until it is deleted, so suspending, resuming, reporting a command exit,
// and deleting are all handled by the same execution that created the sandbox.
func TaskWorkflow(ctx workflow.Context, in TaskWorkflowInput) error {
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

	for !r.deleting {
		handling := r.requests
		r.converge(ctx)
		r.handled = handling

		if r.deleting {
			break
		}
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			r.drainCompletions(ctx)
			if !r.pending() {
				logger.Info("continuing task workflow as new", "task", r.key())
				if err := r.awaitHandlers(ctx); err != nil {
					return err
				}
				return workflow.NewContinueAsNewError(ctx, TaskWorkflow, r.continueInput())
			}
			continue
		}

		requested, err := workflow.AwaitWithTimeout(ctx, resyncInterval, r.pending)
		if err != nil {
			return err
		}
		if !requested {
			r.resync(ctx)
		}
	}

	teardownErr := r.teardown(ctx)
	if err := r.awaitHandlers(ctx); err != nil {
		return err
	}
	if teardownErr != nil {
		return teardownErr
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
	completed       bool
	failed          bool
	deleting        bool
	deleted         bool

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
// them back if the sequence cannot be finished.
//
// Every step is idempotent and keyed by the task, so a retried or replayed step
// addresses the same resource. Compensations are registered before the step
// they undo, because a step whose side effect landed can still fail on the way
// back.
func (r *taskRun) provision(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	actor := r.actorRef()
	provisionCtx := workflow.WithActivityOptions(ctx, activities.ProvisionOptions())
	var saga compensations

	logger.Info("provisioning task sandbox", "task", r.key(), "image", r.desired.Task.GetSpec().GetImage())

	// The atespace is shared by every task in it, so it is never rolled back.
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureAtespace,
		activities.AtespaceInput{Atespace: actor.Atespace}).Get(provisionCtx, nil); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "AtespaceCreationFailed", err.Error())
		return err
	}

	template := activities.TemplateRef{
		Atespace: actor.Atespace,
		Name:     activities.TaskTemplateName(r.desired.Task, r.desired.Workspaces),
	}
	saga.add("actor templates", func(ctx workflow.Context) error {
		return workflow.ExecuteActivity(ctx, acts.DeleteActorTemplates,
			activities.TemplatesInput{Atespace: actor.Atespace, TaskName: actor.Name}).Get(ctx, nil)
	})
	var resolved activities.TemplateRef
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureActorTemplate, activities.TemplateInput{
		Template:   template,
		Image:      r.desired.Task.GetSpec().GetImage(),
		Task:       r.desired.Task,
		Workspaces: r.desired.Workspaces,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
	}).Get(provisionCtx, &resolved); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TemplateCreationFailed", err.Error())
		return err
	}

	saga.add("actor", func(ctx workflow.Context) error {
		return workflow.ExecuteActivity(ctx, acts.DeleteActorIfExists, actor).Get(ctx, nil)
	})
	if err := workflow.ExecuteActivity(provisionCtx, acts.EnsureActor,
		activities.ActorInput{Actor: actor, Template: resolved}).Get(provisionCtx, nil); err != nil {
		saga.run(ctx)
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorCreationFailed", err.Error())
		return err
	}

	saga.add("egress policy", func(ctx workflow.Context) error {
		return workflow.ExecuteActivity(ctx, acts.DeleteEgressPolicyIfExists, actor).Get(ctx, nil)
	})
	allowlist, restricted := egressAllowlist(r.desired.Gateway)
	if err := workflow.ExecuteActivity(provisionCtx, acts.ApplyEgressPolicy,
		activities.EgressInput{Actor: actor, Allowlist: allowlist}).Get(provisionCtx, nil); err != nil {
		r.setCondition(ctx, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionFalse, "PolicyApplyFailed", err.Error())
		if restricted {
			// A task bound to a gateway must not run without the gateway's
			// allowlist, so the sandbox is rolled back instead.
			saga.run(ctx)
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "PolicyApplyFailed", err.Error())
			return err
		}
		logger.Warn("could not apply the default egress policy", "task", r.key(), "error", err)
	} else {
		r.setCondition(ctx, v1alpha1.ConditionGatewayReady, v1alpha1.ConditionTrue, "PoliciesApplied", "Network policies active")
	}

	r.provisioned = cloneDesired(r.desired)
	r.sandbox = sandboxUnknown
	r.workspaceProbed = r.conditionTrue(v1alpha1.ConditionWorkspaceReady)
	r.status.Actor = actor.Name
	return nil
}

// activate brings the sandbox into the state the spec asks for and waits for
// the workspace inside it, so that a task reports Ready only once it can
// actually do work.
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
		suspendCtx := workflow.WithActivityOptions(ctx, activities.ProvisionOptions())
		if err := workflow.ExecuteActivity(suspendCtx, acts.SuspendActor, actor).Get(suspendCtx, nil); err != nil {
			r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "ActorSuspendFailed", err.Error())
			return err
		}
		r.sandbox = sandboxSuspended
		r.status.WorkerIp = ""
		r.setCondition(ctx, v1alpha1.ConditionReady, v1alpha1.ConditionFalse, "TaskSuspended", "Task is suspended")
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

	if !r.workspaceProbed && r.status.GetWorkerIp() != "" {
		probeCtx := workflow.WithActivityOptions(ctx, activities.WorkspaceReadyOptions(activities.WorkspaceReadyTimeout))
		var ready bool
		err := workflow.ExecuteActivity(probeCtx, acts.AwaitWorkspaceReady, activities.WorkspaceReadyInput{
			Actor:    actor,
			WorkerIP: r.status.GetWorkerIp(),
			Timeout:  activities.WorkspaceReadyTimeout,
		}).Get(probeCtx, &ready)
		if err != nil {
			r.setCondition(ctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "ProbeFailed", err.Error())
			return err
		}
		r.workspaceProbed = true
		if ready {
			r.setCondition(ctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionTrue, "SetupComplete",
				fmt.Sprintf("Workspace setup completed at %s", r.status.GetWorkerIp()))
		} else {
			r.setCondition(ctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, "Initializing",
				fmt.Sprintf("Workspace is still initializing at %s", r.status.GetWorkerIp()))
		}
	}

	switch {
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
	return nil
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
	r.setCondition(ctx, v1alpha1.ConditionWorkspaceReady, v1alpha1.ConditionFalse, reason, message)
}

// teardown releases the task's Substrate resources. Templates can only go once
// the actor that references them is gone, so the order matters.
func (r *taskRun) teardown(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	teardownCtx := workflow.WithActivityOptions(ctx, activities.TeardownOptions())
	actor := r.actorRef()

	logger.Info("tearing down task sandbox", "task", r.key())
	var failure error
	if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorIfExists, actor).Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor", "task", r.key(), "error", err)
		failure = err
	} else if err := workflow.ExecuteActivity(teardownCtx, acts.DeleteActorTemplates,
		activities.TemplatesInput{Atespace: actor.Atespace, TaskName: actor.Name}).Get(teardownCtx, nil); err != nil {
		logger.Error("could not delete the task actor templates", "task", r.key(), "error", err)
		failure = err
	}

	// The record disappears either way. Leaving it behind would block a task of
	// the same name from being created, and the failure is reported through the
	// workflow's own result.
	r.deleted = true
	r.syncPhase()
	return failure
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
	case r.failed:
		r.status.Phase = v1alpha1.PhaseFailed
	case r.sandbox == sandboxSuspended:
		r.status.Phase = v1alpha1.PhaseSuspended
	case r.completed:
		r.status.Phase = v1alpha1.PhaseCompleted
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
