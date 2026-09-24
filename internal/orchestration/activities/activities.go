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

// Package activities holds the side effects of task orchestration: every call
// AX makes to Agent Substrate and every probe it makes into a sandbox.
//
// Each activity is idempotent. The keys they act on (the actor name, the actor
// template name) are derived in the workflow from the task spec, so a retry or
// a replay addresses exactly the same Substrate resources as the first attempt.
package activities

import (
	"context"
	"net/http"
	"time"

	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"go.temporal.io/sdk/activity"
)

const (
	geminiSecretName = "gemini-api-secret"
	geminiSecretKey  = "GEMINI_API_KEY"
	// secretLookupTimeout bounds the Kubernetes secret lookup so a slow or
	// unreachable cluster cannot hold an activity open.
	secretLookupTimeout = 2 * time.Second
	// probeTimeout bounds a single readiness probe against a sandbox.
	probeTimeout = 2 * time.Second
	// heartbeatInterval is how often an activity that is blocked in a single
	// control-plane call reports liveness.
	heartbeatInterval = 10 * time.Second
)

// SecretResolver looks up a key from a Kubernetes secret in the given namespace.
type SecretResolver func(ctx context.Context, namespace, secretName, key string) (string, error)

// Activities carries everything the orchestration needs to reach the outside
// world. The worker builds one and registers it, so the dependencies are wired
// once and tests can substitute their own.
type Activities struct {
	// Substrate is the Agent Substrate Control API client.
	Substrate *substrate.Client
	// SecretResolver resolves the Gemini API key for task containers. Nil skips
	// the cluster lookup and falls back to the worker's own environment.
	SecretResolver SecretResolver
	// RouterAddr is the atenet router used to reach a sandbox by actor name when
	// its worker IP is not routable from the worker.
	RouterAddr string
	// TemporalAddress and TemporalNamespace are handed to the task container so
	// its runner can report the command's exit status back to the task workflow.
	TemporalAddress   string
	TemporalNamespace string
	// HTTPClient probes sandbox readiness. Nil uses a client with probeTimeout.
	HTTPClient *http.Client
}

// ActorRef identifies one actor on Substrate. An actor always carries the name
// of the task that owns it.
type ActorRef struct {
	Atespace string
	Name     string
}

// TemplateRef identifies one ActorTemplate on Substrate.
type TemplateRef struct {
	Atespace string
	Name     string
}

// AtespaceInput names the atespace a task needs.
type AtespaceInput struct {
	Atespace string
}

// TemplateInput describes the per-task ActorTemplate to provision. Name is
// derived in the workflow from a digest of the task and workspace specs, so the
// same spec always maps to the same template.
type TemplateInput struct {
	Template   TemplateRef
	Image      string
	Task       *v1alpha1.Task
	Workspaces []*v1alpha1.Workspace
	// WorkflowID is injected into the container so the runner can address the
	// task workflow that owns it.
	WorkflowID string
}

// ActorInput describes the actor to provision and the template it derives from.
type ActorInput struct {
	Actor    ActorRef
	Template TemplateRef
}

// EgressInput describes the egress rules to apply to an actor.
type EgressInput struct {
	Actor     ActorRef
	Allowlist *v1alpha1.EgressAllowlist
}

// TemplatesInput selects every ActorTemplate that belongs to a task.
type TemplatesInput struct {
	Atespace string
	TaskName string
}

// Substrate actor states the workflow reacts to.
const (
	ActorStateRunning   = "ACTOR_STATE_RUNNING"
	ActorStateSuspended = "ACTOR_STATE_SUSPENDED"
	ActorStateCrashed   = "ACTOR_STATE_CRASHED"
)

// ActorObservation is what the workflow needs to know about a sandbox that
// already exists.
type ActorObservation struct {
	// Exists is false when Substrate no longer has the actor.
	Exists bool
	// State is the Substrate actor state, for example ACTOR_STATE_RUNNING.
	State string
	// WorkerIP is the pod IP of the worker the actor is placed on, if any.
	WorkerIP string
}

// WorkspaceReadyInput describes where to probe for workspace setup completion
// and how long to keep trying.
type WorkspaceReadyInput struct {
	Actor    ActorRef
	WorkerIP string
	// Timeout bounds the whole poll. The activity returns not-ready rather than
	// an error when it expires, so a slow maiden run does not fail the task.
	Timeout time.Duration
	// PollInterval is the delay between probes. Zero uses defaultPollInterval.
	PollInterval time.Duration
}

// EnsureAtespace creates the task's atespace if it is missing.
func (a *Activities) EnsureAtespace(ctx context.Context, in AtespaceInput) error {
	if in.Atespace == "" {
		return invalidSpec("atespace is required")
	}
	activity.GetLogger(ctx).Info("ensuring atespace", "atespace", in.Atespace)
	return classify(a.Substrate.EnsureAtespace(ctx, in.Atespace))
}

// EnsureActorTemplate provisions the ActorTemplate a task's actor derives from.
// The template carries the container image and the environment the runner needs:
// the task and workspace specs, the model credentials, and the address of the
// task workflow.
func (a *Activities) EnsureActorTemplate(ctx context.Context, in TemplateInput) (TemplateRef, error) {
	if in.Template.Atespace == "" || in.Template.Name == "" {
		return TemplateRef{}, invalidSpec("template atespace and name are required")
	}
	logger := activity.GetLogger(ctx)
	logger.Info("ensuring actor template", "template", in.Template.Name, "image", in.Image)

	env, err := a.containerEnv(ctx, in)
	if err != nil {
		return TemplateRef{}, err
	}

	tmpl, err := a.Substrate.EnsureActorTemplateWithImage(ctx,
		in.Template.Atespace, in.Template.Name,
		in.Template.Atespace, in.Template.Name,
		in.Image, env)
	if err != nil {
		return TemplateRef{}, classify(err)
	}
	ref := in.Template
	if tmpl.GetMetadata().GetName() != "" {
		ref = TemplateRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: tmpl.GetMetadata().GetName()}
	}
	if ref.Atespace == "" {
		ref.Atespace = in.Template.Atespace
	}
	return ref, nil
}

// EnsureActor creates the task's actor from its template. A crashed actor is
// replaced, which is how a task recovers from a sandbox that died.
func (a *Activities) EnsureActor(ctx context.Context, in ActorInput) error {
	if in.Actor.Name == "" || in.Template.Name == "" {
		return invalidSpec("actor name and template name are required")
	}
	activity.GetLogger(ctx).Info("ensuring actor", "actor", in.Actor.Name, "template", in.Template.Name)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	_, err := a.Substrate.EnsureActor(ctx, in.Actor.Atespace, in.Actor.Name, in.Template.Atespace, in.Template.Name)
	return classify(err)
}

// ApplyEgressPolicy installs the gateway's allowlist as the actor's egress
// policy, replacing whatever was there before.
func (a *Activities) ApplyEgressPolicy(ctx context.Context, in EgressInput) error {
	if in.Actor.Name == "" {
		return invalidSpec("actor name is required")
	}
	activity.GetLogger(ctx).Info("applying egress policy", "actor", in.Actor.Name)
	return classify(a.Substrate.ApplyEgressPolicy(ctx, in.Actor.Atespace, in.Actor.Name, in.Allowlist))
}

// SuspendActor checkpoints the actor's state and stops it.
func (a *Activities) SuspendActor(ctx context.Context, in ActorRef) error {
	if in.Name == "" {
		return invalidSpec("actor name is required")
	}
	activity.GetLogger(ctx).Info("suspending actor", "actor", in.Name)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	return classify(a.Substrate.SuspendActor(ctx, in.Atespace, in.Name))
}

// ResumeActor places the actor on a worker and returns the worker's pod IP.
// Restoring a snapshot can take a while, so the call reports liveness while it
// waits.
func (a *Activities) ResumeActor(ctx context.Context, in ActorRef) (string, error) {
	if in.Name == "" {
		return "", invalidSpec("actor name is required")
	}
	activity.GetLogger(ctx).Info("resuming actor", "actor", in.Name)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	_, workerIP, err := a.Substrate.ResumeActor(ctx, in.Atespace, in.Name)
	if err != nil {
		return "", classify(err)
	}
	return workerIP, nil
}

// ObserveActor reads an actor's current state. The workflow resyncs with it, so
// a sandbox that crashed, was rescheduled onto another worker, or disappeared
// is noticed without anyone asking.
func (a *Activities) ObserveActor(ctx context.Context, in ActorRef) (ActorObservation, error) {
	if in.Name == "" {
		return ActorObservation{}, invalidSpec("actor name is required")
	}
	actor, err := a.Substrate.GetActor(ctx, in.Atespace, in.Name)
	if err != nil {
		return ActorObservation{}, classify(err)
	}
	if actor == nil {
		return ActorObservation{}, nil
	}
	return ActorObservation{
		Exists:   true,
		State:    actor.GetStatus().GetState().String(),
		WorkerIP: actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp(),
	}, nil
}

// DeleteActorIfExists removes the task's actor. A missing actor is a success, so
// the call doubles as the compensation for EnsureActor.
func (a *Activities) DeleteActorIfExists(ctx context.Context, in ActorRef) error {
	if in.Name == "" {
		return invalidSpec("actor name is required")
	}
	activity.GetLogger(ctx).Info("deleting actor", "actor", in.Name)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	return classify(a.Substrate.DeleteActor(ctx, in.Atespace, in.Name))
}

// DeleteEgressPolicyIfExists removes the actor's egress policy. It compensates
// ApplyEgressPolicy when provisioning is rolled back before the actor exists.
func (a *Activities) DeleteEgressPolicyIfExists(ctx context.Context, in ActorRef) error {
	if in.Name == "" {
		return invalidSpec("actor name is required")
	}
	activity.GetLogger(ctx).Info("deleting egress policy", "actor", in.Name)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	return classify(a.Substrate.DeleteEgressPolicy(ctx, in.Atespace, in.Name))
}

// DeleteActorTemplates removes every ActorTemplate belonging to a task.
// Templates are matched by name pattern because a task accumulates one per
// distinct spec revision.
func (a *Activities) DeleteActorTemplates(ctx context.Context, in TemplatesInput) error {
	if in.TaskName == "" {
		return invalidSpec("task name is required")
	}
	logger := activity.GetLogger(ctx)

	stop := heartbeatUntilDone(ctx)
	defer stop()

	templates, err := a.Substrate.ListActorTemplates(ctx, in.Atespace)
	if err != nil {
		return classify(err)
	}
	pattern := TaskTemplatePattern(in.TaskName)
	for _, tmpl := range templates {
		name := tmpl.GetMetadata().GetName()
		if !pattern.MatchString(name) {
			continue
		}
		logger.Info("deleting actor template", "atespace", in.Atespace, "template", name)
		if err := a.Substrate.DeleteActorTemplate(ctx, in.Atespace, name); err != nil {
			// Substrate rejects template deletion while an actor still references
			// it, which resolves once the actor is gone. Let the retry policy wait.
			return classify(err)
		}
	}
	return nil
}

// heartbeatUntilDone reports liveness on an interval until the returned function
// is called. Activities that block in a single control-plane call cannot
// heartbeat inline, and without a heartbeat they would neither be detected as
// stuck nor learn that they were cancelled.
func heartbeatUntilDone(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return func() { close(done) }
}
