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

package activities_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// noSecrets never finds a key and never touches a cluster.
func noSecrets(_ any, _, _, _ string) (string, error) { return "", nil }

// newEnv wires the activities against a fake Control API.
func newEnv(t *testing.T, control *substratetest.ControlServer) (*testsuite.TestActivityEnvironment, *activities.Activities) {
	t.Helper()
	client, stop, err := substratetest.Start(control)
	if err != nil {
		t.Fatalf("starting the substrate fake: %v", err)
	}
	t.Cleanup(stop)

	acts := &activities.Activities{
		Substrate:         client,
		TemporalAddress:   "temporal-frontend.temporal.svc.cluster.local:7233",
		TemporalNamespace: "default",
	}
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(acts)
	return env, acts
}

func testTask() *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "job", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image:   "ghcr.io/example/agent",
			Command: []string{"/bin/agent"},
			Env:     []*v1alpha1.EnvVar{{Name: "GOAL", Value: "fix the bug"}},
		},
	}
}

func TestProvisioningSequence(t *testing.T) {
	control := substratetest.NewControlServer()
	env, acts := newEnv(t, control)

	if _, err := env.ExecuteActivity(acts.EnsureAtespace, activities.AtespaceInput{Atespace: "default"}); err != nil {
		t.Fatalf("EnsureAtespace failed: %v", err)
	}

	task := testTask()
	template := activities.TemplateRef{
		Atespace: "default",
		Name:     activities.TaskTemplateName(task, nil),
	}
	value, err := env.ExecuteActivity(acts.EnsureActorTemplate, activities.TemplateInput{
		Template:   template,
		Image:      task.Spec.Image,
		Task:       task,
		WorkflowID: "default/job",
	})
	if err != nil {
		t.Fatalf("EnsureActorTemplate failed: %v", err)
	}
	var resolved activities.TemplateRef
	if err := value.Get(&resolved); err != nil {
		t.Fatalf("decoding the template ref: %v", err)
	}
	if resolved != template {
		t.Errorf("expected template %v, got %v", template, resolved)
	}

	actor := activities.ActorRef{Atespace: "default", Name: "job"}
	if _, err := env.ExecuteActivity(acts.EnsureActor, activities.ActorInput{Actor: actor, Template: resolved}); err != nil {
		t.Fatalf("EnsureActor failed: %v", err)
	}
	if _, err := env.ExecuteActivity(acts.ApplyEgressPolicy, activities.EgressInput{
		Actor:     actor,
		Allowlist: &v1alpha1.EgressAllowlist{Hosts: []*v1alpha1.HostRule{{Host: "github.com", Port: 443}}},
	}); err != nil {
		t.Fatalf("ApplyEgressPolicy failed: %v", err)
	}

	value, err = env.ExecuteActivity(acts.ResumeActor, actor)
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	var workerIP string
	if err := value.Get(&workerIP); err != nil {
		t.Fatalf("decoding the worker IP: %v", err)
	}
	if workerIP != "10.244.1.42" {
		t.Errorf("expected worker IP 10.244.1.42, got %q", workerIP)
	}

	if got := control.Atespaces(); len(got) != 1 || got[0] != "default" {
		t.Errorf("expected the atespace to be created, got %v", got)
	}
	if got := control.CreatedActors(); len(got) != 1 || got[0] != "job" {
		t.Errorf("expected actor job, got %v", got)
	}
	if got := control.CreatedPolicies(); len(got) != 1 || got[0] != "job" {
		t.Errorf("expected an egress policy for job, got %v", got)
	}
	if got := control.ResumedActors(); len(got) != 1 || got[0] != "job" {
		t.Errorf("expected job to be resumed, got %v", got)
	}
}

// The runner inside the sandbox is handed the specs and the address of the
// workflow that owns the task.
func TestActorTemplateCarriesTheRunnerEnvironment(t *testing.T) {
	control := substratetest.NewControlServer()
	env, acts := newEnv(t, control)
	acts.SecretResolver = nil

	task := testTask()
	task.Status = &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning, WorkerIp: "10.0.0.1"}
	workspaces := []*v1alpha1.Workspace{{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindWorkspace,
		Metadata:   &v1alpha1.ObjectMeta{Name: "repo", Atespace: "default"},
		Spec: &v1alpha1.WorkspaceSpec{
			Git: []*v1alpha1.GitRepo{{Name: "repo", Repo: "https://github.com/example/repo"}},
		},
	}}
	name := activities.TaskTemplateName(task, workspaces)

	if _, err := env.ExecuteActivity(acts.EnsureActorTemplate, activities.TemplateInput{
		Template:   activities.TemplateRef{Atespace: "default", Name: name},
		Image:      task.Spec.Image,
		Task:       task,
		Workspaces: workspaces,
		WorkflowID: "default/job",
	}); err != nil {
		t.Fatalf("EnsureActorTemplate failed: %v", err)
	}

	templateEnv, err := control.TemplateEnv(name)
	if err != nil {
		t.Fatalf("reading the created template: %v", err)
	}
	if got := templateEnv["GOAL"]; got != "fix the bug" {
		t.Errorf("expected the task env to be passed through, got %q", got)
	}
	if got := templateEnv[activities.EnvWorkflowID]; got != "default/job" {
		t.Errorf("expected the workflow ID in the container env, got %q", got)
	}
	if got := templateEnv[activities.EnvTemporalAddress]; got == "" {
		t.Error("expected the Temporal address in the container env")
	}
	taskYAML := templateEnv[activities.EnvTaskYAML]
	if !strings.Contains(taskYAML, "name: job") {
		t.Errorf("expected the task spec in %s, got %q", activities.EnvTaskYAML, taskYAML)
	}
	// The status is the workflow's own view of the task and changes constantly;
	// shipping it would make every status update a new template.
	if strings.Contains(taskYAML, "workerIP") {
		t.Errorf("expected no status in %s, got %q", activities.EnvTaskYAML, taskYAML)
	}
	if got := templateEnv[activities.EnvWorkspacesYAML]; !strings.Contains(got, "name: repo") {
		t.Errorf("expected the workspace spec in %s, got %q", activities.EnvWorkspacesYAML, got)
	}
}

// A template name is stable for one desired state and different for another.
func TestTaskTemplateNameTracksTheDesiredState(t *testing.T) {
	task := testTask()
	base := activities.TaskTemplateName(task, nil)

	withStatus := testTask()
	withStatus.Status = &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning, WorkerIp: "10.0.0.1"}
	if got := activities.TaskTemplateName(withStatus, nil); got != base {
		t.Errorf("status must not change the template name: %q vs %q", got, base)
	}

	suspended := testTask()
	suspended.Spec.Suspend = true
	if got := activities.TaskTemplateName(suspended, nil); got != base {
		t.Errorf("suspending must not change the template name: %q vs %q", got, base)
	}

	reimaged := testTask()
	reimaged.Spec.Image = "ghcr.io/example/other"
	if got := activities.TaskTemplateName(reimaged, nil); got == base {
		t.Error("a new image must yield a new template name")
	}

	withWorkspace := activities.TaskTemplateName(task, []*v1alpha1.Workspace{{
		Metadata: &v1alpha1.ObjectMeta{Name: "repo"},
	}})
	if withWorkspace == base {
		t.Error("binding a workspace must yield a new template name")
	}

	if !activities.TaskTemplatePattern("job").MatchString(base) {
		t.Errorf("the deletion pattern must match generated names, got %q", base)
	}
}

func TestTeardownRemovesTheActorAndItsTemplates(t *testing.T) {
	control := substratetest.NewControlServer(
		"job-tmpl-0a1b2c3d",               // current revision of task "job"
		"job-tmpl-deadbeef",               // stale revision of task "job"
		"job-tmpl-deadbeef-tmpl-01234567", // belongs to a task named "job-tmpl-deadbeef"
		"jobs-tmpl-0a1b2c3d",              // belongs to task "jobs"
		"default-template",
	)
	env, acts := newEnv(t, control)
	actor := activities.ActorRef{Atespace: "default", Name: "job"}

	if _, err := env.ExecuteActivity(acts.DeleteActorIfExists, actor); err != nil {
		t.Fatalf("DeleteActorIfExists failed: %v", err)
	}
	if _, err := env.ExecuteActivity(acts.DeleteEgressPolicyIfExists, actor); err != nil {
		t.Fatalf("DeleteEgressPolicyIfExists failed: %v", err)
	}
	if _, err := env.ExecuteActivity(acts.DeleteActorTemplates, activities.TemplatesInput{
		Atespace: "default", TaskName: "job",
	}); err != nil {
		t.Fatalf("DeleteActorTemplates failed: %v", err)
	}

	if got := control.DeletedActors(); len(got) != 1 || got[0] != "job" {
		t.Errorf("expected actor job to be deleted, got %v", got)
	}
	if got := control.DeletedPolicies(); len(got) != 1 || got[0] != "job" {
		t.Errorf("expected the egress policy to be deleted, got %v", got)
	}
	deleted := map[string]bool{}
	for _, name := range control.DeletedTemplates() {
		deleted[name] = true
	}
	for _, want := range []string{"job-tmpl-0a1b2c3d", "job-tmpl-deadbeef"} {
		if !deleted[want] {
			t.Errorf("expected template %s to be deleted, got %v", want, control.DeletedTemplates())
		}
	}
	for _, keep := range []string{"job-tmpl-deadbeef-tmpl-01234567", "jobs-tmpl-0a1b2c3d", "default-template"} {
		if deleted[keep] {
			t.Errorf("template %s should not have been deleted", keep)
		}
	}
}

func TestObserveActor(t *testing.T) {
	control := substratetest.NewControlServer()
	env, acts := newEnv(t, control)
	actor := activities.ActorRef{Atespace: "default", Name: "job"}

	// Substrate does not have the actor yet.
	value, err := env.ExecuteActivity(acts.ObserveActor, actor)
	if err != nil {
		t.Fatalf("ObserveActor failed: %v", err)
	}
	var observed activities.ActorObservation
	if err := value.Get(&observed); err != nil {
		t.Fatalf("decoding the observation: %v", err)
	}
	if observed.Exists {
		t.Errorf("expected a missing actor, got %+v", observed)
	}

	if _, err := env.ExecuteActivity(acts.EnsureActor, activities.ActorInput{
		Actor:    actor,
		Template: activities.TemplateRef{Atespace: "default", Name: "job-tmpl-0a1b2c3d"},
	}); err != nil {
		t.Fatalf("EnsureActor failed: %v", err)
	}
	if _, err := env.ExecuteActivity(acts.ResumeActor, actor); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	value, err = env.ExecuteActivity(acts.ObserveActor, actor)
	if err != nil {
		t.Fatalf("ObserveActor failed: %v", err)
	}
	if err := value.Get(&observed); err != nil {
		t.Fatalf("decoding the observation: %v", err)
	}
	if !observed.Exists || observed.State != activities.ActorStateRunning {
		t.Errorf("expected a running actor, got %+v", observed)
	}
	if observed.WorkerIP != "10.244.1.42" {
		t.Errorf("expected the worker IP to be reported, got %q", observed.WorkerIP)
	}
}

func TestAwaitWorkspaceReady(t *testing.T) {
	control := substratetest.NewControlServer()
	env, acts := newEnv(t, control)

	sandbox := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("check") != "workspace" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer sandbox.Close()

	in := activities.WorkspaceReadyInput{
		Actor:        activities.ActorRef{Atespace: "default", Name: "job"},
		WorkerIP:     strings.TrimPrefix(sandbox.URL, "http://"),
		Timeout:      time.Second,
		PollInterval: 10 * time.Millisecond,
	}
	value, err := env.ExecuteActivity(acts.AwaitWorkspaceReady, in)
	if err != nil {
		t.Fatalf("AwaitWorkspaceReady failed: %v", err)
	}
	var ready bool
	if err := value.Get(&ready); err != nil {
		t.Fatalf("decoding readiness: %v", err)
	}
	if !ready {
		t.Error("expected the workspace to be reported ready")
	}

	// An unreachable sandbox expires instead of failing, so a slow maiden run
	// leaves the task running rather than failing it.
	in.WorkerIP = "127.0.0.1:1"
	in.Timeout = 50 * time.Millisecond
	value, err = env.ExecuteActivity(acts.AwaitWorkspaceReady, in)
	if err != nil {
		t.Fatalf("AwaitWorkspaceReady failed: %v", err)
	}
	if err := value.Get(&ready); err != nil {
		t.Fatalf("decoding readiness: %v", err)
	}
	if ready {
		t.Error("expected the workspace to be reported as not ready")
	}
}

// A Substrate rejection that no retry can fix is reported as non-retryable, and
// anything else is left for the retry policy.
func TestErrorClassification(t *testing.T) {
	control := substratetest.NewControlServer()
	control.CreateActorErr = status.Error(codes.InvalidArgument, "actor name is not a valid DNS label")
	env, acts := newEnv(t, control)

	_, err := env.ExecuteActivity(acts.EnsureActor, activities.ActorInput{
		Actor:    activities.ActorRef{Atespace: "default", Name: "job"},
		Template: activities.TemplateRef{Atespace: "default", Name: "job-tmpl-0a1b2c3d"},
	})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected an application error, got %v", err)
	}
	if appErr.Type() != activities.ErrTypePermanent {
		t.Errorf("expected type %s, got %s", activities.ErrTypePermanent, appErr.Type())
	}
	if !appErr.NonRetryable() {
		t.Error("a rejected spec must not be retried")
	}

	control.CreateActorErr = status.Error(codes.Unavailable, "control plane is restarting")
	_, err = env.ExecuteActivity(acts.EnsureActor, activities.ActorInput{
		Actor:    activities.ActorRef{Atespace: "default", Name: "job"},
		Template: activities.TemplateRef{Atespace: "default", Name: "job-tmpl-0a1b2c3d"},
	})
	if !errors.As(err, &appErr) {
		t.Fatalf("expected an application error, got %v", err)
	}
	if appErr.Type() != activities.ErrTypeSubstrate {
		t.Errorf("expected type %s, got %s", activities.ErrTypeSubstrate, appErr.Type())
	}
	if appErr.NonRetryable() {
		t.Error("an unavailable control plane must be retried")
	}
}

// Missing input is rejected before anything is called.
func TestInvalidInputIsNonRetryable(t *testing.T) {
	control := substratetest.NewControlServer()
	env, acts := newEnv(t, control)

	_, err := env.ExecuteActivity(acts.EnsureAtespace, activities.AtespaceInput{})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected an application error, got %v", err)
	}
	if appErr.Type() != activities.ErrTypeInvalidSpec || !appErr.NonRetryable() {
		t.Errorf("expected a non-retryable %s, got %s (retryable=%v)",
			activities.ErrTypeInvalidSpec, appErr.Type(), !appErr.NonRetryable())
	}
	if len(control.Atespaces()) != 0 {
		t.Errorf("expected no call to Substrate, got %v", control.Atespaces())
	}
}
