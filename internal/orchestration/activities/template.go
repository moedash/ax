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

package activities

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// templateDigestBytes is how many bytes of the spec digest go into a per-task
// ActorTemplate name.
const templateDigestBytes = 4

// TaskTemplateName derives a task's ActorTemplate name from the specs that end
// up inside the container: the task itself and every workspace it binds. A spec
// change yields a new template, and an unchanged spec always yields the same
// one, which is what makes template provisioning idempotent.
//
// The digest is taken over named fields rather than over marshaled bytes. Wire
// bytes are a property of the protobuf library, so an upgrade of it could
// rename every template in a cluster and strand every sandbox.
//
// The digest deliberately covers only the desired state. Task status, the
// suspend flag, and resolved credentials are excluded so that a status update
// or a rotated key does not strand a fresh template on every reconcile.
func TaskTemplateName(task *v1alpha1.Task, workspaces []*v1alpha1.Workspace) string {
	d := &digest{h: sha256.New()}
	spec := SandboxSpec(task).GetSpec()

	d.field("image", spec.GetImage())
	for _, arg := range spec.GetCommand() {
		d.field("command", arg)
	}
	for _, env := range spec.GetEnv() {
		d.field("env", env.GetName(), env.GetValue())
	}
	d.field("debug", strconv.FormatBool(spec.GetDebug()))
	d.field("cpu", spec.GetResources().GetRequests().GetCpu(), spec.GetResources().GetLimits().GetCpu())
	d.field("memory", spec.GetResources().GetRequests().GetMemory(), spec.GetResources().GetLimits().GetMemory())
	for _, ref := range spec.WorkspaceRefs() {
		d.field("workspace", ref.GetName(), ref.GetPath(), ref.GetGoal())
	}
	for _, ws := range workspaces {
		digestWorkspace(d, ws)
	}
	return fmt.Sprintf("%s-tmpl-%x", task.GetMetadata().GetName(), d.sum()[:templateDigestBytes])
}

// digestWorkspace covers what a workspace puts inside the sandbox.
func digestWorkspace(d *digest, ws *v1alpha1.Workspace) {
	if ws == nil {
		return
	}
	spec := ws.GetSpec()
	d.field("workspace-spec", ws.GetMetadata().GetName())
	for _, repo := range spec.GetGit() {
		d.field("git", repo.GetName(), repo.GetRepo(), repo.GetBranch(), repo.GetDir(), strconv.Itoa(int(repo.GetDepth())))
	}
	for _, registry := range spec.GetMcp().GetRegistries() {
		d.field("mcp-registry", registry.GetProvider(), registry.GetProject(), registry.GetQuery())
		for _, server := range registry.GetServers() {
			digestMCPServer(d, server)
		}
	}
	for _, server := range spec.GetMcp().GetServers() {
		digestMCPServer(d, server)
	}
	for _, registry := range spec.GetSkills().GetRegistries() {
		d.field("skill-registry", registry.GetProvider(), registry.GetProject(), registry.GetQuery())
	}
	d.field("skills-path", spec.GetSkills().GetPath())
}

func digestMCPServer(d *digest, server *v1alpha1.MCPServer) {
	values := []string{server.GetName(), server.GetEndpoint(), server.GetCommand()}
	values = append(values, server.GetArgs()...)
	d.field("mcp-server", values...)
}

// digest writes named values in the order they are given, with separators that
// cannot appear in a field name, so no two different specs hash alike.
type digest struct {
	h hash.Hash
}

func (d *digest) field(name string, values ...string) {
	_, _ = io.WriteString(d.h, name)
	for _, value := range values {
		_, _ = io.WriteString(d.h, "\x00")
		_, _ = io.WriteString(d.h, value)
	}
	_, _ = io.WriteString(d.h, "\n")
}

func (d *digest) sum() []byte {
	return d.h.Sum(nil)
}

// TaskTemplatePattern matches every ActorTemplate name TaskTemplateName can
// produce for the given task, across all spec revisions.
func TaskTemplatePattern(taskName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s-tmpl-[0-9a-f]{%d}$", regexp.QuoteMeta(taskName), 2*templateDigestBytes))
}

// SandboxSpec returns the part of a task that decides what its sandbox is made
// of. It is what the runner inside the container is handed, what the template
// digest is taken over, and what a workflow compares to decide whether a task
// has to be provisioned again.
//
// The status and the suspend flag are dropped. Both change while a task runs,
// and neither changes what the sandbox is, so keeping them would strand a new
// actor template on every status update and every suspend.
func SandboxSpec(task *v1alpha1.Task) *v1alpha1.Task {
	out, ok := proto.Clone(task).(*v1alpha1.Task)
	if !ok || out == nil {
		return &v1alpha1.Task{}
	}
	out.Status = nil
	if out.Spec != nil {
		out.Spec.Suspend = false
	}
	return out
}

// containerEnv builds the environment of a task's container: the task's own
// env, the model credentials, the specs the runner needs, and the coordinates
// of the workflow that owns the task.
func (a *Activities) containerEnv(ctx context.Context, in TemplateInput) (map[string]string, error) {
	env := make(map[string]string)
	for _, e := range in.Task.GetSpec().GetEnv() {
		if e.GetName() != "" {
			env[e.GetName()] = e.GetValue()
		}
	}

	if key := a.lookupGeminiKey(ctx, in.Template.Atespace); key != "" {
		env[geminiSecretKey] = key
	}

	taskYAML, err := yaml.Marshal(SandboxSpec(in.Task))
	if err != nil {
		return nil, invalidSpec("rendering task yaml: %v", err)
	}
	env[v1alpha1.EnvTaskYAML] = string(taskYAML)

	wsYAML, err := marshalWorkspaces(in.Workspaces)
	if err != nil {
		return nil, invalidSpec("rendering workspace yaml: %v", err)
	}
	if wsYAML != "" {
		env[v1alpha1.EnvWorkspacesYAML] = wsYAML
	}

	// Without these the sandbox has no route to the control plane, and the runner
	// only logs how the command finished.
	if a.ReportCompletion {
		if in.WorkflowID != "" {
			env[v1alpha1.EnvWorkflowID] = in.WorkflowID
		}
		if a.TemporalAddress != "" {
			env[v1alpha1.EnvTemporalAddress] = a.TemporalAddress
		}
		if a.TemporalNamespace != "" {
			env[v1alpha1.EnvTemporalNamespace] = a.TemporalNamespace
		}
	}
	return env, nil
}

// lookupGeminiKey resolves the Gemini API key for the task container,
// preferring the Kubernetes secret in the task's atespace and falling back to
// the worker's own environment. It returns "" when neither source has a value.
func (a *Activities) lookupGeminiKey(ctx context.Context, atespace string) string {
	if a.SecretResolver != nil {
		lookupCtx, cancel := context.WithTimeout(ctx, secretLookupTimeout)
		defer cancel()
		if key, err := a.SecretResolver(lookupCtx, atespace, geminiSecretName, geminiSecretKey); err == nil && key != "" {
			return key
		}
	}
	return os.Getenv(geminiSecretKey)
}

// marshalWorkspaces renders the workspaces as a multi-document YAML stream in
// binding order, skipping nil entries. It returns "" when there is nothing to
// render.
func marshalWorkspaces(workspaces []*v1alpha1.Workspace) (string, error) {
	present := make([]*v1alpha1.Workspace, 0, len(workspaces))
	for _, ws := range workspaces {
		if ws != nil {
			present = append(present, ws)
		}
	}
	if len(present) == 0 {
		return "", nil
	}

	var sb strings.Builder
	enc := yaml.NewEncoder(&sb)
	for _, ws := range present {
		if err := enc.Encode(ws); err != nil {
			return "", err
		}
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// httpClient returns the probe client, defaulting to one that gives up quickly
// so a poll keeps its cadence.
func (a *Activities) httpClient() *http.Client {
	if a.HTTPClient != nil {
		return a.HTTPClient
	}
	return &http.Client{Timeout: probeTimeout}
}
