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

package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	APIVersion    = "ax.io/v1alpha1"
	KindTask      = "Task"
	KindWorkspace = "Workspace"
	KindModel     = "Model"

	DefaultTaskImage = "gcr.io/ax-substrate/ate-images/ax-task-runner"

	// DefaultAtespace holds resources whose manifest leaves the atespace empty.
	DefaultAtespace = "default"
)

// Environment variables AX sets in every task container. They are the contract
// between the control plane and whatever runs as PID 1 inside the sandbox.
const (
	// EnvTaskYAML carries the Task resource, without its status.
	EnvTaskYAML = "AX_TASK_YAML"
	// EnvWorkspacesYAML carries every bound Workspace as a multi-document YAML
	// stream, in binding order.
	EnvWorkspacesYAML = "AX_WORKSPACES_YAML"
	// EnvWorkflowID names the task's workflow, which is where the runner reports
	// how the task command finished. It is only set when the control plane runs
	// tasks through Temporal and completion reporting is turned on.
	EnvWorkflowID = "AX_WORKFLOW_ID"
	// EnvTemporalAddress and EnvTemporalNamespace are where that report goes.
	EnvTemporalAddress   = "AX_TEMPORAL_ADDRESS"
	EnvTemporalNamespace = "AX_TEMPORAL_NAMESPACE"
	// EnvSandboxGeneration numbers the sandbox within its task. The runner sends
	// it with the report, so a report from a sandbox the task has since replaced
	// is not taken for the current one.
	EnvSandboxGeneration = "AX_SANDBOX_GENERATION"
)

// Task phases reported on status.phase. A task's phase is derived from the state
// of its sandbox, so the same task can move back and forth between them.
const (
	// PhasePending marks a task whose sandbox is still being provisioned.
	PhasePending = "Pending"
	// PhaseRunning marks a task whose actor is running on a worker.
	PhaseRunning = "Running"
	// PhaseSuspended marks a task whose actor has been checkpointed and stopped.
	PhaseSuspended = "Suspended"
	// PhaseCompleted marks a task whose command has exited. The sandbox stays up
	// and inspectable, and status.exitCode carries the command's exit status.
	PhaseCompleted = "Completed"
	// PhaseFailed marks a task whose sandbox could not be provisioned.
	PhaseFailed = "Failed"
	// PhaseTerminating marks a task whose deletion has been requested and whose
	// actor is being torn down. The record disappears once cleanup completes.
	PhaseTerminating = "Terminating"
)

// Condition types reported on status.conditions.
const (
	// ConditionReady reports whether the task as a whole is ready to do work: its
	// actor is running and the workspace inside it has finished setting up.
	ConditionReady = "Ready"
	// ConditionWorkspaceReady reports whether the workspace inside the actor has
	// finished setting up.
	ConditionWorkspaceReady = "WorkspaceReady"
)

// Condition status values.
const (
	ConditionTrue  = "True"
	ConditionFalse = "False"
)

// YAML encoding.
//
// The API kinds are protobuf messages, so protojson is the single source of
// truth for their wire names (lowerCamelCase), timestamp format (RFC 3339), and
// which fields are omitted when unset. YAML is bridged through it: manifests are
// decoded generically, converted to JSON, and then decoded strictly by protojson,
// so unknown or misspelled fields are rejected. Output is protojson rendered as
// a yaml.Node so field order follows the proto definition instead of being
// sorted.
//
// A few legacy manifest shapes are still accepted. They are rewritten on the
// generic document before strict decoding by the per-kind normalizers below.

var (
	protoMarshal   = protojson.MarshalOptions{}
	protoUnmarshal = protojson.UnmarshalOptions{} // unknown fields are errors
)

// normalizer rewrites a generically decoded document in place so legacy shapes
// match the current schema before strict decoding.
type normalizer func(doc map[string]any)

func marshalYAML(m proto.Message) (any, error) {
	data, err := protoMarshal.Marshal(m)
	if err != nil {
		return nil, err
	}
	return jsonToYAMLNode(data)
}

func unmarshalYAML(value *yaml.Node, m proto.Message, normalize normalizer) error {
	var doc any
	if err := value.Decode(&doc); err != nil {
		return err
	}
	if obj, ok := doc.(map[string]any); ok && normalize != nil {
		normalize(obj)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("converting yaml to json: %w", err)
	}
	return protoUnmarshal.Unmarshal(data, m)
}

// jsonToYAMLNode converts a JSON document into a yaml.Node tree, preserving
// object key order and tagging scalars so the YAML encoder quotes strings that
// would otherwise read as another type (for example a status of "True").
func jsonToYAMLNode(data []byte) (*yaml.Node, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return readJSONValue(dec)
}

func readJSONValue(dec *json.Decoder) (*yaml.Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			n := &yaml.Node{Kind: yaml.MappingNode}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, val)
			}
			_, err = dec.Token() // closing '}'
			return n, err
		case '[':
			n := &yaml.Node{Kind: yaml.SequenceNode}
			for dec.More() {
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, val)
			}
			_, err = dec.Token() // closing ']'
			return n, err
		}
		return nil, fmt.Errorf("unexpected json delimiter %q", v)
	case string:
		// protojson renders timestamps as RFC 3339 strings. Let those stay plain
		// scalars, which YAML reads as timestamps; every other string is tagged so
		// values like "True" or "1" are quoted and read back as strings.
		tag := "!!str"
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
			tag = "!!timestamp"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v}, nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(v.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v.String()}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v)}, nil
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
	return nil, fmt.Errorf("unexpected json token %v", tok)
}

// Legacy manifest shapes.

// normalizeWorkspace accepts spec.mcp written as a bare list of servers.
func normalizeWorkspace(doc map[string]any) {
	spec, _ := doc["spec"].(map[string]any)
	if list, ok := spec["mcp"].([]any); ok {
		spec["mcp"] = map[string]any{"servers": list}
	}
}

// normalizeModel accepts older spellings of the secret reference: spec.secretKeyRef,
// spec.apiKey.secretKeyRef, and a bare string meaning the secret name and key are
// the same.
func normalizeModel(doc map[string]any) {
	spec, ok := doc["spec"].(map[string]any)
	if !ok {
		return
	}
	if _, has := spec["secretKey"]; !has {
		if ref, ok := spec["secretKeyRef"]; ok {
			spec["secretKey"] = ref
		} else if apiKey, ok := spec["apiKey"].(map[string]any); ok {
			if ref, ok := apiKey["secretKeyRef"]; ok {
				spec["secretKey"] = ref
			}
		}
	}
	delete(spec, "secretKeyRef")
	delete(spec, "apiKey")
	if s, ok := spec["secretKey"].(string); ok {
		spec["secretKey"] = map[string]any{"name": s, "key": s}
	}
}

// Per-kind YAML methods. Nested messages are handled by protojson as part of
// their parent, so only the kinds that are encoded on their own need these.

func (t *Task) MarshalYAML() (any, error)        { return marshalYAML(t) }
func (t *Task) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, t, nil) }

func (w *Workspace) MarshalYAML() (any, error)        { return marshalYAML(w) }
func (w *Workspace) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, w, normalizeWorkspace) }

func (m *Model) MarshalYAML() (any, error)        { return marshalYAML(m) }
func (m *Model) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, m, normalizeModel) }

// Workspace bindings.
//
// A task binds one or more workspaces through spec.workspaces. Every entry is
// treated the same way regardless of how many there are.

// DefaultWorkspacePath is the durable volume every workspace lives under. A
// binding without a path is mounted at DefaultWorkspacePath/<name>, and a task
// with no bindings gets an empty working directory at DefaultWorkspacePath.
const DefaultWorkspacePath = "/workspace"

// WorkspaceRefs returns the workspaces the task binds, in declaration order,
// with nil entries dropped.
func (s *TaskSpec) WorkspaceRefs() []*WorkspaceRef {
	if s == nil {
		return nil
	}
	out := make([]*WorkspaceRef, 0, len(s.GetWorkspaces()))
	for _, r := range s.GetWorkspaces() {
		if r != nil {
			out = append(out, r)
		}
	}
	return out
}

// WorkspacePaths returns the mount path of each workspace in WorkspaceRefs, in
// the same order. An explicit path wins; otherwise the workspace lands at
// DefaultWorkspacePath/<name>.
func (s *TaskSpec) WorkspacePaths() []string {
	refs := s.WorkspaceRefs()
	paths := make([]string, len(refs))
	for i, r := range refs {
		if r.GetPath() != "" {
			paths[i] = r.GetPath()
		} else {
			paths[i] = DefaultWorkspacePath + "/" + r.GetName()
		}
	}
	return paths
}

// Resource names.
//
// A resource's name and atespace become Substrate resource names (the task's
// actor and ActorTemplate, and the atespace itself), which must be lowercase
// RFC 1123 labels. Checking them at apply time turns an asynchronous
// ActorCreationFailed condition into an immediate error.

// MaxNameLength is the longest name or atespace a resource may have.
const MaxNameLength = 63

// MaxTaskNameLength is the longest a task name may be. A task's actor template
// is named after the task with a suffix of fourteen characters, "-tmpl-" and
// eight hex digits, and Substrate resource names are DNS labels, so the suffix
// comes out of the task name's budget.
const MaxTaskNameLength = MaxNameLength - 14

var nameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidateName reports whether s can be used as a resource name or atespace: a
// lowercase RFC 1123 label of at most MaxNameLength characters.
func ValidateName(s string) error {
	if s == "" {
		return errors.New("must not be empty")
	}
	if len(s) > MaxNameLength || !nameRegexp.MatchString(s) {
		return fmt.Errorf("must be a lowercase RFC 1123 label: at most %d lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character", MaxNameLength)
	}
	return nil
}

// ValidateObjectMeta checks a resource's name and, when set, its atespace. An
// empty atespace is fine here; the API server defaults it.
func ValidateObjectMeta(meta *ObjectMeta) error {
	if err := ValidateName(meta.GetName()); err != nil {
		return fmt.Errorf("metadata.name: invalid value %q: %w", meta.GetName(), err)
	}
	if atespace := meta.GetAtespace(); atespace != "" {
		if err := ValidateName(atespace); err != nil {
			return fmt.Errorf("metadata.atespace: invalid value %q: %w", atespace, err)
		}
	}
	return nil
}

// ValidateWorkspace reports the first problem with a workspace that would make
// it unusable. It is called by the API server before saving.
func ValidateWorkspace(w *Workspace) error {
	if err := ValidateObjectMeta(w.GetMetadata()); err != nil {
		return err
	}
	if w.GetSpec() != nil {
		for i, f := range w.GetSpec().GetFiles() {
			field := fmt.Sprintf("spec.files[%d]", i)
			if f == nil || f.GetPath() == "" {
				return fmt.Errorf("%s: path is required", field)
			}
		}
	}
	return nil
}

// ValidateModel reports the first problem with a model that would make it
// unusable. It is called by the API server before saving.
func ValidateModel(m *Model) error {
	return ValidateObjectMeta(m.GetMetadata())
}

// ValidateTask reports the first problem with a task that would make it
// impossible to run correctly. It is called by the API server before saving.
func ValidateTask(t *Task) error {
	if err := ValidateObjectMeta(t.GetMetadata()); err != nil {
		return err
	}
	if name := t.GetMetadata().GetName(); len(name) > MaxTaskNameLength {
		return fmt.Errorf("metadata.name %q is too long: a task name is at most %d characters, "+
			"so that its actor template name fits a DNS label", name, MaxTaskNameLength)
	}
	spec := t.GetSpec()
	if spec == nil {
		return nil
	}
	refs := spec.WorkspaceRefs()
	paths := spec.WorkspacePaths()
	names := make(map[string]bool, len(refs))
	seen := make(map[string]string, len(refs))
	for i, r := range refs {
		field := fmt.Sprintf("spec.workspaces[%d]", i)
		if r.GetName() == "" {
			return fmt.Errorf("%s: name is required", field)
		}
		if names[r.GetName()] {
			return fmt.Errorf("%s: workspace %q is bound more than once", field, r.GetName())
		}
		names[r.GetName()] = true
		if r.GetPath() != "" && !strings.HasPrefix(r.GetPath(), "/") {
			return fmt.Errorf("%s: path %q must be absolute", field, r.GetPath())
		}
		p := strings.TrimRight(paths[i], "/")
		if p == "" {
			p = "/"
		}
		if other, dup := seen[p]; dup {
			return fmt.Errorf("%s: path %q is already used by workspace %q", field, paths[i], other)
		}
		seen[p] = r.GetName()
	}
	return nil
}
