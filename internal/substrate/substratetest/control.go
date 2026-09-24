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

// Package substratetest provides an in-process fake of the Agent Substrate
// Control API. It records the calls a test makes and answers them with the
// minimum a caller needs, so tests can exercise the real substrate client and
// the activities built on it without a cluster.
package substratetest

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ControlServer is a fake Control API. Every field is guarded by a mutex because
// activities call it from their own goroutines while the test inspects it.
type ControlServer struct {
	ateapipb.UnimplementedControlServer

	mu sync.Mutex

	// WorkerIP is reported as the worker assignment of a resumed actor.
	WorkerIP string
	// ResumeErr, when set, fails every ResumeActor call.
	ResumeErr error
	// CreateActorErr, when set, fails every CreateActor call.
	CreateActorErr error

	atespaces  []string
	actors     []string
	actorState map[string]ateapipb.ActorState
	actorTmpl  map[string]*ateapipb.ObjectRef
	resumed    []string
	suspended  []string
	policies   []string
	delActors  []string
	delPolicy  []string
	templates  map[string]bool
	delTmpl    []string
	createTmpl []string
	createdEnv map[string]map[string]string
}

// NewControlServer returns a fake Control API preloaded with the given actor
// template names.
func NewControlServer(templates ...string) *ControlServer {
	tmpl := make(map[string]bool, len(templates))
	for _, t := range templates {
		tmpl[t] = true
	}
	return &ControlServer{
		templates:  tmpl,
		createdEnv: make(map[string]map[string]string),
		actorState: make(map[string]ateapipb.ActorState),
		actorTmpl:  make(map[string]*ateapipb.ObjectRef),
		WorkerIP:   "10.244.1.42",
	}
}

// Start serves the fake on a loopback port and returns a substrate client bound
// to it together with a stop function that releases both.
func Start(s *ControlServer) (*substrate.Client, func(), error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, s)
	go func() { _ = grpcServer.Serve(lis) }()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		return nil, nil, err
	}
	stop := func() {
		_ = client.Close()
		grpcServer.Stop()
	}
	return client, stop, nil
}

// Recorded call logs. Each returns a copy.

func (s *ControlServer) Atespaces() []string        { return s.snapshot(&s.atespaces) }
func (s *ControlServer) CreatedActors() []string    { return s.snapshot(&s.actors) }
func (s *ControlServer) ResumedActors() []string    { return s.snapshot(&s.resumed) }
func (s *ControlServer) SuspendedActors() []string  { return s.snapshot(&s.suspended) }
func (s *ControlServer) CreatedPolicies() []string  { return s.snapshot(&s.policies) }
func (s *ControlServer) DeletedActors() []string    { return s.snapshot(&s.delActors) }
func (s *ControlServer) DeletedPolicies() []string  { return s.snapshot(&s.delPolicy) }
func (s *ControlServer) DeletedTemplates() []string { return s.snapshot(&s.delTmpl) }
func (s *ControlServer) CreatedTemplates() []string { return s.snapshot(&s.createTmpl) }

// Templates returns the names of the templates the fake currently holds.
func (s *ControlServer) Templates() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.templates))
	for name := range s.templates {
		out = append(out, name)
	}
	return out
}

func (s *ControlServer) snapshot(list *[]string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), *list...)
}

func (s *ControlServer) record(list *[]string, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*list = append(*list, name)
}

// Control API implementation.

func (s *ControlServer) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	name := req.GetAtespace().GetMetadata().GetName()
	s.record(&s.atespaces, name)
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func (s *ControlServer) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	failure := s.CreateActorErr
	s.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	name := req.GetActor().GetMetadata().GetName()
	s.mu.Lock()
	if _, exists := s.actorState[name]; exists {
		s.mu.Unlock()
		// Substrate binds an actor to the template it was created from, so a
		// second create is rejected rather than rebinding the actor.
		return nil, status.Errorf(codes.AlreadyExists, "actor %q already exists", name)
	}
	template := req.GetActor().GetActorTemplate()
	s.actors = append(s.actors, name)
	s.actorState[name] = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	s.actorTmpl[name] = template
	s.mu.Unlock()
	return &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name},
		ActorTemplate: template,
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}, nil
}

func (s *ControlServer) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	s.mu.Lock()
	failure, workerIP := s.ResumeErr, s.WorkerIP
	s.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	name := req.GetActor().GetName()
	s.mu.Lock()
	s.resumed = append(s.resumed, name)
	s.actorState[name] = ateapipb.ActorState_ACTOR_STATE_RUNNING
	s.mu.Unlock()
	return &ateapipb.ResumeActorResponse{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerPod:   "worker-pod-1",
					WorkerPodIp: workerIP,
				},
			},
		},
		Resumed: true,
	}, nil
}

func (s *ControlServer) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	name := req.GetActor().GetName()
	s.mu.Lock()
	s.suspended = append(s.suspended, name)
	s.actorState[name] = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	s.mu.Unlock()
	return &ateapipb.SuspendActorResponse{}, nil
}

// SetActorState puts an actor into a state a test wants to observe, for example
// a crashed sandbox.
func (s *ControlServer) SetActorState(name string, state ateapipb.ActorState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actorState[name] = state
}

func (s *ControlServer) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetName()
	s.mu.Lock()
	state, ok := s.actorState[name]
	template := s.actorTmpl[name]
	workerIP := s.WorkerIP
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", name)
	}
	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetActor().GetAtespace()},
		ActorTemplate: template,
		Status:        &ateapipb.ActorStatus{State: state},
	}
	if state == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		actor.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
			WorkerPod:   "worker-pod-1",
			WorkerPodIp: workerIP,
		}
	}
	return actor, nil
}

func (s *ControlServer) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.record(&s.policies, req.GetActor().GetName())
	return &ateapipb.EgressPolicy{}, nil
}

func (s *ControlServer) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.record(&s.delPolicy, req.GetActor().GetName())
	return &ateapipb.EgressPolicy{}, nil
}

func (s *ControlServer) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetName()
	s.mu.Lock()
	s.delActors = append(s.delActors, name)
	delete(s.actorState, name)
	delete(s.actorTmpl, name)
	s.mu.Unlock()
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func (s *ControlServer) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	name := req.GetActorTemplate().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.templates[name] {
		return nil, status.Errorf(codes.NotFound, "actor template %q not found", name)
	}
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetActorTemplate().GetAtespace()},
	}, nil
}

func (s *ControlServer) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	tmpl := req.GetActorTemplate()
	name := tmpl.GetMetadata().GetName()
	env := make(map[string]string)
	for _, container := range tmpl.GetContainers() {
		for _, e := range container.GetEnv() {
			env[e.GetName()] = e.GetValue()
		}
	}
	s.mu.Lock()
	s.templates[name] = true
	s.createTmpl = append(s.createTmpl, name)
	s.createdEnv[name] = env
	s.mu.Unlock()
	return tmpl, nil
}

func (s *ControlServer) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	resp := &ateapipb.ListActorTemplatesResponse{}
	for _, name := range s.Templates() {
		resp.ActorTemplates = append(resp.ActorTemplates, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetAtespace()},
		})
	}
	return resp, nil
}

func (s *ControlServer) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	name := req.GetActorTemplate().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	for actor, template := range s.actorTmpl {
		if template.GetName() != name {
			continue
		}
		// A template an actor still derives from cannot go until the actor does.
		return nil, status.Errorf(codes.FailedPrecondition,
			"actor template %q is still used by actor %q", name, actor)
	}
	delete(s.templates, name)
	s.delTmpl = append(s.delTmpl, name)
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

// TemplateEnv returns the container environment of a template the fake holds,
// as created through CreateActorTemplate.
func (s *ControlServer) TemplateEnv(name string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, ok := s.createdEnv[name]
	if !ok {
		return nil, fmt.Errorf("template %q was never created", name)
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		out[k] = v
	}
	return out, nil
}
