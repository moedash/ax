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

package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// memorySource is the old task store, held in a map.
type memorySource struct {
	mu    sync.Mutex
	tasks map[string]*v1alpha1.Task
}

func (m *memorySource) List(ctx context.Context) ([]*v1alpha1.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*v1alpha1.Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		out = append(out, proto.Clone(task).(*v1alpha1.Task))
	}
	return out, nil
}

func (m *memorySource) Remove(ctx context.Context, task *v1alpha1.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, task.GetMetadata().GetName())
	return nil
}

// appliedTasks records what the API server handed to the workflows.
type appliedTasks struct {
	orchestration.Tasks

	mu      sync.Mutex
	applied []*workflows.TaskDesiredState
}

func (a *appliedTasks) Apply(
	ctx context.Context, desired *workflows.TaskDesiredState,
) (*v1alpha1.Task, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applied = append(a.applied, desired)
	return desired.Task, nil
}

// serveAPI runs the API server over a listener and returns a client for it.
func serveAPI(t *testing.T, tasks orchestration.Tasks) v1alpha1.AXClient {
	t.Helper()
	srv := server.NewServer(memory.NewStore(), tasks, server.Options{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	httpServer := &http.Server{Handler: srv.Handler()}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = httpServer.Serve(ln) }()
	t.Cleanup(func() { _ = httpServer.Close() })

	conn, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return v1alpha1.NewAXClient(conn)
}

func oldTask(name string) *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec:       &v1alpha1.TaskSpec{Image: "ghcr.io/example/agent", Suspend: name == "paused"},
		Status:     &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning, WorkerIp: "10.0.0.1"},
	}
}

// Every task the old store has is applied through the API server, with the
// old status dropped, and leaves the store once the server has it.
func TestMigrateImportsAndRemovesTasks(t *testing.T) {
	source := &memorySource{tasks: map[string]*v1alpha1.Task{
		"job":    oldTask("job"),
		"paused": oldTask("paused"),
	}}
	tasks := &appliedTasks{}
	api := serveAPI(t, tasks)

	if err := migrate(context.Background(), source, api, false, 5*time.Second); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	tasks.mu.Lock()
	defer tasks.mu.Unlock()
	if len(tasks.applied) != 2 {
		t.Fatalf("expected both tasks to be applied, got %d", len(tasks.applied))
	}
	for _, desired := range tasks.applied {
		if desired.Task.GetStatus() != nil {
			t.Errorf("expected the old status to be dropped, got %v", desired.Task.GetStatus())
		}
		if desired.Task.GetMetadata().GetName() == "paused" && !desired.Task.GetSpec().GetSuspend() {
			t.Error("expected a suspended task to stay suspended")
		}
	}
	if len(source.tasks) != 0 {
		t.Errorf("expected the imported tasks to leave the old store, got %v", source.tasks)
	}
}

// A task the server refuses stays where it was and is named, so a rerun can
// pick it up once it is fixed.
func TestMigrateKeepsWhatTheServerRefuses(t *testing.T) {
	source := &memorySource{tasks: map[string]*v1alpha1.Task{
		"job":              oldTask("job"),
		"Not A DNS Label!": oldTask("Not A DNS Label!"),
	}}
	api := serveAPI(t, &appliedTasks{})

	err := migrate(context.Background(), source, api, false, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "default/Not A DNS Label!") {
		t.Fatalf("expected the refused task to be reported, got %v", err)
	}
	if _, kept := source.tasks["Not A DNS Label!"]; !kept {
		t.Error("expected the refused task to stay in the old store")
	}
	if _, kept := source.tasks["job"]; kept {
		t.Error("expected the accepted task to be removed")
	}
}

// With --keep the old records stay for a look before they go.
func TestMigrateCanKeepTheOldRecords(t *testing.T) {
	source := &memorySource{tasks: map[string]*v1alpha1.Task{"job": oldTask("job")}}
	api := serveAPI(t, &appliedTasks{})

	if err := migrate(context.Background(), source, api, true, 5*time.Second); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	if _, kept := source.tasks["job"]; !kept {
		t.Error("expected the old record to be kept")
	}
}
