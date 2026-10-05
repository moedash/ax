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
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// createdTasks records what the API server handed to the workflows. A name
// listed in taken answers as a task that already exists.
type createdTasks struct {
	orchestration.Tasks

	mu      sync.Mutex
	created []*workflows.TaskDesiredState
	taken   map[string]bool
}

func (c *createdTasks) Create(
	ctx context.Context, desired *workflows.TaskDesiredState,
) (*v1alpha1.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taken[desired.Task.GetMetadata().GetName()] {
		return nil, orchestration.ErrTaskExists
	}
	c.created = append(c.created, desired)
	return desired.Task, nil
}

// serveAPI runs the API server over a listener and returns a client for it.
func serveAPI(t *testing.T, tasks orchestration.Tasks) v1alpha1.AXClient {
	t.Helper()
	srv := server.NewServer(memory.NewStore(), server.Options{Tasks: tasks})
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

// storedTasks fills a store the way the direct orchestrator leaves it.
func storedTasks(t *testing.T, names ...string) store.Store {
	t.Helper()
	s := memory.NewStore()
	for _, name := range names {
		err := s.SaveTask(context.Background(), &v1alpha1.Task{
			ApiVersion: v1alpha1.APIVersion,
			Kind:       v1alpha1.KindTask,
			Metadata:   &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
			Spec:       &v1alpha1.TaskSpec{Image: "ghcr.io/example/agent"},
			Status:     &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseRunning, WorkerIp: "10.0.0.1"},
		})
		if err != nil {
			t.Fatalf("SaveTask failed: %v", err)
		}
	}
	return s
}

func stillStored(t *testing.T, s store.Store, name string) bool {
	t.Helper()
	_, err := s.GetTask(context.Background(), "default", name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTask failed: %v", err)
	}
	return err == nil
}

// Every task the store has is created through the API server, with the stored
// status dropped, and leaves the store once the server has it.
func TestMigrateImportsAndRemovesTasks(t *testing.T) {
	source := storedTasks(t, "job", "other")
	tasks := &createdTasks{}
	api := serveAPI(t, tasks)

	if err := migrate(context.Background(), source, api, false, 5*time.Second); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	tasks.mu.Lock()
	defer tasks.mu.Unlock()
	if len(tasks.created) != 2 {
		t.Fatalf("expected both tasks to be created, got %d", len(tasks.created))
	}
	for _, desired := range tasks.created {
		if desired.Task.GetStatus() != nil {
			t.Errorf("expected the stored status to be dropped, got %v", desired.Task.GetStatus())
		}
	}
	for _, name := range []string{"job", "other"} {
		if stillStored(t, source, name) {
			t.Errorf("expected %s to leave the store once imported", name)
		}
	}
}

// A task the server refuses stays where it was and is named, so a rerun can
// pick it up once it is fixed.
func TestMigrateKeepsWhatTheServerRefuses(t *testing.T) {
	source := storedTasks(t, "job", strings.Repeat("a", v1alpha1.MaxTaskNameLength+1))
	tooLong := strings.Repeat("a", v1alpha1.MaxTaskNameLength+1)
	api := serveAPI(t, &createdTasks{})

	err := migrate(context.Background(), source, api, false, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "default/"+tooLong) {
		t.Fatalf("expected the refused task to be reported, got %v", err)
	}
	if !stillStored(t, source, tooLong) {
		t.Error("expected the refused task to stay in the store")
	}
	if stillStored(t, source, "job") {
		t.Error("expected the accepted task to be removed")
	}
}

// A task the server already has counts as imported, so a rerun after a failure
// removes the record it could not remove the first time.
func TestMigrateTreatsAnExistingTaskAsImported(t *testing.T) {
	source := storedTasks(t, "job")
	api := serveAPI(t, &createdTasks{taken: map[string]bool{"job": true}})

	if err := migrate(context.Background(), source, api, false, 5*time.Second); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	if stillStored(t, source, "job") {
		t.Error("expected the record of a task the server has to be removed")
	}
}

// With --keep the records stay for a look before they go.
func TestMigrateCanKeepTheRecords(t *testing.T) {
	source := storedTasks(t, "job")
	api := serveAPI(t, &createdTasks{})

	if err := migrate(context.Background(), source, api, true, 5*time.Second); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	if !stillStored(t, source, "job") {
		t.Error("expected the record to be kept")
	}
}
