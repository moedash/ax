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

package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeTasks stands in for the task workflows. It answers the way a workflow
// does: a created task comes up suspended, resuming gives it a worker address,
// and a deleted task is gone when Delete returns.
type fakeTasks struct {
	mu    sync.Mutex
	tasks map[string]*v1alpha1.Task
	// created records the desired state the server resolved, which is what the
	// workflow would be started with.
	created []*workflows.TaskDesiredState
	// ready, when set, marks a task Ready as soon as it is resumed.
	ready bool
	// unavailableGets is how many Gets in a row answer as if no worker were
	// there, which is what a server restart looks like to a watch.
	unavailableGets int
	// changePending makes Suspend and Resume answer as if the worker took the
	// change and is still carrying it out.
	changePending bool
	// lastLimit records the limit the server asked a listing for.
	lastLimit int64
	// deleted records the tasks whose Delete ran to the end.
	deleted []string
}

func newFakeTasks() *fakeTasks {
	return &fakeTasks{tasks: make(map[string]*v1alpha1.Task)}
}

func fakeKey(atespace, name string) string {
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return fmt.Sprintf("%s/%s", atespace, name)
}

func (f *fakeTasks) Create(
	ctx context.Context,
	desired *workflows.TaskDesiredState,
) (*v1alpha1.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := fakeKey(desired.Task.GetMetadata().GetAtespace(), desired.Task.GetMetadata().GetName())
	if _, exists := f.tasks[key]; exists {
		return nil, orchestration.ErrTaskExists
	}
	task, ok := proto.Clone(desired.Task).(*v1alpha1.Task)
	if !ok {
		return nil, fmt.Errorf("cloning task")
	}
	task.Status = &v1alpha1.TaskStatus{
		Phase: v1alpha1.PhaseSuspended,
		Actor: task.GetMetadata().GetName(),
	}
	f.created = append(f.created, desired)
	f.tasks[key] = task
	return proto.Clone(task).(*v1alpha1.Task), nil
}

func (f *fakeTasks) Get(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailableGets > 0 {
		f.unavailableGets--
		return nil, orchestration.ErrTaskUnavailable
	}
	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return nil, orchestration.ErrTaskNotFound
	}
	return proto.Clone(task).(*v1alpha1.Task), nil
}

// workerAway makes the next n Gets answer as if no worker were there.
func (f *fakeTasks) workerAway(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailableGets = n
}

func (f *fakeTasks) List(
	ctx context.Context,
	atespace string,
	limit, offset int64,
) ([]*v1alpha1.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLimit = limit

	keys := make([]string, 0, len(f.tasks))
	for k := range f.tasks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := []*v1alpha1.Task{}
	for _, k := range keys {
		task := f.tasks[k]
		if atespace != "" && atespace != "*" && task.GetMetadata().GetAtespace() != atespace {
			continue
		}
		out = append(out, proto.Clone(task).(*v1alpha1.Task))
	}
	if offset >= int64(len(out)) {
		return []*v1alpha1.Task{}, nil
	}
	out = out[offset:]
	if limit > 0 && int64(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeTasks) Suspend(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	return f.setSuspend(atespace, name, true)
}

func (f *fakeTasks) Resume(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	return f.setSuspend(atespace, name, false)
}

func (f *fakeTasks) setSuspend(atespace, name string, suspend bool) (*v1alpha1.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.changePending {
		return nil, fmt.Errorf("%w: %s", orchestration.ErrTaskChangePending,
			fakeKey(atespace, name))
	}
	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return nil, orchestration.ErrTaskNotFound
	}
	if suspend {
		task.Status.Phase = v1alpha1.PhaseSuspended
		task.Status.WorkerIp = ""
		task.Status.Conditions = nil
		return proto.Clone(task).(*v1alpha1.Task), nil
	}
	task.Status.Phase = v1alpha1.PhaseRunning
	task.Status.WorkerIp = "10.244.1.42"
	if f.ready {
		task.Status.Conditions = readyConditions()
	}
	return proto.Clone(task).(*v1alpha1.Task), nil
}

func (f *fakeTasks) Delete(ctx context.Context, atespace, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fakeKey(atespace, name)
	if _, ok := f.tasks[key]; !ok {
		return orchestration.ErrTaskNotFound
	}
	delete(f.tasks, key)
	f.deleted = append(f.deleted, key)
	return nil
}

// markReady flips a task to ready, which is what a watch waits for.
func (f *fakeTasks) markReady(atespace, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return
	}
	task.Status.Conditions = readyConditions()
}

func readyConditions() []*v1alpha1.Condition {
	return []*v1alpha1.Condition{{
		Type:   v1alpha1.ConditionReady,
		Status: v1alpha1.ConditionTrue,
		Reason: "TaskRunning",
	}}
}

func (f *fakeTasks) lastCreated() *workflows.TaskDesiredState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) == 0 {
		return nil
	}
	return f.created[len(f.created)-1]
}

// serveTasks serves a Temporal-path server over a listener and returns a client
// for it. The server and the connection are closed with the test.
func serveTasks(t *testing.T, tasks *fakeTasks, opts server.Options) v1alpha1.AXClient {
	t.Helper()
	opts.Tasks = tasks
	srv := server.NewServer(memory.NewStore(), opts)
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
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return v1alpha1.NewAXClient(conn)
}

// With a Tasks client the task RPCs go to the workflows and answer with the
// same codes the direct path does, so a client cannot tell the two apart.
func TestTemporalPathTaskLifecycle(t *testing.T) {
	tasks := newFakeTasks()
	tasks.ready = true
	client := serveTasks(t, tasks, server.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	created, err := client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}})
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}
	if created.GetStatus().GetPhase() != v1alpha1.PhaseSuspended {
		t.Errorf("expected a created task to be Suspended, got %q", created.GetStatus().GetPhase())
	}
	if created.GetMetadata().GetAtespace() != "default" ||
		created.GetMetadata().GetCreationTimestamp() == nil {
		t.Errorf("expected the metadata to be defaulted, got %v", created.GetMetadata())
	}

	// Tasks are immutable, whichever path runs them.
	_, err = client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition for a second create, got %v", err)
	}

	resumed, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "job"})
	if err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}
	if resumed.GetStatus().GetPhase() != v1alpha1.PhaseRunning {
		t.Errorf("expected Running after resume, got %q", resumed.GetStatus().GetPhase())
	}
	suspended, err := client.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "job"})
	if err != nil {
		t.Fatalf("SuspendTask failed: %v", err)
	}
	if suspended.GetStatus().GetPhase() != v1alpha1.PhaseSuspended {
		t.Errorf("expected Suspended after suspend, got %q", suspended.GetStatus().GetPhase())
	}

	listed, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(listed.GetTasks()) != 1 || listed.GetTasks()[0].GetMetadata().GetName() != "job" {
		t.Errorf("expected the task to be listed, got %v", listed.GetTasks())
	}

	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "job"}); err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}
	if _, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "job"}); status.Code(
		err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "job"}); status.Code(
		err) != codes.NotFound {
		t.Errorf("expected NotFound for a second delete, got %v", err)
	}
}

// The API server resolves the workspaces a task binds and hands them to the
// task's workflow, so the worker never reads the configuration store.
func TestTemporalPathResolvesBindings(t *testing.T) {
	memStore := memory.NewStore()
	tasks := newFakeTasks()
	srv := server.NewServer(memStore, server.Options{Tasks: tasks})
	ctx := context.Background()

	if err := memStore.SaveWorkspace(ctx, &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "repo", Atespace: "default"},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}); err != nil {
		t.Fatalf("SaveWorkspace failed: %v", err)
	}

	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "bound"},
		Spec: &v1alpha1.TaskSpec{
			// The second binding has no Workspace resource; the runner treats it as
			// an empty directory, so the task is still created.
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "repo"}, {Name: "missing"}},
		},
	}}); err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	created := tasks.lastCreated()
	if created == nil {
		t.Fatal("expected the task to be created")
	}
	if len(created.Workspaces) != 1 || created.Workspaces[0].GetMetadata().GetName() != "repo" {
		t.Errorf("expected one resolved workspace, got %v", created.Workspaces)
	}
	if created.Task.GetMetadata().GetAtespace() != "default" {
		t.Errorf("expected the atespace to be defaulted, got %q",
			created.Task.GetMetadata().GetAtespace())
	}

	// The store is not where tasks live on this path.
	if _, err := memStore.GetTask(ctx, "default", "bound"); err == nil {
		t.Error("expected the task to stay out of the store")
	}
}

// WatchTask asks the task for its state on an interval and emits what changed,
// until the task can do work.
func TestTemporalPathWatchStreamsUntilReady(t *testing.T) {
	tasks := newFakeTasks()
	client := serveTasks(t, tasks, server.Options{WatchPollInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "watched"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}
	if _, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "watched"}); err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}

	stream, err := client.WatchTask(ctx,
		&v1alpha1.WatchTaskRequest{Atespace: "default", Name: "watched"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchTask Recv failed: %v", err)
	}
	if initial.GetAction() != "INITIAL" {
		t.Errorf("expected INITIAL, got %q", initial.GetAction())
	}

	tasks.markReady("default", "watched")
	modified, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchTask Recv failed: %v", err)
	}
	if modified.GetAction() != "MODIFIED" {
		t.Errorf("expected MODIFIED, got %q", modified.GetAction())
	}

	// A ready task ends the watch.
	if _, err := stream.Recv(); err != io.EOF {
		t.Errorf("expected the stream to end once the task is ready, got %v", err)
	}
}

// A worker restart makes a task unanswerable for a few seconds. A watch waits
// that out rather than ending on the first unanswered poll, and gives up only
// when nothing answers for much longer than a restart takes.
func TestTemporalPathWatchOutlastsAWorkerRestart(t *testing.T) {
	tasks := newFakeTasks()
	tasks.ready = true
	client := serveTasks(t, tasks, server.Options{WatchPollInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "watched"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}
	if _, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "watched"}); err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}

	tasks.workerAway(3)
	stream, err := client.WatchTask(ctx,
		&v1alpha1.WatchTaskRequest{Atespace: "default", Name: "watched"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected the watch to wait for the worker, got %v", err)
	}
	if initial.GetAction() != "INITIAL" {
		t.Errorf("expected INITIAL once the worker answered, got %q", initial.GetAction())
	}

	tasks.workerAway(1000)
	stream, err = client.WatchTask(ctx,
		&v1alpha1.WatchTaskRequest{Atespace: "default", Name: "watched"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("expected a watch nobody answers to give up as Unavailable, got %v", err)
	}
}

// A name or atespace becomes a workflow ID on every task call, not only on the
// create, so every path checks it before building one.
func TestTemporalPathRejectsNamesThatAreNotDNSLabels(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), server.Options{Tasks: newFakeTasks()})
	ctx := context.Background()
	const bad = "job' OR '1'='1"

	calls := map[string]func() error{
		"GetTask": func() error {
			_, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: bad})
			return err
		},
		"GetTask atespace": func() error {
			_, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: bad, Name: "job"})
			return err
		},
		"DeleteTask": func() error {
			_, err := srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: bad})
			return err
		},
		"SuspendTask": func() error {
			_, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: bad})
			return err
		},
		"ResumeTask": func() error {
			_, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: bad})
			return err
		},
		"ListTasks": func() error {
			_, err := srv.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: bad})
			return err
		},
	}
	for name, call := range calls {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument for a name with a quote, got %v", name, err)
		}
	}
}

// A listing walks visibility on the caller's behalf, so the page it asks for
// is capped whatever the caller asked.
func TestTemporalPathCapsTheListLimit(t *testing.T) {
	tasks := newFakeTasks()
	srv := server.NewServer(memory.NewStore(), server.Options{Tasks: tasks})

	req := &v1alpha1.ListTasksRequest{Limit: 1_000_000}
	if _, err := srv.ListTasks(context.Background(), req); err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if tasks.lastLimit <= 0 || tasks.lastLimit >= 1_000_000 {
		t.Errorf("expected the limit to be capped, got %d", tasks.lastLimit)
	}
}

// Every orchestration error has one status code, shared with the direct path
// where the situation exists there.
func TestTemporalPathErrorCodes(t *testing.T) {
	tasks := newFakeTasks()
	srv := server.NewServer(memory.NewStore(), server.Options{Tasks: tasks})
	ctx := context.Background()

	if _, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "missing"}); status.Code(
		err) != codes.NotFound {
		t.Errorf("expected NotFound for a missing task, got %v", err)
	}
	if _, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "missing"}); status.Code(
		err) != codes.NotFound {
		t.Errorf("expected NotFound when suspending a missing task, got %v", err)
	}

	tasks.workerAway(1)
	if _, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "missing"}); status.Code(
		err) != codes.Unavailable {
		t.Errorf("expected Unavailable when no worker answers, got %v", err)
	}

	tasks.changePending = true
	if _, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "busy"}); status.Code(
		err) != codes.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded for a change still running, got %v", err)
	}
}
