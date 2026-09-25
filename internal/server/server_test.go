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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServerHealthzHTTP(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), newFakeTasks(), server.Options{})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", rec.Code)
	}
	if rec.Body.String() != "ok\n" {
		t.Fatalf("expected 'ok\\n' from /healthz, got %q", rec.Body.String())
	}

	// Verify non-healthz HTTP returns 404
	req404 := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec404 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for /api/v1/tasks, got %d", rec404.Code)
	}
}

func TestServerGRPC(t *testing.T) {
	memStore := memory.NewStore()
	tasks := newFakeTasks()
	tasks.ready = true
	srv := server.NewServer(memStore, tasks, server.Options{WatchPollInterval: 10 * time.Millisecond})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	httpServer := &http.Server{
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	go func() {
		_ = httpServer.Serve(ln)
	}()
	defer httpServer.Close()

	conn, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := v1alpha1.NewAXClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create one resource of each kind through the typed RPCs. Metadata is left
	// partially empty to exercise server-side defaulting.
	if _, err := client.UpdateGateway(ctx, &v1alpha1.UpdateGatewayRequest{Gateway: &v1alpha1.Gateway{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-gw"},
		Spec: &v1alpha1.GatewaySpec{
			Listeners: []*v1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
		},
	}}); err != nil {
		t.Fatalf("UpdateGateway failed: %v", err)
	}
	if _, err := client.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{Workspace: &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-ws"},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}}); err != nil {
		t.Fatalf("UpdateWorkspace failed: %v", err)
	}
	if _, err := client.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-model"},
		Spec:     &v1alpha1.ModelSpec{Provider: "google", Model: "gemini-3.8-flash"},
	}}); err != nil {
		t.Fatalf("UpdateModel failed: %v", err)
	}
	if _, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-task"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	// 2. Defaulting applies to every kind: atespace and creation timestamp are filled in.
	gw, err := client.GetGateway(ctx, &v1alpha1.GetGatewayRequest{Atespace: "default", Name: "grpc-gw"})
	if err != nil {
		t.Fatalf("GetGateway failed: %v", err)
	}
	if gw.GetMetadata().GetAtespace() != "default" || gw.GetMetadata().GetCreationTimestamp() == nil {
		t.Errorf("expected gateway metadata to be defaulted, got %v", gw.GetMetadata())
	}

	// 3. GetTask & ListTasks
	task, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if task.Metadata.Name != "grpc-task" {
		t.Errorf("expected name 'grpc-task', got %s", task.Metadata.Name)
	}
	if task.Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp to be populated on applied task")
	}

	listTasksResp, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(listTasksResp.Tasks) != 1 {
		t.Fatalf("expected 1 task in list, got %d", len(listTasksResp.Tasks))
	}
	if listTasksResp.Tasks[0].Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp on listed task")
	}

	// Test UpdateTask
	task.Spec.Image = "ghcr.io/test/updated-image"
	updatedTask, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: task})
	if err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}
	if updatedTask.Spec.Image != "ghcr.io/test/updated-image" {
		t.Errorf("expected image 'ghcr.io/test/updated-image', got %s", updatedTask.Spec.Image)
	}

	// 4. Suspend & Resume Task
	suspTask, err := client.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("SuspendTask failed: %v", err)
	}
	if !suspTask.Spec.Suspend {
		t.Errorf("expected task to be suspended")
	}

	resTask, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}
	if resTask.Spec.Suspend {
		t.Errorf("expected task to be resumed")
	}

	// 5. Gateways
	gw, err = client.GetGateway(ctx, &v1alpha1.GetGatewayRequest{Atespace: "default", Name: "grpc-gw"})
	if err != nil {
		t.Fatalf("GetGateway failed: %v", err)
	}
	if gw.Metadata.Name != "grpc-gw" {
		t.Errorf("expected gateway 'grpc-gw', got %s", gw.Metadata.Name)
	}

	listGatewaysResp, err := client.ListGateways(ctx, &v1alpha1.ListGatewaysRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListGateways failed: %v", err)
	}
	if len(listGatewaysResp.Gateways) != 1 {
		t.Fatalf("expected 1 gateway in list, got %d", len(listGatewaysResp.Gateways))
	}

	// 6. Workspaces
	ws, err := client.GetWorkspace(ctx, &v1alpha1.GetWorkspaceRequest{Atespace: "default", Name: "grpc-ws"})
	if err != nil {
		t.Fatalf("GetWorkspace failed: %v", err)
	}
	if ws.Metadata.Name != "grpc-ws" {
		t.Errorf("expected workspace 'grpc-ws', got %s", ws.Metadata.Name)
	}

	listWorkspacesResp, err := client.ListWorkspaces(ctx, &v1alpha1.ListWorkspacesRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}
	if len(listWorkspacesResp.Workspaces) != 1 {
		t.Fatalf("expected 1 workspace in list, got %d", len(listWorkspacesResp.Workspaces))
	}

	// 7. Models
	model, err := client.GetModel(ctx, &v1alpha1.GetModelRequest{Atespace: "default", Name: "grpc-model"})
	if err != nil {
		t.Fatalf("GetModel failed: %v", err)
	}
	if model.Metadata.Name != "grpc-model" {
		t.Errorf("expected model 'grpc-model', got %s", model.Metadata.Name)
	}

	listModelsResp, err := client.ListModels(ctx, &v1alpha1.ListModelsRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(listModelsResp.Models) != 1 {
		t.Fatalf("expected 1 model in list, got %d", len(listModelsResp.Models))
	}

	// 7b. WatchTask (should receive INITIAL state)
	stream, err := client.WatchTask(ctx, &v1alpha1.WatchTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	watchMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchTask Recv failed: %v", err)
	}
	if watchMsg.Action != "INITIAL" {
		t.Errorf("expected INITIAL action, got %s", watchMsg.Action)
	}
	if watchMsg.Task == nil || watchMsg.Task.Metadata == nil || watchMsg.Task.Metadata.Name != "grpc-task" {
		t.Errorf("expected task 'grpc-task' in watch, got %+v", watchMsg.Task)
	}

	// 8. Delete operations
	// Deleting a task is asynchronous: the RPC is accepted, the task reports
	// Terminating while its sandbox is torn down, and the record disappears once
	// that has finished.
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "grpc-task"}); err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}
	terminating, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("GetTask after DeleteTask failed: %v", err)
	}
	if terminating.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Errorf("expected phase %q after DeleteTask, got %q", v1alpha1.PhaseTerminating, terminating.GetStatus().GetPhase())
	}
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "no-such-task"}); status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound deleting a missing task, got %v", err)
	}
	// Stand in for the task workflow finishing its teardown.
	tasks.finishDelete("default", "grpc-task")
	if _, err := client.DeleteGateway(ctx, &v1alpha1.DeleteGatewayRequest{Atespace: "default", Name: "grpc-gw"}); err != nil {
		t.Fatalf("DeleteGateway failed: %v", err)
	}
	if _, err := client.DeleteWorkspace(ctx, &v1alpha1.DeleteWorkspaceRequest{Atespace: "default", Name: "grpc-ws"}); err != nil {
		t.Fatalf("DeleteWorkspace failed: %v", err)
	}
	if _, err := client.DeleteModel(ctx, &v1alpha1.DeleteModelRequest{Atespace: "default", Name: "grpc-model"}); err != nil {
		t.Fatalf("DeleteModel failed: %v", err)
	}

	// Check 404 after delete
	_, err = client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound code, got %v", err)
	}
}

func TestUpdateTask_ValidatesWorkspaceBindings(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), newFakeTasks(), server.Options{})
	ctx := context.Background()

	_, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "bad"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a", Path: "/same"}, {Name: "b", Path: "/same"}},
		},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for colliding workspace paths, got %v", err)
	}

	_, err = srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "good"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a"}, {Name: "b"}},
		},
	}})
	if err != nil {
		t.Fatalf("expected a valid multi-workspace task to be accepted, got %v", err)
	}
}

// The API server resolves what a task binds and hands it to the task's
// workflow, so the worker never reads the configuration store.
func TestUpdateTaskResolvesBindings(t *testing.T) {
	memStore := memory.NewStore()
	tasks := newFakeTasks()
	srv := server.NewServer(memStore, tasks, server.Options{})
	ctx := context.Background()

	if err := memStore.SaveGateway(ctx, &v1alpha1.Gateway{
		Metadata: &v1alpha1.ObjectMeta{Name: "gw", Atespace: "default"},
		Spec: &v1alpha1.GatewaySpec{
			Egress: &v1alpha1.EgressConfig{
				Allowlist: &v1alpha1.EgressAllowlist{
					Hosts: []*v1alpha1.HostRule{{Host: "github.com", Port: 443}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("SaveGateway failed: %v", err)
	}
	if err := memStore.SaveWorkspace(ctx, &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "repo", Atespace: "default"},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}); err != nil {
		t.Fatalf("SaveWorkspace failed: %v", err)
	}

	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "bound"},
		Spec: &v1alpha1.TaskSpec{
			Gateway: &v1alpha1.GatewayRef{Name: "gw"},
			// The second binding has no Workspace resource; the runner treats it as
			// an empty directory, so the task is still applied.
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "repo"}, {Name: "missing"}},
		},
	}}); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	applied := tasks.lastApplied()
	if applied == nil {
		t.Fatal("expected the task to be applied")
	}
	if applied.Gateway.GetMetadata().GetName() != "gw" {
		t.Errorf("expected the gateway to be resolved, got %v", applied.Gateway)
	}
	if len(applied.Workspaces) != 1 || applied.Workspaces[0].GetMetadata().GetName() != "repo" {
		t.Errorf("expected one resolved workspace, got %v", applied.Workspaces)
	}
	if applied.Task.GetMetadata().GetAtespace() != "default" {
		t.Errorf("expected the atespace to be defaulted, got %q",
			applied.Task.GetMetadata().GetAtespace())
	}
}

// WatchTask asks the task for its state on an interval and emits what changed,
// until the task can do work.
func TestWatchTaskStreamsUntilReady(t *testing.T) {
	memStore := memory.NewStore()
	tasks := newFakeTasks()
	srv := server.NewServer(memStore, tasks, server.Options{WatchPollInterval: 10 * time.Millisecond})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	httpServer := &http.Server{Handler: srv.Handler()}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = httpServer.Serve(ln) }()
	defer httpServer.Close()

	conn, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := v1alpha1.NewAXClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "watched"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
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

// serveGRPC serves the API over a listener and returns a client for it. The
// server and the connection are closed with the test.
func serveGRPC(t *testing.T, srv *server.Server) v1alpha1.AXClient {
	t.Helper()
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

// A worker restart makes a task unanswerable for a few seconds. A watch waits
// that out rather than ending on the first unanswered poll, and gives up only
// when nothing answers for much longer than a restart takes.
func TestWatchTaskOutlastsAWorkerRestart(t *testing.T) {
	tasks := newFakeTasks()
	tasks.ready = true
	client := serveGRPC(t, server.NewServer(memory.NewStore(), tasks,
		server.Options{WatchPollInterval: 10 * time.Millisecond}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "watched"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
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
// write, so every path checks it before building one.
func TestTaskReadsRejectNamesThatAreNotDNSLabels(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), newFakeTasks(), server.Options{})
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
func TestListTasksCapsTheLimit(t *testing.T) {
	tasks := newFakeTasks()
	srv := server.NewServer(memory.NewStore(), tasks, server.Options{})

	req := &v1alpha1.ListTasksRequest{Limit: 1_000_000}
	if _, err := srv.ListTasks(context.Background(), req); err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if tasks.lastLimit <= 0 || tasks.lastLimit >= 1_000_000 {
		t.Errorf("expected the limit to be capped, got %d", tasks.lastLimit)
	}
}

// A name or atespace ends up in Substrate resource names, in HTTP headers, and
// in the queries the control plane builds, so the API rejects anything that is
// not a DNS label, for every kind alike.
func TestUpdateRejectsNamesThatAreNotDNSLabels(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), newFakeTasks(), server.Options{})
	ctx := context.Background()

	const bad = "job' OR '1'='1"

	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: bad},
		Spec:     &v1alpha1.TaskSpec{},
	}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for a task name with a quote, got %v", err)
	}
	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: bad},
		Spec:     &v1alpha1.TaskSpec{},
	}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for an atespace with a quote, got %v", err)
	}
	if _, err := srv.UpdateGateway(ctx, &v1alpha1.UpdateGatewayRequest{Gateway: &v1alpha1.Gateway{
		Metadata: &v1alpha1.ObjectMeta{Name: bad},
		Spec:     &v1alpha1.GatewaySpec{},
	}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for a gateway name with a quote, got %v", err)
	}
	badWorkspace := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: bad},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}
	if _, err := srv.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{
		Workspace: badWorkspace,
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for a workspace name with a quote, got %v", err)
	}
	if _, err := srv.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{
		Metadata: &v1alpha1.ObjectMeta{Name: bad},
		Spec:     &v1alpha1.ModelSpec{},
	}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for a model name with a quote, got %v", err)
	}

	// A name that is a DNS label is still accepted, with the atespace defaulted.
	if _, err := srv.UpdateGateway(ctx, &v1alpha1.UpdateGatewayRequest{Gateway: &v1alpha1.Gateway{
		Metadata: &v1alpha1.ObjectMeta{Name: "gw-1"},
		Spec:     &v1alpha1.GatewaySpec{},
	}}); err != nil {
		t.Errorf("expected a valid gateway to be accepted, got %v", err)
	}
}
