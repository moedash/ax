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

package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// defaultWatchPollInterval is how often WatchTask asks a task for its state.
const defaultWatchPollInterval = time.Second

// Options configures the API server.
type Options struct {
	// WatchPollInterval is how often WatchTask asks a task for its state. Zero
	// uses defaultWatchPollInterval.
	WatchPollInterval time.Duration
}

// Server provides the gRPC API for AX. Tasks live in their workflows and the
// configuration kinds live in the store, so the server reads each from where it
// belongs.
type Server struct {
	v1alpha1.UnimplementedAXServer
	store             store.Store
	tasks             orchestration.Tasks
	grpcServer        *grpc.Server
	watchPollInterval time.Duration
}

// NewServer creates a new AX API server.
func NewServer(s store.Store, tasks orchestration.Tasks, opts Options) *Server {
	srv := &Server{
		store:             s,
		tasks:             tasks,
		grpcServer:        grpc.NewServer(),
		watchPollInterval: opts.WatchPollInterval,
	}
	if srv.watchPollInterval <= 0 {
		srv.watchPollInterval = defaultWatchPollInterval
	}
	v1alpha1.RegisterAXServer(srv.grpcServer, srv)
	return srv
}

// GRPCServer returns the underlying gRPC server.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Handler returns the HTTP handler for the server, routing gRPC and HTTP health checks.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			s.grpcServer.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.NotFound(w, r)
	})
}

// --- gRPC AXServer implementation ---

// --- Tasks ---

func (s *Server) GetTask(ctx context.Context, req *v1alpha1.GetTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := atespaceOf(req.Atespace)
	task, err := s.tasks.Get(ctx, atespace, req.Name)
	if err != nil {
		return nil, taskError(err, atespace, req.Name)
	}
	return task, nil
}

func (s *Server) ListTasks(ctx context.Context, req *v1alpha1.ListTasksRequest) (*v1alpha1.ListTasksResponse, error) {
	atespace := ""
	limit := int64(50)
	offset := int64(0)
	if req != nil {
		atespace = req.Atespace
		if req.Limit > 0 {
			limit = req.Limit
		}
		if req.Offset > 0 {
			offset = req.Offset
		}
	}
	tasks, err := s.tasks.List(ctx, atespace, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tasks: %v", err)
	}
	return &v1alpha1.ListTasksResponse{Tasks: tasks}, nil
}

// UpdateTask creates or updates a task. The configuration the task binds is
// resolved here and handed to the task's workflow, so the worker that runs it
// never reads the configuration store.
func (s *Server) UpdateTask(ctx context.Context, req *v1alpha1.UpdateTaskRequest) (*v1alpha1.Task, error) {
	if req == nil || req.Task == nil {
		return nil, status.Error(codes.InvalidArgument, "task required")
	}
	task := req.Task
	if err := v1alpha1.ValidateTask(task); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// A task that already exists keeps the creation time its workflow recorded,
	// so nothing here has to read the task back first.
	task.Metadata = defaultMetadata(task.Metadata, func(string, string) *v1alpha1.ObjectMeta { return nil })

	desired, err := s.resolveTask(ctx, task)
	if err != nil {
		return nil, err
	}
	applied, err := s.tasks.Apply(ctx, desired)
	if err != nil {
		return nil, taskError(err, task.GetMetadata().GetAtespace(), task.GetMetadata().GetName())
	}
	return applied, nil
}

func (s *Server) DeleteTask(ctx context.Context, req *v1alpha1.DeleteTaskRequest) (*v1alpha1.DeleteTaskResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := atespaceOf(req.Atespace)
	// Deletion is asynchronous: the task moves to Terminating while its sandbox
	// is torn down, and disappears once that has finished. Clients poll GetTask
	// for NotFound.
	if err := s.tasks.Delete(ctx, atespace, req.Name); err != nil {
		return nil, taskError(err, atespace, req.Name)
	}
	return &v1alpha1.DeleteTaskResponse{}, nil
}

func (s *Server) SuspendTask(ctx context.Context, req *v1alpha1.SuspendTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := atespaceOf(req.Atespace)
	task, err := s.tasks.Suspend(ctx, atespace, req.Name)
	if err != nil {
		return nil, taskError(err, atespace, req.Name)
	}
	return task, nil
}

func (s *Server) ResumeTask(ctx context.Context, req *v1alpha1.ResumeTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := atespaceOf(req.Atespace)
	task, err := s.tasks.Resume(ctx, atespace, req.Name)
	if err != nil {
		return nil, taskError(err, atespace, req.Name)
	}
	return task, nil
}

// WatchTask streams a task's state as it changes. It asks the task for its
// state on an interval and emits whatever is different, until the task is ready,
// has finished, has failed, or is gone.
func (s *Server) WatchTask(req *v1alpha1.WatchTaskRequest, stream grpc.ServerStreamingServer[v1alpha1.WatchTaskResponse]) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := atespaceOf(req.Atespace)
	ctx := stream.Context()

	ticker := time.NewTicker(s.watchPollInterval)
	defer ticker.Stop()

	var last *v1alpha1.Task
	action := "INITIAL"
	for {
		task, err := s.tasks.Get(ctx, atespace, req.Name)
		switch {
		case err == nil:
		case errors.Is(err, orchestration.ErrTaskNotFound) && last != nil:
			// The task was deleted while it was being watched.
			return stream.Send(&v1alpha1.WatchTaskResponse{Task: last, Action: "DELETED"})
		default:
			return taskError(err, atespace, req.Name)
		}

		if last == nil || !proto.Equal(last, task) {
			if err := stream.Send(&v1alpha1.WatchTaskResponse{Task: task, Action: action}); err != nil {
				return err
			}
			action = "MODIFIED"
			last = task
		}
		if watchDone(task) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// watchDone reports whether a task has reached a state worth stopping a watch
// at: it can do work, or it never will.
func watchDone(task *v1alpha1.Task) bool {
	switch task.GetStatus().GetPhase() {
	case v1alpha1.PhaseFailed, v1alpha1.PhaseCompleted:
		return true
	case v1alpha1.PhaseRunning:
		for _, c := range task.GetStatus().GetConditions() {
			if c.GetType() == v1alpha1.ConditionReady {
				return c.GetStatus() == v1alpha1.ConditionTrue
			}
		}
	}
	return false
}

// resolveTask looks up the gateway and the workspaces a task binds. A binding
// that does not exist is left out rather than rejected: a task may be applied
// before its configuration is, and the runner treats a missing workspace as an
// empty directory.
func (s *Server) resolveTask(ctx context.Context, task *v1alpha1.Task) (*workflows.TaskDesiredState, error) {
	atespace := task.GetMetadata().GetAtespace()
	desired := &workflows.TaskDesiredState{Task: task}

	if name := task.GetSpec().GetGateway().GetName(); name != "" {
		gw, err := s.store.GetGateway(ctx, atespace, name)
		switch {
		case err == nil:
			desired.Gateway = gw
		case errors.Is(err, store.ErrNotFound):
			slog.Warn("task binds a gateway that does not exist", "task", task.GetMetadata().GetName(), "gateway", name)
		default:
			return nil, status.Errorf(codes.Internal, "reading gateway %q: %v", name, err)
		}
	}

	for _, ref := range task.GetSpec().WorkspaceRefs() {
		if ref.GetName() == "" {
			continue
		}
		ws, err := s.store.GetWorkspace(ctx, atespace, ref.GetName())
		switch {
		case err == nil:
			desired.Workspaces = append(desired.Workspaces, ws)
		case errors.Is(err, store.ErrNotFound):
			slog.Warn("task binds a workspace that does not exist", "task", task.GetMetadata().GetName(), "workspace", ref.GetName())
		default:
			return nil, status.Errorf(codes.Internal, "reading workspace %q: %v", ref.GetName(), err)
		}
	}
	return desired, nil
}

// taskError turns an orchestration error into the status code its caller
// expects.
func taskError(err error, atespace, name string) error {
	switch {
	case errors.Is(err, orchestration.ErrTaskNotFound):
		return status.Errorf(codes.NotFound, "task %q not found in atespace %q", name, atespace)
	case errors.Is(err, orchestration.ErrTaskTerminating):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case errors.Is(err, orchestration.ErrInvalidTask):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, orchestration.ErrTaskUnavailable):
		return status.Errorf(codes.Unavailable, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func atespaceOf(atespace string) string {
	if atespace == "" {
		return v1alpha1.DefaultAtespace
	}
	return atespace
}

// --- Gateways ---

func (s *Server) GetGateway(ctx context.Context, req *v1alpha1.GetGatewayRequest) (*v1alpha1.Gateway, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	gw, err := s.store.GetGateway(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "gateway %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting gateway: %v", err)
	}
	return gw, nil
}

func (s *Server) ListGateways(ctx context.Context, req *v1alpha1.ListGatewaysRequest) (*v1alpha1.ListGatewaysResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	gateways, err := s.store.ListGateways(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing gateways: %v", err)
	}
	return &v1alpha1.ListGatewaysResponse{Gateways: gateways}, nil
}

func (s *Server) UpdateGateway(ctx context.Context, req *v1alpha1.UpdateGatewayRequest) (*v1alpha1.Gateway, error) {
	if req == nil || req.Gateway == nil {
		return nil, status.Error(codes.InvalidArgument, "gateway required")
	}
	if err := v1alpha1.ValidateMetadata(req.Gateway.GetMetadata()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Gateway.Metadata = defaultMetadata(req.Gateway.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetGateway(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})
	if err := s.store.SaveGateway(ctx, req.Gateway); err != nil {
		return nil, status.Errorf(codes.Internal, "saving gateway: %v", err)
	}
	return req.Gateway, nil
}

func (s *Server) DeleteGateway(ctx context.Context, req *v1alpha1.DeleteGatewayRequest) (*v1alpha1.DeleteGatewayResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	if err := s.store.DeleteGateway(ctx, atespace, req.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting gateway: %v", err)
	}
	return &v1alpha1.DeleteGatewayResponse{}, nil
}

// --- Workspaces ---

func (s *Server) GetWorkspace(ctx context.Context, req *v1alpha1.GetWorkspaceRequest) (*v1alpha1.Workspace, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	ws, err := s.store.GetWorkspace(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workspace %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting workspace: %v", err)
	}
	return ws, nil
}

func (s *Server) ListWorkspaces(ctx context.Context, req *v1alpha1.ListWorkspacesRequest) (*v1alpha1.ListWorkspacesResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	workspaces, err := s.store.ListWorkspaces(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing workspaces: %v", err)
	}
	return &v1alpha1.ListWorkspacesResponse{Workspaces: workspaces}, nil
}

func (s *Server) UpdateWorkspace(ctx context.Context, req *v1alpha1.UpdateWorkspaceRequest) (*v1alpha1.Workspace, error) {
	if req == nil || req.Workspace == nil {
		return nil, status.Error(codes.InvalidArgument, "workspace required")
	}
	if err := v1alpha1.ValidateMetadata(req.Workspace.GetMetadata()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Workspace.Metadata = defaultMetadata(req.Workspace.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetWorkspace(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})
	if err := s.store.SaveWorkspace(ctx, req.Workspace); err != nil {
		return nil, status.Errorf(codes.Internal, "saving workspace: %v", err)
	}
	return req.Workspace, nil
}

func (s *Server) DeleteWorkspace(ctx context.Context, req *v1alpha1.DeleteWorkspaceRequest) (*v1alpha1.DeleteWorkspaceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	if err := s.store.DeleteWorkspace(ctx, atespace, req.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting workspace: %v", err)
	}
	return &v1alpha1.DeleteWorkspaceResponse{}, nil
}

// --- Models ---

func (s *Server) GetModel(ctx context.Context, req *v1alpha1.GetModelRequest) (*v1alpha1.Model, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	model, err := s.store.GetModel(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "model %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting model: %v", err)
	}
	return model, nil
}

func (s *Server) ListModels(ctx context.Context, req *v1alpha1.ListModelsRequest) (*v1alpha1.ListModelsResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	models, err := s.store.ListModels(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing models: %v", err)
	}
	return &v1alpha1.ListModelsResponse{Models: models}, nil
}

func (s *Server) UpdateModel(ctx context.Context, req *v1alpha1.UpdateModelRequest) (*v1alpha1.Model, error) {
	if req == nil || req.Model == nil {
		return nil, status.Error(codes.InvalidArgument, "model required")
	}
	if err := v1alpha1.ValidateMetadata(req.Model.GetMetadata()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Model.Metadata = defaultMetadata(req.Model.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetModel(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})
	if err := s.store.SaveModel(ctx, req.Model); err != nil {
		return nil, status.Errorf(codes.Internal, "saving model: %v", err)
	}
	return req.Model, nil
}

// defaultMetadata normalizes resource metadata before a save: a missing atespace
// becomes "default", and the creation timestamp is carried over from the existing
// resource (looked up via existing) or set to now for a new one.
func defaultMetadata(meta *v1alpha1.ObjectMeta, existing func(atespace, name string) *v1alpha1.ObjectMeta) *v1alpha1.ObjectMeta {
	if meta == nil {
		meta = &v1alpha1.ObjectMeta{}
	}
	if meta.Atespace == "" {
		meta.Atespace = "default"
	}
	if meta.CreationTimestamp == nil {
		if prev := existing(meta.Atespace, meta.Name); prev.GetCreationTimestamp() != nil {
			meta.CreationTimestamp = prev.GetCreationTimestamp()
		} else {
			meta.CreationTimestamp = timestamppb.Now()
		}
	}
	return meta
}

func (s *Server) DeleteModel(ctx context.Context, req *v1alpha1.DeleteModelRequest) (*v1alpha1.DeleteModelResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	if err := s.store.DeleteModel(ctx, atespace, req.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting model: %v", err)
	}
	return &v1alpha1.DeleteModelResponse{}, nil
}
