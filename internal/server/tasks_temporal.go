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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// The task RPCs below are the Temporal path of the server, taken when Options
// carries a Tasks client. Each task is a workflow: the RPCs send it updates and
// queries instead of locking a record and calling Substrate inline, and the
// workflow ID is what serializes changes to a task, so no lock is taken here.

const (
	// defaultWatchPollInterval is how often WatchTask asks a task for its state.
	// Each interval is one query per open watch, so it is not free.
	defaultWatchPollInterval = 2 * time.Second

	// maxUnavailablePolls is how many polls in a row a watch sits through with
	// no worker answering for the task before it gives up. A server rollout
	// takes seconds; a task nobody answers for much longer than that is a
	// problem the watcher should hear about.
	maxUnavailablePolls = 15

	// maxListTasks caps one listing. Anything larger walks visibility page by
	// page on the caller's behalf, and a caller that wants more can page.
	maxListTasks = 500
)

func (s *Server) getTaskWorkflow(
	ctx context.Context, atespace, name string,
) (*v1alpha1.Task, error) {
	if err := taskRef(atespace, name); err != nil {
		return nil, err
	}
	task, err := s.tasks.Get(ctx, atespace, name)
	if err != nil {
		return nil, taskError(err, atespace, name)
	}
	return task, nil
}

func (s *Server) listTaskWorkflows(
	ctx context.Context, atespace string, limit, offset int64,
) (*v1alpha1.ListTasksResponse, error) {
	if atespace != "" && atespace != "*" {
		if err := v1alpha1.ValidateName(atespace); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "atespace: invalid value %q: %v",
				atespace, err)
		}
	}
	tasks, err := s.tasks.List(ctx, atespace, min(limit, maxListTasks), offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tasks: %v", err)
	}
	return &v1alpha1.ListTasksResponse{Tasks: tasks}, nil
}

// createTaskWorkflow starts the task's workflow. The workspaces the task binds
// are resolved here and handed to it, so the worker never reads the store.
func (s *Server) createTaskWorkflow(
	ctx context.Context, task *v1alpha1.Task,
) (*v1alpha1.Task, error) {
	atespace := task.GetMetadata().GetAtespace()
	name := task.GetMetadata().GetName()
	desired := &workflows.TaskDesiredState{Task: task}

	// A binding that does not exist is left out rather than rejected: a task may
	// be created before its workspace is, and the runner treats a missing
	// workspace as an empty directory.
	for _, ref := range task.GetSpec().WorkspaceRefs() {
		if ref.GetName() == "" {
			continue
		}
		ws, err := s.store.GetWorkspace(ctx, atespace, ref.GetName())
		switch {
		case err == nil:
			desired.Workspaces = append(desired.Workspaces, ws)
		case errors.Is(err, store.ErrNotFound):
			slog.Warn("task binds a workspace that does not exist",
				"task", name, "workspace", ref.GetName())
		default:
			return nil, status.Errorf(codes.Internal, "reading workspace %q: %v",
				ref.GetName(), err)
		}
	}

	created, err := s.tasks.Create(ctx, desired)
	if err != nil {
		return nil, taskError(err, atespace, name)
	}
	return created, nil
}

func (s *Server) deleteTaskWorkflow(
	ctx context.Context, atespace, name string,
) (*v1alpha1.DeleteTaskResponse, error) {
	if err := taskRef(atespace, name); err != nil {
		return nil, err
	}
	if err := s.tasks.Delete(ctx, atespace, name); err != nil {
		return nil, taskError(err, atespace, name)
	}
	return &v1alpha1.DeleteTaskResponse{}, nil
}

func (s *Server) suspendTaskWorkflow(
	ctx context.Context, atespace, name string,
) (*v1alpha1.Task, error) {
	if err := taskRef(atespace, name); err != nil {
		return nil, err
	}
	task, err := s.tasks.Suspend(ctx, atespace, name)
	if err != nil {
		return nil, taskError(err, atespace, name)
	}
	return task, nil
}

func (s *Server) resumeTaskWorkflow(
	ctx context.Context, atespace, name string,
) (*v1alpha1.Task, error) {
	if err := taskRef(atespace, name); err != nil {
		return nil, err
	}
	task, err := s.tasks.Resume(ctx, atespace, name)
	if err != nil {
		return nil, taskError(err, atespace, name)
	}
	return task, nil
}

// watchTaskWorkflow streams a task's state as it changes. It asks the task for
// its state on an interval and emits whatever is different, until the task is
// ready, has finished, has failed, or is gone.
//
// A task nobody answers for is not gone: the worker may be restarting. The
// watch sits through a bounded number of such polls before it gives up.
func (s *Server) watchTaskWorkflow(
	atespace, name string, stream grpc.ServerStreamingServer[v1alpha1.WatchTaskResponse],
) error {
	if err := taskRef(atespace, name); err != nil {
		return err
	}
	ctx := stream.Context()

	ticker := time.NewTicker(s.watchPollInterval)
	defer ticker.Stop()

	var last *v1alpha1.Task
	action := "INITIAL"
	unavailable := 0
	for {
		task, err := s.tasks.Get(ctx, atespace, name)
		switch {
		case err == nil:
			unavailable = 0
		case errors.Is(err, orchestration.ErrTaskNotFound) && last != nil:
			return stream.Send(&v1alpha1.WatchTaskResponse{Task: last, Action: "DELETED"})
		case errors.Is(err, orchestration.ErrTaskUnavailable) && unavailable < maxUnavailablePolls:
			unavailable++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			continue
		default:
			return taskError(err, atespace, name)
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

// taskRef validates the identity a task call names. Names and atespaces become
// workflow IDs and visibility queries, so they are checked on every path that
// builds one, not only on the create.
func taskRef(atespace, name string) error {
	meta := &v1alpha1.ObjectMeta{Name: name, Atespace: atespace}
	if err := v1alpha1.ValidateObjectMeta(meta); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

// taskError turns an orchestration error into the status code its caller
// expects. The codes match what the synchronous path answers for the same
// situations, so a client cannot tell the two apart.
func taskError(err error, atespace, name string) error {
	switch {
	case errors.Is(err, orchestration.ErrTaskNotFound):
		return status.Errorf(codes.NotFound, "task %q not found in atespace %q", name, atespace)
	case errors.Is(err, orchestration.ErrTaskExists):
		return status.Errorf(codes.FailedPrecondition,
			"task %s/%s already exists and is immutable", atespace, name)
	case errors.Is(err, orchestration.ErrTaskTerminating):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case errors.Is(err, orchestration.ErrInvalidTask):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, orchestration.ErrTaskUnavailable):
		return status.Errorf(codes.Unavailable, "%v", err)
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
