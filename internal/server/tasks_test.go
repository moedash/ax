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
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeTasks stands in for the task workflows. It answers the way a workflow
// does: applying a task reports it running, suspending clears the worker
// address, and a deleted task first reports Terminating and then disappears.
type fakeTasks struct {
	mu    sync.Mutex
	tasks map[string]*v1alpha1.Task
	// applied records the desired state the server resolved, which is what the
	// workflow would be started with.
	applied []*workflows.TaskDesiredState
	// ready, when set, marks a task Ready as soon as it is applied.
	ready bool
	// unavailableGets is how many Gets in a row answer as if no worker were
	// there, which is what a controller restart looks like to the server.
	unavailableGets int
	// lastLimit records the limit the server asked a listing for.
	lastLimit int64
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

func (f *fakeTasks) Apply(
	ctx context.Context,
	desired *workflows.TaskDesiredState,
) (*v1alpha1.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	task, ok := proto.Clone(desired.Task).(*v1alpha1.Task)
	if !ok {
		return nil, fmt.Errorf("cloning task")
	}
	task.Status = &v1alpha1.TaskStatus{
		Phase:    v1alpha1.PhaseRunning,
		Actor:    task.GetMetadata().GetName(),
		WorkerIp: "10.244.1.42",
	}
	if f.ready {
		task.Status.Conditions = []*v1alpha1.Condition{{
			Type:   v1alpha1.ConditionReady,
			Status: v1alpha1.ConditionTrue,
			Reason: "TaskRunning",
		}}
	}
	f.applied = append(f.applied, desired)
	f.tasks[fakeKey(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName())] = task
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

	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return nil, orchestration.ErrTaskNotFound
	}
	if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
		return nil, orchestration.ErrTaskTerminating
	}
	if task.Spec == nil {
		task.Spec = &v1alpha1.TaskSpec{}
	}
	task.Spec.Suspend = suspend
	if suspend {
		task.Status.Phase = v1alpha1.PhaseSuspended
		task.Status.WorkerIp = ""
	} else {
		task.Status.Phase = v1alpha1.PhaseRunning
		task.Status.WorkerIp = "10.244.1.42"
	}
	return proto.Clone(task).(*v1alpha1.Task), nil
}

// Delete moves the task to Terminating, as a workflow does while it tears the
// sandbox down. finishDelete stands in for that teardown completing.
func (f *fakeTasks) Delete(ctx context.Context, atespace, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return orchestration.ErrTaskNotFound
	}
	task.Status.Phase = v1alpha1.PhaseTerminating
	return nil
}

func (f *fakeTasks) finishDelete(atespace, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tasks, fakeKey(atespace, name))
}

// markReady flips a task to ready, which is what a watch waits for.
func (f *fakeTasks) markReady(atespace, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[fakeKey(atespace, name)]
	if !ok {
		return
	}
	task.Status.Conditions = []*v1alpha1.Condition{{
		Type:   v1alpha1.ConditionReady,
		Status: v1alpha1.ConditionTrue,
		Reason: "TaskRunning",
	}}
}

func (f *fakeTasks) lastApplied() *workflows.TaskDesiredState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applied) == 0 {
		return nil
	}
	return f.applied[len(f.applied)-1]
}
