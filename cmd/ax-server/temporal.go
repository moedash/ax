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
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/google/ax/internal/model"
	"github.com/google/ax/internal/orchestration"
	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/internal/orchestration/taskclient"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/substrate"
)

// Orchestrators the server can run tasks through.
const (
	// orchestratorDirect reconciles each task call with Substrate inline, under
	// a distributed lock.
	orchestratorDirect = "direct"
	// orchestratorTemporal runs each task as a Temporal workflow and hosts the
	// worker for it in this process.
	orchestratorTemporal = "temporal"
)

const (
	// maxConcurrentActivities is high because activities spend their time
	// waiting: on a Substrate control call, on an actor being placed, or on a
	// workspace being set up inside a sandbox.
	maxConcurrentActivities = 200

	// workerStopTimeout outlasts the longest control-plane call, placing an
	// actor on a worker, so a shutdown does not abandon one halfway. The pod's
	// termination grace period has to allow for it; a call cut short is retried
	// by another worker either way.
	workerStopTimeout = 6 * time.Minute
)

// temporalOptions is everything the Temporal orchestrator needs. The flags are
// accepted whatever the orchestrator is and read only when it is temporal, so
// the default command line is the same as without them.
type temporalOptions struct {
	address        string
	namespace      string
	taskQueue      string
	resyncInterval time.Duration
	watchInterval  time.Duration
}

func (o *temporalOptions) bindFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.address, "temporal-address", "localhost:7233",
		"Temporal frontend address (with --orchestrator=temporal)")
	fs.StringVar(&o.namespace, "temporal-namespace", "default",
		"Temporal namespace (with --orchestrator=temporal)")
	fs.StringVar(&o.taskQueue, "task-queue", "ax-tasks",
		"Task queue the task workflows run on (with --orchestrator=temporal)")
	fs.DurationVar(&o.resyncInterval, "resync-interval", workflows.DefaultConfig().ResyncInterval,
		"How often a settled task checks its sandbox against Substrate. "+
			"Each interval costs one read per task (with --orchestrator=temporal)")
	fs.DurationVar(&o.watchInterval, "watch-interval", 2*time.Second,
		"How often WatchTask asks a task workflow for its state (with --orchestrator=temporal)")
}

// applyEnv lets the deployment override the flags the way the Redis ones are.
func (o *temporalOptions) applyEnv() {
	if env := os.Getenv("TEMPORAL_ADDRESS"); env != "" {
		o.address = env
	}
	if env := os.Getenv("TEMPORAL_NAMESPACE"); env != "" {
		o.namespace = env
	}
	if env := os.Getenv("AX_TASK_QUEUE"); env != "" {
		o.taskQueue = env
	}
}

// startTemporal connects to Temporal and starts the worker that drives tasks.
// It returns the client the API server reaches tasks through and a function
// that stops both.
func startTemporal(
	ctx context.Context, o temporalOptions, sub *substrate.Client, logger *slog.Logger,
) (orchestration.Tasks, func(), error) {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  o.address,
		Namespace: o.namespace,
		Logger:    log.NewStructuredLogger(logger),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to temporal at %s: %w", o.address, err)
	}

	w := worker.New(temporalClient, o.taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: maxConcurrentActivities,
		WorkerStopTimeout:                  workerStopTimeout,
	})
	// Registered under the workflow type rather than the function, so servers
	// with different settings still answer for the same tasks.
	w.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(workflows.Config{ResyncInterval: o.resyncInterval}),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)
	w.RegisterActivity(&activities.Activities{
		Substrate:      sub,
		SecretResolver: model.GetKubernetesSecret,
		RouterAddr:     os.Getenv("ATENET_ROUTER_ADDR"),
	})
	if err := w.Start(); err != nil {
		temporalClient.Close()
		return nil, nil, fmt.Errorf("starting the temporal worker: %w", err)
	}

	stop := func() {
		w.Stop()
		temporalClient.Close()
	}
	return taskclient.New(temporalClient, o.taskQueue), stop, nil
}
