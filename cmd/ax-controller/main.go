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

// Command ax-controller runs the Temporal worker that drives AX tasks. It polls
// the task queue for task workflows and activities, and turns them into calls
// against Agent Substrate. Run as many replicas as the load needs; Temporal
// hands each piece of work to exactly one of them.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/google/ax/internal/model"
	"github.com/google/ax/internal/orchestration/activities"
	"github.com/google/ax/internal/orchestration/taskclient"
	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/internal/substrate"
)

const (
	defaultTemporalAddress   = "localhost:7233"
	defaultTemporalNamespace = "default"
	defaultTaskQueue         = "ax-tasks"

	// maxConcurrentActivities is high because activities spend their time
	// waiting: on a Substrate control call, on an actor being placed, or on a
	// workspace being set up inside a sandbox.
	maxConcurrentActivities = 200

	// workerStopTimeout outlasts the longest control-plane call, placing an
	// actor on a worker, so a shutdown does not abandon one halfway. The
	// readiness poll can run longer than this and is cut short on purpose: it
	// is idempotent and another worker picks it up.
	workerStopTimeout = 6 * time.Minute

	// startupCheckTimeout bounds the one read of the namespace made before the
	// worker starts polling.
	startupCheckTimeout = 30 * time.Second
)

func main() {
	var (
		resyncInterval       time.Duration
		temporalAddress      string
		temporalNamespace    string
		taskQueue            string
		sandboxAddress       string
		reportCompletion     bool
		substrateEndpoint    string
		substrateAuthority   string
		substrateTokenFile   string
		substrateCAFile      string
		substrateInsecureTLS bool
		substratePlaintext   bool
	)

	flag.StringVar(&temporalAddress, "temporal-address", defaultTemporalAddress, "Temporal frontend address")
	flag.StringVar(&temporalNamespace, "temporal-namespace", defaultTemporalNamespace, "Temporal namespace")
	flag.StringVar(&taskQueue, "task-queue", defaultTaskQueue, "Task queue this worker polls")
	flag.DurationVar(&resyncInterval, "resync-interval", workflows.DefaultConfig().ResyncInterval,
		"How often a settled task checks its sandbox against Substrate. Each interval costs one read per task.")
	flag.BoolVar(&reportCompletion, "sandbox-report-completion", false, "Let task containers report their command's exit to their own workflow. This gives anything in a sandbox a route to the Temporal frontend, so it is only as safe as the frontend's authentication.")
	flag.StringVar(&sandboxAddress, "sandbox-temporal-address", "", "Temporal address task containers dial to report their command's exit (defaults to --temporal-address)")
	flag.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	flag.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	flag.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	flag.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	flag.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	flag.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	flag.Parse()

	if env := os.Getenv("TEMPORAL_ADDRESS"); env != "" {
		temporalAddress = env
	}
	if env := os.Getenv("TEMPORAL_NAMESPACE"); env != "" {
		temporalNamespace = env
	}
	if env := os.Getenv("AX_TASK_QUEUE"); env != "" {
		taskQueue = env
	}
	if sandboxAddress == "" {
		sandboxAddress = temporalAddress
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	workflowConfig := workflows.DefaultConfig()
	if resyncInterval > 0 {
		workflowConfig.ResyncInterval = resyncInterval
	}

	slog.Info("starting ax-controller",
		"temporalAddress", temporalAddress,
		"resyncInterval", workflowConfig.ResyncInterval,
		"temporalNamespace", temporalNamespace,
		"taskQueue", taskQueue,
		"sandboxReportCompletion", reportCompletion,
		"substrateEndpoint", substrateEndpoint,
		"authority", substrateAuthority,
	)

	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Error("failed to initialize substrate client", "error", err)
		os.Exit(1)
	}
	defer subClient.Close()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  temporalAddress,
		Namespace: temporalNamespace,
		Logger:    log.NewStructuredLogger(logger),
	})
	if err != nil {
		slog.Error("failed to connect to temporal", "address", temporalAddress, "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	// A task publishes search attributes on every phase change. One the
	// namespace does not have fails the workflow task, so every task would loop
	// without ever saying why; better to refuse to start and say it here.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), startupCheckTimeout)
	err = taskclient.VerifySearchAttributes(verifyCtx, temporalClient.OperatorService(), temporalNamespace)
	cancelVerify()
	if err != nil {
		slog.Error("the namespace is not ready for tasks", "error", err)
		os.Exit(1)
	}

	w := worker.New(temporalClient, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: maxConcurrentActivities,
		WorkerStopTimeout:                  workerStopTimeout,
	})
	// Registered under the workflow type rather than the function, so workers
	// with different settings still answer for the same tasks.
	w.RegisterWorkflowWithOptions(
		workflows.NewTaskWorkflow(workflowConfig),
		workflow.RegisterOptions{Name: workflows.TaskWorkflowType},
	)
	w.RegisterActivity(&activities.Activities{
		Substrate:         subClient,
		SecretResolver:    model.GetKubernetesSecret,
		RouterAddr:        os.Getenv("ATENET_ROUTER_ADDR"),
		ReportCompletion:  reportCompletion,
		TemporalAddress:   sandboxAddress,
		TemporalNamespace: temporalNamespace,
	})

	if err := w.Run(worker.InterruptCh()); err != nil {
		slog.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
}
