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

// Command ax-task-runner is the entrypoint of every AX task container. It
// loads the Task and Workspace specs and hands them to the runner package,
// which does everything else.
//
// The controller delivers the Task as YAML in AX_TASK_YAML and the bound
// Workspaces as a multi-document YAML stream in AX_WORKSPACES_YAML. For local
// runs the specs can be read from files instead with --task-file and one or
// more --workspace-file flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"

	"github.com/google/ax/internal/orchestration/workflows"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

// reportTimeout bounds reporting the command's exit. The sandbox stays up after
// the command finishes, so a report that cannot be delivered quickly is dropped
// rather than held on to.
const reportTimeout = 10 * time.Second

// stringList collects a repeatable flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var (
		cfg      runner.Config
		taskFile string
		wsFiles  stringList
	)
	flag.IntVar(&cfg.Port, "port", runner.DefaultPort, "Port for the metadata and guest server")
	flag.StringVar(&taskFile, "task-file", "", "Read the Task YAML from this file instead of AX_TASK_YAML")
	flag.Var(&wsFiles, "workspace-file", "Read Workspace YAML from this file instead of the environment; repeatable, and each file may hold several documents")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	var task v1alpha1.Task
	if err := loadSpec(taskFile, "AX_TASK_YAML", &task); err != nil {
		fatal(err)
	} else if task.GetMetadata() != nil {
		cfg.Task = &task
	}

	workspaces, err := loadWorkspaces(wsFiles)
	if err != nil {
		fatal(err)
	}
	cfg.Workspaces = workspaces
	cfg.OnCommandExit = reportCommandExit

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runner.Run(ctx, cfg); err != nil {
		fatal(err)
	}
}

// loadSpec decodes YAML into out from file when set, otherwise from the named
// environment variable. It is not an error for neither to be present.
func loadSpec(file, envVar string, out any) error {
	var raw []byte
	switch {
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading %s: %w", file, err)
		}
		raw = data
	case os.Getenv(envVar) != "":
		raw = []byte(os.Getenv(envVar))
	default:
		return nil
	}
	if err := yaml.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("parsing %T: %w", out, err)
	}
	return nil
}

// loadWorkspaces reads Workspace documents from the given files, or when none
// are given from AX_WORKSPACES_YAML. Empty documents are skipped. It is not an
// error for no source to be present.
func loadWorkspaces(files []string) ([]*v1alpha1.Workspace, error) {
	var sources [][]byte
	switch {
	case len(files) > 0:
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", f, err)
			}
			sources = append(sources, data)
		}
	case os.Getenv("AX_WORKSPACES_YAML") != "":
		sources = append(sources, []byte(os.Getenv("AX_WORKSPACES_YAML")))
	}

	var workspaces []*v1alpha1.Workspace
	for _, src := range sources {
		dec := yaml.NewDecoder(strings.NewReader(string(src)))
		for {
			var ws v1alpha1.Workspace
			err := dec.Decode(&ws)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("parsing workspace yaml: %w", err)
			}
			if ws.GetMetadata() != nil {
				workspaces = append(workspaces, &ws)
			}
		}
	}
	return workspaces, nil
}

// reportCommandExit tells the task's workflow how the command finished, which
// is what turns a task Completed and carries its exit status. The report is
// best effort: this process is PID 1 of the sandbox, and the sandbox has to stay
// up and inspectable whether or not the control plane can be reached.
func reportCommandExit(exit runner.CommandExit) {
	address := os.Getenv(v1alpha1.EnvTemporalAddress)
	workflowID := os.Getenv(v1alpha1.EnvWorkflowID)
	if address == "" || workflowID == "" {
		slog.Info("no task workflow to report to", "exitCode", exit.ExitCode)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	// A lazy client connects on first use, so the whole report stays inside the
	// deadline above instead of blocking PID 1 on a dial.
	c, err := client.NewLazyClient(client.Options{
		HostPort:  address,
		Namespace: os.Getenv(v1alpha1.EnvTemporalNamespace),
	})
	if err != nil {
		slog.Error("could not reach the task workflow", "address", address, "error", err)
		return
	}
	defer c.Close()

	in := workflows.CompleteInput{ExitCode: int32(exit.ExitCode)}
	if exit.Err != nil {
		in.Message = exit.Err.Error()
	}

	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateName:   workflows.UpdateComplete,
		Args:         []any{in},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		if err = handle.Get(ctx, nil); err == nil {
			slog.Info("reported the task command exit", "workflow", workflowID, "exitCode", exit.ExitCode)
			return
		}
	}

	// An update needs a worker to accept it. A signal only needs the service, so
	// it still records the exit when no worker is available right now.
	slog.Warn("could not report the task command exit as an update", "error", err)
	if err := c.SignalWorkflow(ctx, workflowID, "", workflows.SignalComplete, in); err != nil {
		slog.Error("could not report the task command exit", "workflow", workflowID, "error", err)
	}
}

func fatal(err error) {
	slog.Error("task runner failed", "error", err)
	os.Exit(1)
}
