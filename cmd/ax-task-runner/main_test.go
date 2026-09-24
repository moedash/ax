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
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

// Reporting a command exit must never take the sandbox down with it, however
// unreachable the control plane is.
func TestReportCommandExitSurvivesAnUnreachableControlPlane(t *testing.T) {
	t.Setenv(v1alpha1.EnvTemporalAddress, "127.0.0.1:1")
	t.Setenv(v1alpha1.EnvWorkflowID, "default/job")
	t.Setenv(v1alpha1.EnvTemporalNamespace, "default")

	done := make(chan struct{})
	go func() {
		defer close(done)
		reportCommandExit(runner.CommandExit{Pid: 42, ExitCode: 1})
	}()

	select {
	case <-done:
	case <-time.After(reportTimeout + 5*time.Second):
		t.Fatal("reporting the command exit did not give up")
	}
}

// Without a workflow to report to, the exit is only logged.
func TestReportCommandExitWithoutAWorkflow(t *testing.T) {
	t.Setenv(v1alpha1.EnvTemporalAddress, "")
	t.Setenv(v1alpha1.EnvWorkflowID, "")

	reportCommandExit(runner.CommandExit{Pid: 42, ExitCode: 0})
}
