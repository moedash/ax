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

package activities

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.temporal.io/sdk/activity"
)

// defaultPollInterval is the delay between readiness probes when the input
// leaves it unset.
const defaultPollInterval = 500 * time.Millisecond

// AwaitWorkspaceReady polls a sandbox until its workspace setup has finished.
//
// The poll lives inside the activity so that waiting for a maiden run, which
// clones repositories and installs skills, costs one event in the workflow
// history instead of one per probe. It heartbeats on every iteration, so a
// worker that dies mid-wait is noticed and the poll is restarted elsewhere.
//
// Expiry is reported as not-ready rather than as an error: a workspace that is
// slow to come up leaves the task running with WorkspaceReady False, which is
// what an operator needs to see.
func (a *Activities) AwaitWorkspaceReady(ctx context.Context, in WorkspaceReadyInput) (bool, error) {
	if in.WorkerIP == "" {
		return false, nil
	}
	logger := activity.GetLogger(ctx)
	interval := in.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	// A retry continues the wait the first attempt started. Without this, an
	// activity that is retried a few times waits several times its budget.
	deadline := time.Now().Add(in.Timeout)
	if activity.HasHeartbeatDetails(ctx) {
		var started time.Time
		if err := activity.GetHeartbeatDetails(ctx, &started); err == nil && !started.IsZero() {
			deadline = started
		}
	}

	for {
		if a.probeWorkspace(ctx, in) {
			logger.Info("workspace setup finished", "actor", in.Actor.Name, "workerIP", in.WorkerIP)
			return true, nil
		}
		activity.RecordHeartbeat(ctx, deadline)
		if err := ctx.Err(); err != nil {
			return false, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			logger.Info("workspace setup still running", "actor", in.Actor.Name, "waited", in.Timeout.String())
			return false, nil
		}
		if remaining < interval {
			interval = remaining
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// probeWorkspace reports whether the sandbox says its workspace is ready. It
// tries the worker pod directly first and falls back to the atenet router,
// which is the only route when the worker's pod network is not reachable.
func (a *Activities) probeWorkspace(ctx context.Context, in WorkspaceReadyInput) bool {
	host, port := in.WorkerIP, "80"
	if h, p, err := net.SplitHostPort(in.WorkerIP); err == nil {
		host, port = h, p
	}
	if a.probeOK(ctx, fmt.Sprintf("http://%s:%s/readyz?check=workspace", host, port), "") {
		return true
	}
	if a.RouterAddr == "" {
		return false
	}
	target := fmt.Sprintf("%s/%s", in.Actor.Atespace, in.Actor.Name)
	return a.probeOK(ctx, fmt.Sprintf("http://%s/readyz?check=workspace", a.RouterAddr), target)
}

// probeOK issues one readiness request, addressing an actor through the router
// when targetActor is set.
func (a *Activities) probeOK(ctx context.Context, url, targetActor string) bool {
	reqCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	if targetActor != "" {
		req.Header.Set("ate-target-actor", targetActor)
	}
	resp, err := a.httpClient().Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}
