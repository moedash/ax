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

// Command fakecontrol serves the Agent Substrate Control API from the
// in-process fake, plus a readiness endpoint that stands in for a sandbox. It
// exists so the whole AX control plane can be run and watched on a laptop
// without a Substrate cluster:
//
//	go run ./internal/substrate/substratetest/cmd/fakecontrol
//	go run ./cmd/ax-server --orchestrator=temporal \
//	  --substrate-endpoint=127.0.0.1:9001 --substrate-plaintext
//
// The fault flags let a demo replay a control plane that misbehaves. A control
// HTTP API lets a demo flip a fault on while a task is already running and read
// back what the fake really holds. It keeps no state beyond the process and is
// not meant for anything else.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/ax/internal/substrate/substratetest"
)

// errSuspendRejected is what a suspend fails with. Unavailable is retried by the
// Temporal path, so the suspend keeps failing rather than settling the task.
var errSuspendRejected = status.Error(codes.Unavailable,
	"substrate control plane rejected suspend")

func main() {
	var (
		controlAddr          string
		sandboxAddr          string
		apiAddr              string
		failCreateActorTimes int
		failSuspend          bool
		delayResume          time.Duration
	)
	flag.StringVar(&controlAddr, "addr", "127.0.0.1:9001", "Address to serve the fake Control API on")
	flag.StringVar(&sandboxAddr, "sandbox-addr", "127.0.0.1:9002",
		"Address to serve the fake sandbox readiness endpoint on")
	flag.StringVar(&apiAddr, "control-addr", "127.0.0.1:9003",
		"Address to serve the fault and introspection HTTP API on")
	flag.IntVar(&failCreateActorTimes, "fail-create-actor-times", 0,
		"Fail the first N CreateActor calls with Unavailable, then succeed")
	flag.BoolVar(&failSuspend, "fail-suspend", false, "Fail every SuspendActor call")
	flag.DurationVar(&delayResume, "delay-resume", 0,
		"Sleep this long inside every ResumeActor call, so a resume can be interrupted")
	flag.Parse()

	sandbox, err := net.Listen("tcp", sandboxAddr)
	if err != nil {
		log.Fatalf("serving the fake sandbox: %v", err)
	}
	go func() {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Printf("sandbox probe %s", r.URL)
			w.WriteHeader(http.StatusOK)
		})
		log.Fatalf("serving the fake sandbox: %v", http.Serve(sandbox, handler))
	}()

	// Actors are reported as running on the fake sandbox, so the workspace
	// readiness poll finds something to talk to.
	control := substratetest.NewControlServer()
	control.WorkerIP = sandbox.Addr().String()
	control.SetFailCreateActorTimes(failCreateActorTimes)
	control.SetResumeDelay(delayResume)
	if failSuspend {
		control.SetSuspendErr(errSuspendRejected)
	}

	go serveControlAPI(apiAddr, control)

	listener, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatalf("serving the fake Control API: %v", err)
	}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	log.Printf("fake Control API on %s, fake sandbox on %s, control HTTP API on %s",
		listener.Addr(), sandbox.Addr(), apiAddr)
	log.Fatalf("serving the fake Control API: %v", server.Serve(listener))
}

// serveControlAPI lets a demo read what the fake holds and flip faults on while
// a task is already running, which the startup flags cannot do on their own.
func serveControlAPI(addr string, control *substratetest.ControlServer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/introspect", func(w http.ResponseWriter, r *http.Request) {
		snapshot := map[string]any{
			"actors":    control.ActorStates(),
			"created":   control.CreatedActors(),
			"resumed":   control.ResumedActors(),
			"suspended": control.SuspendedActors(),
			"reverted":  control.RevertedActors(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot)
	})
	mux.HandleFunc("/knobs", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("fail-suspend"); v != "" {
			if v == "true" {
				control.SetSuspendErr(errSuspendRejected)
			} else {
				control.SetSuspendErr(nil)
			}
		}
		if v := r.URL.Query().Get("fail-create-actor-times"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				control.SetFailCreateActorTimes(n)
			}
		}
		if v := r.URL.Query().Get("delay-resume"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				control.SetResumeDelay(d)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	log.Fatalf("serving the control HTTP API: %v", http.ListenAndServe(addr, mux))
}
