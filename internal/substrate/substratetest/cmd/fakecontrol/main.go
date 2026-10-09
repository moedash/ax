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
// A small HTTP API on --fault-addr lets the demos under demo/ inject a fault
// while a task runs, and read back what the fake really holds:
//
//	GET  /actors                          each actor's state
//	POST /fault?create-actor-failures=N   fail the next N CreateActor calls
//	POST /fault?suspend=fail|ok           fail every SuspendActor call, or stop
//	POST /fault?resume-delay=25s          hold every ResumeActor call open
//
// It keeps no state beyond the process and is not meant for anything else.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/ax/internal/substrate/substratetest"
)

// errSuspendRejected is Unavailable on purpose. The Temporal path retries it, so
// a failed suspend keeps failing until the fault is cleared.
var errSuspendRejected = status.Error(codes.Unavailable,
	"substrate control plane rejected suspend")

func main() {
	var controlAddr, sandboxAddr, faultAddr string
	flag.StringVar(&controlAddr, "addr", "127.0.0.1:9001", "Address to serve the fake Control API on")
	flag.StringVar(&sandboxAddr, "sandbox-addr", "127.0.0.1:9002",
		"Address to serve the fake sandbox readiness endpoint on")
	flag.StringVar(&faultAddr, "fault-addr", "127.0.0.1:9003",
		"Address to serve the fault HTTP API the demos use")
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
	go serveFaults(faultAddr, control)

	listener, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatalf("serving the fake Control API: %v", err)
	}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	log.Printf("fake Control API on %s, fake sandbox on %s, fault API on %s",
		listener.Addr(), sandbox.Addr(), faultAddr)
	log.Fatalf("serving the fake Control API: %v", server.Serve(listener))
}

// serveFaults serves the fault API described at the top of this file.
func serveFaults(addr string, control *substratetest.ControlServer) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /actors", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(control.ActorStates())
	})
	mux.HandleFunc("POST /fault", func(w http.ResponseWriter, r *http.Request) {
		if err := applyFaults(control, r.URL.Query()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	})
	log.Fatalf("serving the fault API: %v", http.ListenAndServe(addr, mux))
}

// applyFaults rejects a value it can't read instead of ignoring it, so a demo
// never runs against a fault it didn't get.
func applyFaults(control *substratetest.ControlServer, query url.Values) error {
	if v := query.Get("create-actor-failures"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("create-actor-failures: %w", err)
		}
		control.SetCreateActorFailures(n)
	}
	switch query.Get("suspend") {
	case "":
	case "fail":
		control.SetSuspendErr(errSuspendRejected)
	case "ok":
		control.SetSuspendErr(nil)
	default:
		return errors.New("suspend must be fail or ok")
	}
	if v := query.Get("resume-delay"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("resume-delay: %w", err)
		}
		control.SetResumeDelay(d)
	}
	return nil
}
