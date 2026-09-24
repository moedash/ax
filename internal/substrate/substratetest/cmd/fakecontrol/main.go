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
//	go run ./cmd/ax-controller --substrate-endpoint=127.0.0.1:9001 --substrate-plaintext
//
// It keeps no state beyond the process and is not meant for anything else.
package main

import (
	"flag"
	"log"
	"net"
	"net/http"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"

	"github.com/google/ax/internal/substrate/substratetest"
)

func main() {
	var controlAddr, sandboxAddr string
	flag.StringVar(&controlAddr, "addr", "127.0.0.1:9001", "Address to serve the fake Control API on")
	flag.StringVar(&sandboxAddr, "sandbox-addr", "127.0.0.1:9002", "Address to serve the fake sandbox readiness endpoint on")
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

	listener, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatalf("serving the fake Control API: %v", err)
	}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	log.Printf("fake Control API on %s, fake sandbox on %s", listener.Addr(), sandbox.Addr())
	log.Fatalf("serving the fake Control API: %v", server.Serve(listener))
}
