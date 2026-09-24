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

// Command ax-server serves the AX gRPC API. Task calls go to Temporal, which
// owns every task's lifecycle; the configuration kinds tasks bind are kept in
// Redis.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"

	"github.com/google/ax/internal/orchestration/taskclient"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/redis"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultTemporalAddress   = "localhost:7233"
	defaultTemporalNamespace = "default"
	defaultTaskQueue         = "ax-tasks"
)

func main() {
	var (
		listenAddr        string
		redisAddr         string
		redisPassword     string
		temporalAddress   string
		temporalNamespace string
		taskQueue         string
		watchInterval     time.Duration
	)

	flag.StringVar(&listenAddr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address for the configuration kinds")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&temporalAddress, "temporal-address", defaultTemporalAddress, "Temporal frontend address")
	flag.StringVar(&temporalNamespace, "temporal-namespace", defaultTemporalNamespace, "Temporal namespace")
	flag.StringVar(&taskQueue, "task-queue", defaultTaskQueue, "Task queue task workflows are started on")
	flag.DurationVar(&watchInterval, "watch-interval", 2*time.Second, "How often WatchTask asks a task for its state. Each interval is one query per open watch.")
	flag.Parse()

	if env := os.Getenv("ADDR"); env != "" {
		listenAddr = env
	}
	if env := os.Getenv("REDIS_ADDR"); env != "" {
		redisAddr = env
	}
	if env := os.Getenv("REDIS_PASSWORD"); env != "" {
		redisPassword = env
	}
	if env := os.Getenv("TEMPORAL_ADDRESS"); env != "" {
		temporalAddress = env
	}
	if env := os.Getenv("TEMPORAL_NAMESPACE"); env != "" {
		temporalNamespace = env
	}
	if env := os.Getenv("AX_TASK_QUEUE"); env != "" {
		taskQueue = env
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server",
		"listenAddr", listenAddr,
		"redisAddr", redisAddr,
		"temporalAddress", temporalAddress,
		"temporalNamespace", temporalNamespace,
		"taskQueue", taskQueue,
	)

	rClient := goredis.NewClient(&goredis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	})
	defer rClient.Close()

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

	srv := server.NewServer(
		redis.NewStore(rClient, redis.Options{}),
		taskclient.New(temporalClient, taskQueue),
		server.Options{WatchPollInterval: watchInterval},
	)

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down ax-server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
