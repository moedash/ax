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

// Command ax-migrate-tasks moves the tasks the direct orchestrator kept in
// Redis into task workflows. It reads the task records, creates each task
// through an ax-server that runs with --orchestrator=temporal, and removes the
// record once the server has the task. Run it once, after switching the
// orchestrator, for a cluster that was switched with tasks still in it.
//
// Creating is idempotent here: a task the server already has counts as
// imported. So the tool can be run again after a failure. The task's actor is
// bound to the template the direct orchestrator built it from, so the task's
// workflow replaces the sandbox; whatever was in its workspace goes with it.
// Tasks that were deleted before the switch need none of this.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// listPageSize is how many task records one read of the store returns.
const listPageSize = 100

func main() {
	var (
		serverAddr    string
		redisAddr     string
		redisPassword string
		keyPrefix     string
		keep          bool
		timeout       time.Duration
	)
	flag.StringVar(&serverAddr, "server", "localhost:8080",
		"Address of the ax-server that runs tasks through Temporal")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379",
		"Redis server the direct orchestrator kept its tasks in")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&keyPrefix, "key-prefix", "ax", "Key prefix the store uses")
	flag.BoolVar(&keep, "keep", false, "Leave the task records in Redis after importing")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "How long one task may take to import")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rClient := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: redisPassword})
	defer rClient.Close()

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not reach ax-server", "server", serverAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	source := redis.NewStore(rClient, redis.Options{KeyPrefix: keyPrefix})
	api := v1alpha1.NewAXClient(conn)
	if err := migrate(context.Background(), source, api, keep, timeout); err != nil {
		slog.Error("migration did not finish", "error", err)
		os.Exit(1)
	}
}

// migrate creates every task the store has through the API server and removes
// each from the store once the server has taken it. A task the server refuses
// stays in the store and is reported, so a rerun picks up where this one
// stopped.
func migrate(
	ctx context.Context, source store.Store, api v1alpha1.AXClient,
	keep bool, timeout time.Duration,
) error {
	tasks, err := listAll(ctx, source)
	if err != nil {
		return fmt.Errorf("reading the stored tasks: %w", err)
	}
	slog.Info("found tasks to import", "count", len(tasks))

	var failed []string
	for _, task := range tasks {
		key := task.GetMetadata().GetAtespace() + "/" + task.GetMetadata().GetName()
		// The workflow derives the status from what it does; the stored one is
		// a record of what the direct orchestrator last saw.
		task.Status = nil
		if err := importOne(ctx, source, api, task, keep, timeout); err != nil {
			slog.Error("could not import task", "task", key, "error", err)
			failed = append(failed, key)
			continue
		}
		slog.Info("imported task", "task", key)
	}
	if len(failed) > 0 {
		return fmt.Errorf("tasks not imported: %s", strings.Join(failed, ", "))
	}
	return nil
}

// listAll walks the store's task index page by page. The index is ordered by
// creation time, so a page boundary is stable while nothing is being written.
func listAll(ctx context.Context, source store.Store) ([]*v1alpha1.Task, error) {
	var out []*v1alpha1.Task
	for offset := int64(0); ; offset += listPageSize {
		page, err := source.ListTasks(ctx, "", listPageSize, offset)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if int64(len(page)) < listPageSize {
			return out, nil
		}
	}
}

func importOne(
	ctx context.Context, source store.Store, api v1alpha1.AXClient, task *v1alpha1.Task,
	keep bool, timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := api.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: task})
	// A task the server already has was imported by an earlier run that did
	// not get to remove the record.
	if err != nil && status.Code(err) != codes.FailedPrecondition {
		return err
	}
	if keep {
		return nil
	}
	return source.DeleteTask(ctx, task.GetMetadata().GetAtespace(), task.GetMetadata().GetName())
}
