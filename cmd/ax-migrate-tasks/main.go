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

// Command ax-migrate-tasks moves the tasks an earlier control plane kept in
// Redis into task workflows. It reads the old task index, applies each task
// through the API server, and removes the task's Redis keys once the server
// has it. Run it once, after the new ax-server is up, for a cluster that was
// upgraded with tasks still in it.
//
// Applying is idempotent, so the tool can be run again after a failure. The
// old actor is bound to the template the old controller built it from, so the
// task's workflow replaces the sandbox; whatever was in its workspace goes
// with it. Tasks that were deleted before the upgrade need none of this.
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

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
		"Address of the ax-server that owns tasks now")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379",
		"Redis server the old control plane used")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&keyPrefix, "key-prefix", "ax", "Key prefix the old control plane used")
	flag.BoolVar(&keep, "keep", false, "Leave the Redis task keys in place after importing")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "How long one task may take to import")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rClient := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: redisPassword})
	defer rClient.Close()

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not reach ax-server", "server", serverAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	source := redisTasks{client: rClient, prefix: keyPrefix}
	api := v1alpha1.NewAXClient(conn)
	if err := migrate(context.Background(), source, api, keep, timeout); err != nil {
		slog.Error("migration did not finish", "error", err)
		os.Exit(1)
	}
}

// taskSource is where the old control plane kept its tasks.
type taskSource interface {
	// List returns every task the source has.
	List(ctx context.Context) ([]*v1alpha1.Task, error)
	// Remove forgets one task. It is called once the API server has the task.
	Remove(ctx context.Context, task *v1alpha1.Task) error
}

// migrate applies every task the source has and removes each from the source
// once the server has taken it. A task the server refuses stays in the source
// and is reported, so a rerun picks up where this one stopped.
func migrate(
	ctx context.Context, source taskSource, api v1alpha1.AXClient, keep bool, timeout time.Duration,
) error {
	tasks, err := source.List(ctx)
	if err != nil {
		return fmt.Errorf("reading the old tasks: %w", err)
	}
	slog.Info("found tasks to import", "count", len(tasks))

	var failed []string
	for _, task := range tasks {
		key := task.GetMetadata().GetAtespace() + "/" + task.GetMetadata().GetName()
		// The workflow derives the status from what it does; the old one is a
		// record of what a controller that no longer runs last saw.
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

func importOne(
	ctx context.Context, source taskSource, api v1alpha1.AXClient, task *v1alpha1.Task,
	keep bool, timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := api.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: task}); err != nil {
		return err
	}
	if keep {
		return nil
	}
	return source.Remove(ctx, task)
}

// redisTasks reads the layout the old control plane wrote: one protojson task
// per key, a sorted set indexing every task as "atespace:name", and one sorted
// set per atespace indexing its names.
type redisTasks struct {
	client *goredis.Client
	prefix string
}

func (r redisTasks) taskKey(atespace, name string) string {
	return fmt.Sprintf("%s:task:%s:%s", r.prefix, atespace, name)
}

func (r redisTasks) List(ctx context.Context) ([]*v1alpha1.Task, error) {
	members, err := r.client.ZRange(ctx, r.prefix+":tasks:index", 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("reading the task index: %w", err)
	}
	var tasks []*v1alpha1.Task
	for _, member := range members {
		atespace, name, ok := strings.Cut(member, ":")
		if !ok {
			atespace, name = v1alpha1.DefaultAtespace, member
		}
		raw, err := r.client.Get(ctx, r.taskKey(atespace, name)).Result()
		if err == goredis.Nil {
			slog.Warn("task is indexed but has no record; skipping", "task", member)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading task %s: %w", member, err)
		}
		var task v1alpha1.Task
		unmarshal := protojson.UnmarshalOptions{DiscardUnknown: true}
		if err := unmarshal.Unmarshal([]byte(raw), &task); err != nil {
			return nil, fmt.Errorf("decoding task %s: %w", member, err)
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (r redisTasks) Remove(ctx context.Context, task *v1alpha1.Task) error {
	atespace, name := task.GetMetadata().GetAtespace(), task.GetMetadata().GetName()
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, r.taskKey(atespace, name))
	pipe.ZRem(ctx, r.prefix+":tasks:index", atespace+":"+name)
	pipe.ZRem(ctx, r.prefix+":tasks:atespace:"+atespace, name)
	_, err := pipe.Exec(ctx)
	return err
}
