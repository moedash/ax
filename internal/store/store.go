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

package store

import (
	"context"
	"errors"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

var (
	ErrNotFound = errors.New("resource not found")
)

// Store holds the AX configuration kinds: the gateways, workspaces, and models
// that tasks bind. Tasks themselves are not here. A task's desired state and
// status live in the workflow that owns it, which is what makes a task's
// lifecycle recoverable.
type Store interface {
	SaveGateway(ctx context.Context, gw *v1alpha1.Gateway) error
	GetGateway(ctx context.Context, atespace, name string) (*v1alpha1.Gateway, error)
	ListGateways(ctx context.Context, atespace string) ([]*v1alpha1.Gateway, error)
	DeleteGateway(ctx context.Context, atespace, name string) error

	SaveWorkspace(ctx context.Context, ws *v1alpha1.Workspace) error
	GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error)
	ListWorkspaces(ctx context.Context, atespace string) ([]*v1alpha1.Workspace, error)
	DeleteWorkspace(ctx context.Context, atespace, name string) error

	SaveModel(ctx context.Context, model *v1alpha1.Model) error
	GetModel(ctx context.Context, atespace, name string) (*v1alpha1.Model, error)
	ListModels(ctx context.Context, atespace string) ([]*v1alpha1.Model, error)
	DeleteModel(ctx context.Context, atespace, name string) error

	Close() error
}
