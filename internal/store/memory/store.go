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

package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// clone returns a deep copy of a protobuf message so stored resources are never
// shared with callers. Copying the struct by value is unsafe: generated messages
// embed internal state, including a mutex.
func clone[T proto.Message](m T) T {
	return proto.Clone(m).(T)
}

// MemoryStore is an in-memory implementation of store.Store for tests and
// single-node setups.
type MemoryStore struct {
	mu         sync.RWMutex
	gateways   map[string]*v1alpha1.Gateway
	models     map[string]*v1alpha1.Model
	workspaces map[string]*v1alpha1.Workspace
}

// NewStore creates a new in-memory Store.
func NewStore() *MemoryStore {
	return &MemoryStore{
		gateways:   make(map[string]*v1alpha1.Gateway),
		models:     make(map[string]*v1alpha1.Model),
		workspaces: make(map[string]*v1alpha1.Workspace),
	}
}

// key addresses one resource of a kind.
func key(atespace, name string) string {
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return fmt.Sprintf("%s:%s", atespace, name)
}

func (s *MemoryStore) SaveGateway(ctx context.Context, gw *v1alpha1.Gateway) error {
	if gw.Metadata.Name == "" {
		return errors.New("gateway name is required")
	}
	if gw.Metadata.Atespace == "" {
		gw.Metadata.Atespace = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(gw)
	s.gateways[key(gw.Metadata.Atespace, gw.Metadata.Name)] = cp
	return nil
}

func (s *MemoryStore) GetGateway(ctx context.Context, atespace, name string) (*v1alpha1.Gateway, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gw, ok := s.gateways[key(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(gw)
	return cp, nil
}

func (s *MemoryStore) ListGateways(ctx context.Context, atespace string) ([]*v1alpha1.Gateway, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Gateway
	for _, g := range s.gateways {
		if atespace == "" || atespace == "*" || g.Metadata.Atespace == atespace {
			cp := clone(g)
			result = append(result, cp)
		}
	}
	return result, nil
}

func (s *MemoryStore) DeleteGateway(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.gateways, key(atespace, name))
	return nil
}

func (s *MemoryStore) SaveModel(ctx context.Context, model *v1alpha1.Model) error {
	if model.Metadata.Name == "" {
		return errors.New("model name is required")
	}
	if model.Metadata.Atespace == "" {
		model.Metadata.Atespace = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(model)
	s.models[key(model.Metadata.Atespace, model.Metadata.Name)] = cp
	return nil
}

func (s *MemoryStore) GetModel(ctx context.Context, atespace, name string) (*v1alpha1.Model, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m, ok := s.models[key(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(m)
	return cp, nil
}

func (s *MemoryStore) ListModels(ctx context.Context, atespace string) ([]*v1alpha1.Model, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Model
	for _, m := range s.models {
		if atespace == "" || atespace == "*" || m.Metadata.Atespace == atespace {
			cp := clone(m)
			result = append(result, cp)
		}
	}
	return result, nil
}

func (s *MemoryStore) DeleteModel(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.models, key(atespace, name))
	return nil
}

func (s *MemoryStore) SaveWorkspace(ctx context.Context, ws *v1alpha1.Workspace) error {
	if ws.Metadata.Name == "" {
		return errors.New("workspace name is required")
	}
	if ws.Metadata.Atespace == "" {
		ws.Metadata.Atespace = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(ws)
	s.workspaces[key(ws.Metadata.Atespace, ws.Metadata.Name)] = cp
	return nil
}

func (s *MemoryStore) GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ws, ok := s.workspaces[key(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(ws)
	return cp, nil
}

func (s *MemoryStore) ListWorkspaces(ctx context.Context, atespace string) ([]*v1alpha1.Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Workspace
	for _, w := range s.workspaces {
		if atespace == "" || atespace == "*" || w.Metadata.Atespace == atespace {
			cp := clone(w)
			result = append(result, cp)
		}
	}
	return result, nil
}

func (s *MemoryStore) DeleteWorkspace(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.workspaces, key(atespace, name))
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
