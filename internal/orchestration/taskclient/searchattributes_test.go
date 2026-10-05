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

package taskclient

import (
	"context"
	"errors"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"google.golang.org/grpc"

	"github.com/google/ax/internal/orchestration/workflows"
)

// fakeOperator answers ListSearchAttributes with what a namespace has. The
// rest of the operator service is left unimplemented on purpose.
type fakeOperator struct {
	operatorservice.OperatorServiceClient

	attributes map[string]enumspb.IndexedValueType
	err        error
	namespace  string
}

func (f *fakeOperator) ListSearchAttributes(
	ctx context.Context, in *operatorservice.ListSearchAttributesRequest, opts ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
	f.namespace = in.GetNamespace()
	if f.err != nil {
		return nil, f.err
	}
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: f.attributes}, nil
}

func registeredAttributes() map[string]enumspb.IndexedValueType {
	return map[string]enumspb.IndexedValueType{
		workflows.AtespaceSearchAttribute:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		workflows.PhaseSearchAttribute:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		workflows.WorkspacesSearchAttribute: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"SomethingElse":                     enumspb.INDEXED_VALUE_TYPE_TEXT,
	}
}

func TestVerifySearchAttributesAcceptsARegisteredNamespace(t *testing.T) {
	fake := &fakeOperator{attributes: registeredAttributes()}
	if err := VerifySearchAttributes(context.Background(), fake, "ax"); err != nil {
		t.Fatalf("expected a namespace with the attributes to pass, got %v", err)
	}
	if fake.namespace != "ax" {
		t.Errorf("expected the namespace to be asked about, got %q", fake.namespace)
	}
}

// A missing attribute fails the workflow task, not the upsert, so the message
// here is the one place an operator learns what to do.
func TestVerifySearchAttributesNamesWhatIsMissing(t *testing.T) {
	attributes := registeredAttributes()
	delete(attributes, workflows.PhaseSearchAttribute)
	attributes[workflows.WorkspacesSearchAttribute] = enumspb.INDEXED_VALUE_TYPE_KEYWORD

	err := VerifySearchAttributes(context.Background(), &fakeOperator{attributes: attributes}, "ax")
	if err == nil {
		t.Fatal("expected the namespace to be refused")
	}
	for _, want := range []string{
		"AxPhase is missing",
		"AxWorkspaces is " + enumspb.INDEXED_VALUE_TYPE_KEYWORD.String(),
		"temporal operator search-attribute create --namespace ax",
		"--name AxWorkspaces --type KeywordList",
		"--name AxAtespace --type Keyword",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected the message to say %q, got:\n%v", want, err)
		}
	}
}

func TestVerifySearchAttributesReportsAnUnreachableService(t *testing.T) {
	fake := &fakeOperator{err: errors.New("connection refused")}
	err := VerifySearchAttributes(context.Background(), fake, "ax")
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected the service error to be reported, got %v", err)
	}
}
