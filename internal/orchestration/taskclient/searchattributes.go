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
	"fmt"
	"sort"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"

	"github.com/google/ax/internal/orchestration/workflows"
)

// requiredSearchAttributes are the attributes a task publishes about itself,
// with the type each has to be registered as.
var requiredSearchAttributes = map[string]enumspb.IndexedValueType{
	workflows.AtespaceSearchAttribute:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	workflows.PhaseSearchAttribute:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	workflows.GatewaySearchAttribute:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	workflows.WorkspacesSearchAttribute: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
}

// VerifySearchAttributes checks that the namespace has every attribute a task
// publishes, registered with the type the task writes. Upserting an attribute
// the namespace does not have fails the workflow task rather than the call, so
// a worker would retry every task forever without saying why. Checking once at
// startup turns that into one message naming the command that fixes it.
func VerifySearchAttributes(
	ctx context.Context, operator operatorservice.OperatorServiceClient, namespace string,
) error {
	resp, err := operator.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{
		Namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("listing the search attributes of namespace %q: %w", namespace, err)
	}
	registered := resp.GetCustomAttributes()

	var problems []string
	for _, name := range sortedAttributeNames() {
		want := requiredSearchAttributes[name]
		got, ok := registered[name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s is missing", name))
		case got != want:
			problems = append(problems, fmt.Sprintf("%s is %s, not %s", name, got, want))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("namespace %q cannot list tasks: %s; register the attributes with:\n  %s",
		namespace, strings.Join(problems, ", "), RegisterSearchAttributesCommand(namespace))
}

// RegisterSearchAttributesCommand is the CLI command that registers the
// attributes a task publishes, for the message an operator sees when they are
// not there.
func RegisterSearchAttributesCommand(namespace string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "temporal operator search-attribute create --namespace %s", namespace)
	for _, name := range sortedAttributeNames() {
		fmt.Fprintf(&b, " --name %s --type %s", name, cliTypeName(requiredSearchAttributes[name]))
	}
	return b.String()
}

// cliTypeName renders an attribute type the way the CLI's --type flag spells it.
func cliTypeName(t enumspb.IndexedValueType) string {
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return "KeywordList"
	default:
		return strings.TrimPrefix(t.String(), "INDEXED_VALUE_TYPE_")
	}
}

func sortedAttributeNames() []string {
	names := make([]string, 0, len(requiredSearchAttributes))
	for name := range requiredSearchAttributes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
