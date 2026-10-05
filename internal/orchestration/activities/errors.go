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

package activities

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error types an activity can return. The retry policies list the permanent
// ones so that a task fails fast on a spec a human has to fix, and keeps
// retrying anything that a cluster can recover from on its own.
const (
	// ErrTypeInvalidSpec marks input that no retry can fix.
	ErrTypeInvalidSpec = "InvalidSpec"
	// ErrTypePermanent marks a Substrate rejection that no retry can fix, such
	// as a malformed request or a denied identity.
	ErrTypePermanent = "PermanentSubstrateError"
	// ErrTypeSubstrate marks a Substrate failure that is worth retrying.
	ErrTypeSubstrate = "SubstrateError"
)

// invalidSpec reports input an activity cannot act on.
func invalidSpec(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...),
		ErrTypeInvalidSpec, nil)
}

// classifyTemplateDeletion is classify with one exception. Substrate refuses to
// delete an actor template while an actor still derives from it, and that
// refusal clears as soon as the actor is gone, so it is worth retrying even
// though the same code means something permanent elsewhere.
func classifyTemplateDeletion(err error) error {
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.Aborted:
		return temporal.NewApplicationErrorWithCause(err.Error(), ErrTypeSubstrate, err)
	}
	return classify(err)
}

// classify turns a Substrate error into a Temporal error whose type says
// whether retrying can help.
//
// Unauthenticated is treated as retryable on purpose: the worker's bearer token
// is a projected service account token that is rotated in place, so a rejected
// call usually succeeds on the next attempt.
func classify(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument,
		codes.PermissionDenied,
		codes.NotFound,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented:
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypePermanent, err)
	default:
		return temporal.NewApplicationErrorWithCause(err.Error(), ErrTypeSubstrate, err)
	}
}
