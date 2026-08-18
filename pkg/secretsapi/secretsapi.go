// Package secretsapi resolves secret references AT THE POINT OF USE:
// inside activity code, against the server's worker plane, with the
// worker's own token. The value never travels back into workflow
// history — use it and let it go.
package secretsapi

import (
	"context"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/workerapi"
)

// Resolve fetches the secret's value. Call it only from activity code —
// resolving a secret is a side effect.
func Resolve(ctx context.Context, s ref.SecretRef) (string, error) {
	if err := s.Name.Validate(); err != nil {
		return "", err
	}
	return workerapi.GetSecret(ctx, string(s.Name))
}
