// Package secretsapi resolves secret references AT THE POINT OF USE:
// inside activity code, against the server's secret API, with the
// worker's own token. The value never travels back into workflow
// history — use it and let it go.
package secretsapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Resolve fetches the secret's value. Call it only from activity code —
// resolving a secret is a side effect.
func Resolve(ctx context.Context, s ref.SecretRef) (string, error) {
	if err := s.Name.Validate(); err != nil {
		return "", err
	}
	base := os.Getenv(wire.EnvHTTP)
	if base == "" {
		return "", fmt.Errorf("%s is not set", wire.EnvHTTP)
	}
	//nolint:gosec // the base URL is the installation's own server from the env — the only door
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/secrets/"+string(s.Name), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv(wire.EnvToken))
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // see request construction above
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("secret %q: %s: %s", s.Name, resp.Status, raw)
	}
	return string(raw), nil
}
