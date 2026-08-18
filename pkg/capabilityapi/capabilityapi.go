// Package capabilityapi publishes capabilities from ACTIVITY code — the
// way libraries record what they just installed, right where the
// installation happened. Workflow code uses pipeline.PublishCapability
// instead.
package capabilityapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Publish writes a capability onto a machine's record through the
// server API.
func Publish(ctx context.Context, machineId id.MachineId, capability pipeline.Capability) error {
	base := os.Getenv(wire.EnvHTTP)
	if base == "" {
		return fmt.Errorf("%s is not set", wire.EnvHTTP)
	}
	raw, err := json.Marshal(capability)
	if err != nil {
		return err
	}
	//nolint:gosec // the base URL is the installation's own server from the env — the only door
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		base+"/api/v1/machines/"+string(machineId)+"/capabilities/"+capability.Name, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv(wire.EnvToken))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // see request construction above
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("publish capability %q: %s: %s", capability.Name, resp.Status, body)
	}
	return nil
}

// PublishSelf publishes onto the machine THIS code runs on — for
// activity bodies inside the per-(agent × run) container, which knows
// its machine from the environment.
func PublishSelf(ctx context.Context, capability pipeline.Capability) error {
	machineId, err := id.ParseMachineId(os.Getenv(wire.EnvMachineId))
	if err != nil {
		return fmt.Errorf("%s: %w", wire.EnvMachineId, err)
	}
	return Publish(ctx, machineId, capability)
}
