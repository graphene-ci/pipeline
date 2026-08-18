// Package capabilityapi publishes capabilities from ACTIVITY code — the
// way libraries record what they just installed, right where the
// installation happened. Workflow code uses pipeline.PublishCapability
// instead.
package capabilityapi

import (
	"context"
	"fmt"
	"os"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/pipeline/pkg/workerapi"
)

// Publish writes a capability onto a machine's record through the
// worker plane.
func Publish(ctx context.Context, agentId id.AgentId, capability pipeline.Capability) error {
	return workerapi.PublishCapability(ctx, string(agentId), &workerplanev1.Capability{
		Name:      capability.Name,
		Version:   capability.Version,
		Labels:    capability.Labels,
		BroughtBy: capability.BroughtBy,
		Ready:     capability.Ready,
	})
}

// PublishSelf publishes onto the machine THIS code runs on — for
// activity bodies inside the per-(agent × run) container, which knows
// its machine from the environment.
func PublishSelf(ctx context.Context, capability pipeline.Capability) error {
	agentId, err := id.ParseAgentId(os.Getenv(wire.EnvAgentId))
	if err != nil {
		return fmt.Errorf("%s: %w", wire.EnvAgentId, err)
	}
	return Publish(ctx, agentId, capability)
}
