package pipeline

import (
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Resource is the handle primitive: declaring a resource returns one
// immediately, without blocking. Outputs are reachable only through
// Ready — the first read blocks until the resource converges, so an
// unready resource cannot be used by construction. This is Temporal's own
// Future model lifted to the resource level; declaring several resources
// in a row runs their creation in parallel with no explicit concurrency.
type Resource[Out any] struct {
	fut workflow.Future
}

// Ready blocks until the resource is ready and returns its outputs.
// Subsequent calls return the same outcome.
func (r Resource[Out]) Ready(ctx workflow.Context) (Out, error) {
	var out Out
	err := r.fut.Get(ctx, &out)
	return out, err
}

// failed builds a handle already resolved to an error (spec validation).
func failed[Out any](ctx workflow.Context, err error) Resource[Out] {
	fut, set := workflow.NewFuture(ctx)
	set.SetError(err)
	return Resource[Out]{fut: fut}
}

// MachineHandle is the machine resource handle; Ready means the agent of
// the machine has connected.
type MachineHandle = Resource[MachineState]

// Machine declares a machine record and returns its handle without
// blocking. The record is a link between a real machine — created by
// whatever the user chose — and its agent; Ready when the agent connects.
// The owner defaults to the current run.
func Machine(ctx workflow.Context, machineId id.MachineId, spec MachineSpec) MachineHandle {
	if err := spec.Validate(); err != nil {
		return failed[MachineState](ctx, err)
	}
	if spec.Owner == "" {
		spec.Owner = ref.RunOwner(RunId(ctx))
	}
	return MachineHandle{fut: workflow.ExecuteActivity(serverCtx(ctx), wire.DeclareMachineActivity, machineId, spec)}
}

// MachineViaSSH declares a machine record that first installs the agent
// over ssh — the only case where the record acts on the machine.
func MachineViaSSH(ctx workflow.Context, machineId id.MachineId, install SSHInstall) MachineHandle {
	return Machine(ctx, machineId, MachineSpec{SSH: &install})
}

// AgentUserData returns the user-data script that installs the agent on a
// fresh VM and points it at this installation under the given machine id.
// Feed it to whatever creates the machine (a crossplane resource, a cloud
// console): one script for both paths — ssh install runs the same bytes —
// because two scripts would drift.
func AgentUserData(ctx workflow.Context, machineId id.MachineId) (string, error) {
	var script string
	err := workflow.ExecuteActivity(serverCtx(ctx), wire.AgentUserDataActivity, machineId).Get(ctx, &script)
	return script, err
}

// ArtifactHandle is the artifact resource handle.
type ArtifactHandle = Resource[ArtifactState]

// Artifact declares an artifact record about bytes the code has already
// stored and returns its handle without blocking. The owner defaults to
// the current run.
func Artifact(ctx workflow.Context, artifactId id.ArtifactId, spec ArtifactSpec) ArtifactHandle {
	if err := spec.Validate(); err != nil {
		return failed[ArtifactState](ctx, err)
	}
	if spec.Owner == "" {
		spec.Owner = ref.RunOwner(RunId(ctx))
	}
	return ArtifactHandle{fut: workflow.ExecuteActivity(serverCtx(ctx), wire.DeclareArtifactActivity, artifactId, spec)}
}
