// Package wire holds conventions shared between graphene components (the
// server, the pipeline library, and the agent): queue names and search
// attribute keys. Pure functions over identifiers — no behavior.
package wire

import (
	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// ServerQueue is the task queue of the graphene server worker: system
// entity workflows and their activities run here.
const ServerQueue = "graphene-server"

// MachineRunQueue names the task queue served by the user-code container
// hosted on a machine for one run. The container is scoped to
// (machine × run) and owned by the run: its version is the run's image
// version, and the run's end tears it down.
func MachineRunQueue(m id.MachineId, r id.RunId) string {
	return "machine/" + string(m) + "/run/" + string(r)
}

// RunQueue names the task queue of a run's main worker (managed container
// or inplace binary): the run workflow and machine-independent activities.
func RunQueue(r id.RunId) string {
	return "run/" + string(r)
}

// Server activity names: the contract between the pipeline library and
// the server worker (the server implements these on ServerQueue).
const (
	// DeclareMachineActivity declares a machine record and waits for
	// readiness (agent connected), heartbeating while converging.
	DeclareMachineActivity = "server.machine.declare"
	// DeclareArtifactActivity declares an artifact record about stored
	// bytes and verifies them.
	DeclareArtifactActivity = "server.artifact.declare"
	// DeleteResourceActivity deletes a resource by owner reference.
	DeleteResourceActivity = "server.resource.delete"
	// AgentUserDataActivity returns the agent install script for a fresh
	// machine's user-data (the same bytes the ssh install runs).
	AgentUserDataActivity = "server.machine.user-data"
)

// Environment variable names read by pipeline.Main to learn its role and
// wiring; the server and the agent set them when launching worker
// containers.
const (
	// EnvRole selects the worker role: "run" (default) or "machine".
	EnvRole = "GRAPHENE_ROLE"
	// EnvAddress is the Temporal frontend address handed to the worker.
	EnvAddress = "GRAPHENE_ADDRESS"
	// EnvRunId is the run this worker serves.
	EnvRunId = "GRAPHENE_RUN_ID"
	// EnvMachineId is set for the machine role: the machine this
	// container runs on.
	EnvMachineId = "GRAPHENE_MACHINE_ID"
)

// Search attribute keys used across the system in addition to the ones
// registered by temporal-entity (EntityKind, EntityPhase).
var (
	// SearchAttrOwner carries the OwnerRef of an entity for listing and
	// cascade queries ("find everything owned by run/X").
	SearchAttrOwner = temporal.NewSearchAttributeKeyKeyword("EntityOwner")
)
