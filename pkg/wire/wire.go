// Package wire holds conventions shared between graphene components (the
// server, the pipeline library, and the agent): queue names and search
// attribute keys. Pure functions over identifiers — no behavior.
package wire

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// ServerQueue is the task queue of the graphene server worker: system
// entity workflows and their activities run here.
const ServerQueue = "graphene-server"

// AgentRunQueue names the task queue served by the user-code container
// hosted by the agent for one run. The container is scoped to
// (machine × run) and owned by the run: its version is the run's image
// version, and the run's end tears it down.
func AgentRunQueue(m id.AgentId, r id.RunId) string {
	return "agent/" + string(m) + "/run/" + string(r)
}

// RunQueue names the task queue of a run's main worker (managed container
// or inplace binary): the run workflow and machine-independent activities.
func RunQueue(r id.RunId) string {
	return "run/" + string(r)
}

// Server activity names: the contract between the pipeline library and
// the server worker (the server implements these on ServerQueue).
const (
	// DeclareAgentActivity declares a machine record and waits for
	// readiness (agent connected), heartbeating while converging.
	DeclareAgentActivity = "server.agent.declare"
	// DeclareArtifactActivity declares an artifact record about stored
	// bytes and verifies them.
	DeclareArtifactActivity = "server.artifact.declare"
	// DeleteResourceActivity deletes a resource by owner reference.
	DeleteResourceActivity = "server.resource.delete"
	// AgentUserDataActivity returns the agent install script for a fresh
	// machine's user-data (the same bytes the ssh install runs).
	AgentUserDataActivity = "server.agent.user-data"
	// EnsureContainerActivity brings the per-(machine × run) container up
	// on the machine's agent: the first touch of a machine by a run pays
	// it, later touches are no-ops. Idempotent.
	EnsureContainerActivity = "server.container.ensure"
	// RunCleanupActivity is the guaranteed teardown of a finished run:
	// delete everything the run owns, stop its machine containers. The
	// run worker calls it on every exit path of the run workflow.
	RunCleanupActivity = "server.run.cleanup"

	// TransferResourceActivity moves a resource (with its subtree) under
	// a new owner. Ownership is given away, never taken: the caller must
	// be the current owner's side.
	TransferResourceActivity = "server.resource.transfer"
	// AttachAgentActivity waits for an EXISTING machine record to be
	// ready and returns its state — recognition, never creation: a
	// missing record is an error.
	AttachAgentActivity = "server.agent.attach"
	// AttachArtifactActivity is the same recognition for artifacts.
	AttachArtifactActivity = "server.artifact.attach"
	// SelectAgentsActivity lists the machine ids matching a selector
	// (record labels + capability needs) — a snapshot at call time.
	SelectAgentsActivity = "server.agents.select"
	// PublishCapabilityActivity writes a capability onto a machine's
	// record.
	PublishCapabilityActivity = "server.capability.publish"

	// StartChildRunActivity starts (or attaches to) a CHILD run owned by
	// the calling run: the server raises the child's managed container on
	// the child pipeline's own active image and stamps the child's
	// EntityOwner to the caller run — so the parent's tree, cancel cascade
	// and teardown reach it. Idempotent by child run id (USE_EXISTING):
	// a parent replay re-attaches to the live child, never forks a second.
	// The parent is taken from the activity's own workflow id, never
	// trusted from the request.
	StartChildRunActivity = "server.run.start-child"
	// AwaitChildRunActivity blocks until a child run reaches a terminal
	// state and returns its typed result as JSON (or an error for a
	// failed/cancelled child), heartbeating so a worker restart re-attaches
	// instead of re-running. The caller must own the child.
	AwaitChildRunActivity = "server.run.await-child"
)

// StartChildRunRequest asks the server to start a child run under the
// calling run. The parent (owner) is derived server-side from the
// activity's workflow id, not from this request.
type StartChildRunRequest struct {
	// RunId is the child's run id — deterministic ("<parent>-<cell>") so a
	// parent replay attaches to the same child.
	RunId string `json:"runId"`
	// Pipeline is the child's pipeline id; its ACTIVE image is used.
	Pipeline string `json:"pipeline"`
	// Params is the child pipeline's typed params, as JSON.
	Params json.RawMessage `json:"params,omitempty"`
	// Labels are user labels for the child run.
	Labels map[string]string `json:"labels,omitempty"`
}

// AwaitChildRunRequest names the child run to wait for.
type AwaitChildRunRequest struct {
	RunId string `json:"runId"`
}

// NeedSpec is one capability requirement: the capability must exist,
// be ready, and match the label constraints — equality and In only,
// k8s-selector semantics; the system never interprets values.
type NeedSpec struct {
	Name        string              `json:"name"`
	MatchLabels map[string]string   `json:"matchLabels,omitempty"`
	In          map[string][]string `json:"in,omitempty"`
}

// LabelPrefixSystem is the reserved label namespace: keys under it are
// written by the system, never by user code.
const LabelPrefixSystem = "graphene.io/"

// LabelRun marks the run that created a record — created-by, stable
// across ownership transfers (unlike EntityOwner).
const LabelRun = "graphene.io/run"

// ValidateUserLabels rejects labels a user may not set.
func ValidateUserLabels(labels map[string]string) error {
	for k := range labels {
		if strings.HasPrefix(k, LabelPrefixSystem) {
			return fmt.Errorf("label %q: the %q prefix is reserved for the system", k, LabelPrefixSystem)
		}
	}
	return nil
}

// AgentSelector picks agents by record labels and capability needs.
type AgentSelector struct {
	Labels map[string]string `json:"labels,omitempty"`
	Needs  []NeedSpec        `json:"needs,omitempty"`
}

// TransferResourceRequest asks the server to reparent a resource.
type TransferResourceRequest struct {
	Resource ref.OwnerRef `json:"resource"`
	NewOwner ref.OwnerRef `json:"newOwner"`
	// Keep bounds the stay under the new owner (stand TTL); zero keeps
	// the resource until an explicit delete.
	Keep time.Duration `json:"keep,omitempty"`
	// From names who hands the resource over (the run's workflow id
	// when the transfer comes from a run). The stand will not tear a
	// holding down from under its still-running origin.
	From string `json:"from,omitempty"`
}

// StandOwner is the owner reference of a pipeline's Stand — the
// permanent owner every pipeline has.
func StandOwner(p id.PipelineId) ref.OwnerRef {
	return ref.OwnerRef("stand/" + string(p))
}

// RunCleanupRequest is the run's last word: which run ended and HOW.
// The outcome is what a downstream pipeline trigger fires on — the
// cross-pipeline edge needs to know success from failure.
type RunCleanupRequest struct {
	RunId id.RunId `json:"runId"`
	// Outcome: "success" | "failure" | "canceled".
	Outcome string `json:"outcome"`
}

// EnsureContainerRequest asks the server to bring the worker container
// of (machine × run) up on the machine's agent.
type EnsureContainerRequest struct {
	AgentId id.AgentId `json:"agentId"`
	RunId   id.RunId   `json:"runId"`
	// Image is the run's own worker image: the version of the run is the
	// version of every container it touches.
	Image string `json:"image"`
}

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
	// EnvAgentId is set for the machine role: the machine this
	// container runs on.
	EnvAgentId = "GRAPHENE_AGENT_ID"
	// EnvImage is the worker's own image ref — handed to agents when the
	// run first touches a machine, so the container matches the run's
	// version.
	EnvImage = "GRAPHENE_IMAGE"
	// EnvToken is the run-scoped token; the Temporal connection goes
	// through the server's gRPC proxy, which authenticates it.
	EnvToken = "GRAPHENE_TOKEN" //nolint:gosec // the env var NAME, not a credential
	// EnvInsecure ("1"/"true") disables TLS towards the server — dev
	// contours only.
	EnvInsecure = "GRAPHENE_INSECURE"
	// EnvNamespace is the worker's namespace — symmetric to the Temporal
	// namespace it runs in; default "default".
	EnvNamespace = "GRAPHENE_NAMESPACE"
)

// Search attribute keys used across the system in addition to the ones
// registered by temporal-entity (EntityKind, EntityPhase).
var (
	// SearchAttrOwner carries the CURRENT OwnerRef of an entity — flows
	// upsert it on init and on transfer; the cascade is one visibility
	// query ("find everything owned by run/X").
	SearchAttrOwner = temporal.NewSearchAttributeKeyKeyword("EntityOwner")
	// SearchAttrKeepUntil carries the stand-TTL deadline; the server's
	// sweeper deletes what expired.
	SearchAttrKeepUntil = temporal.NewSearchAttributeKeyTime("EntityKeepUntil")
)

// TransferOwnerCmdName is the entity command every OWNED system resource
// serves: give the resource (and so its subtree) to a new owner.
const TransferOwnerCmdName = "transfer-owner"
