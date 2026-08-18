// Package machine holds the temporal flow of the Machine system
// resource. A machine record is a LINK between a real machine — created
// by whatever the user chose (crossplane through a resource library, a
// person, a cloud console) — and its agent. The record never creates
// machines; the only case where it acts is the ssh install: put the agent
// on a machine that already exists. Readiness in every case means the
// agent has connected.
//
// The user-facing types live in the pipeline root package; side effects
// sit behind Ops, implemented by the graphene server, which registers
// this definition on its worker.
package machine

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Kind is the entity kind name; workflow IDs are "machine/{machine-id}".
const Kind = entity.KindName("machine")

// State extends the shared observable state with flow-internal fields.
type State struct {
	pipeline.MachineState
	ConnectedAt time.Time `json:"connectedAt,omitempty"`
}

// Ops is the side-effect boundary of the machine flow. Implemented by the
// server; every method idempotent.
type Ops interface {
	// InstallSSH goes to the existing machine over ssh and runs the agent
	// install script — the same bytes a fresh VM gets through user-data.
	InstallSSH(machineId id.MachineId, install pipeline.SSHInstall) error
	// AgentStatus reports whether the agent of the machine is currently
	// connected and, if so, its addresses and the digest of its facts.
	AgentStatus(machineId id.MachineId) (AgentStatus, error)
}

// AgentStatus is what the agent registry reports about a machine.
type AgentStatus struct {
	Connected   bool     `json:"connected"`
	Addresses   []string `json:"addresses,omitempty"`
	FactsDigest string   `json:"factsDigest,omitempty"`
}

// Activity names (registered by the server against its Ops).
const (
	InstallSSHActivity  = "machine.install-ssh"
	AgentStatusActivity = "machine.agent-status"
)

// Options tune the machine flow.
type Options struct {
	// ConnectTimeout bounds waiting for the agent during creation.
	ConnectTimeout time.Duration
	// ReconcileEvery is the health-check period.
	ReconcileEvery time.Duration
	// PollInterval is the agent-status polling period during creation.
	PollInterval time.Duration
}

func (o *Options) defaults() {
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 10 * time.Minute
	}
	if o.ReconcileEvery == 0 {
		o.ReconcileEvery = 30 * time.Second
	}
	if o.PollInterval == 0 {
		o.PollInterval = 5 * time.Second
	}
}

// Definition builds the machine entity definition. The server registers
// it on its worker together with activities implementing Ops.
func Definition(opts Options) *entdefine.Definition[pipeline.MachineSpec, State] {
	opts.defaults()
	def := entdefine.New[pipeline.MachineSpec, State](Kind,
		entdefine.WithInit[pipeline.MachineSpec, State](func(ctx workflow.Context, spec pipeline.MachineSpec) (State, error) {
			return initMachine(ctx, opts, spec)
		}),
		// No finalizer: the record owns no machine — it never created one.
		// Deleting the record leaves the real machine to whoever made it.
		entdefine.WithReconcileEvery[pipeline.MachineSpec, State](opts.ReconcileEvery, reconcileMachine),
		entdefine.WithSearchAttributes[pipeline.MachineSpec, State](true),
	)
	entdefine.Handle(def, publishCapability)
	return def
}

// PublishCapabilityCmd writes what the machine now CAN onto its record:
// a capability belongs to the machine, whoever published it.
type PublishCapabilityCmd struct {
	Capability pipeline.Capability `json:"capability"`
}

// Name is the command's wire identity.
func (PublishCapabilityCmd) Name() entity.CommandName { return "publish-capability" }

// Result binds the response type.
func (PublishCapabilityCmd) Result() PublishCapabilityRes { return PublishCapabilityRes{} }

// Validate rejects an unnamed capability before anything runs.
func (c PublishCapabilityCmd) Validate() error {
	if c.Capability.Name == "" {
		return fmt.Errorf("capability needs a name")
	}
	return nil
}

// PublishCapabilityRes reports the record's capability count.
type PublishCapabilityRes struct {
	Count int `json:"count"`
}

// publishCapability merges the capability by name — re-publishing
// replaces, the way SSA replaces.
func publishCapability(_ workflow.Context, ec *entdefine.Ctx[pipeline.MachineSpec, State], cmd PublishCapabilityCmd) (PublishCapabilityRes, error) {
	st := ec.State()
	for i, c := range st.Capabilities {
		if c.Name == cmd.Capability.Name {
			st.Capabilities[i] = cmd.Capability
			return PublishCapabilityRes{Count: len(st.Capabilities)}, nil
		}
	}
	st.Capabilities = append(st.Capabilities, cmd.Capability)
	return PublishCapabilityRes{Count: len(st.Capabilities)}, nil
}

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// machineId derives the machine id from the entity workflow ID
// ("machine/{id}").
func machineId(ctx workflow.Context) id.MachineId {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return id.MachineId(full[len(prefix):])
	}
	return id.MachineId(full)
}

func initMachine(ctx workflow.Context, opts Options, spec pipeline.MachineSpec) (State, error) {
	var st State
	mid := machineId(ctx)
	actx := activityCtx(ctx)

	// The only acting case: put the agent on an existing machine over ssh.
	if spec.SSH != nil {
		if err := workflow.ExecuteActivity(actx, InstallSSHActivity, mid, *spec.SSH).Get(ctx, nil); err != nil {
			return st, fmt.Errorf("ssh install: %w", err)
		}
	}

	// Readiness in every case: the agent has connected.
	deadline := workflow.Now(ctx).Add(opts.ConnectTimeout)
	for workflow.Now(ctx).Before(deadline) {
		var status AgentStatus
		if err := workflow.ExecuteActivity(actx, AgentStatusActivity, mid).Get(ctx, &status); err != nil {
			return st, fmt.Errorf("agent status: %w", err)
		}
		if status.Connected {
			st.AgentConnected = true
			st.ConnectedAt = workflow.Now(ctx)
			st.Addresses = status.Addresses
			st.FactsDigest = status.FactsDigest
			return st, nil
		}
		if err := workflow.Sleep(ctx, opts.PollInterval); err != nil {
			return st, err
		}
	}
	return st, fmt.Errorf("agent did not connect within %s", opts.ConnectTimeout)
}

func reconcileMachine(ctx workflow.Context, ec *entdefine.Ctx[pipeline.MachineSpec, State]) error {
	if ec.Phase() != entity.PhaseReady {
		return nil
	}
	var status AgentStatus
	if err := workflow.ExecuteActivity(activityCtx(ctx), AgentStatusActivity, machineId(ctx)).Get(ctx, &status); err != nil {
		return err
	}
	st := ec.State()
	st.AgentConnected = status.Connected
	if status.Connected {
		st.Addresses = status.Addresses
		st.FactsDigest = status.FactsDigest
	}
	return nil
}
