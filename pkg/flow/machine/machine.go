// Package machine holds the temporal flow of the Machine system
// resource: its definition on the temporal-entity chassis and the
// lifecycle code (init, reconcile, finalize). The user-facing types live
// in the pipeline root package; side effects sit behind Ops, implemented
// by the graphene server, which registers this definition on its worker.
package machine

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Kind is the entity kind name; workflow IDs are "machine/{machine-id}".
const Kind = entity.KindName("machine")

// State extends the shared observable state with teardown data the
// finalizer needs (it sees only State — temporal-entity limitation,
// candidate for a chassis change).
type State struct {
	pipeline.MachineState
	ConnectedAt time.Time             `json:"connectedAt,omitempty"`
	Cloud       *pipeline.CloudSource `json:"cloud,omitempty"`
}

// Ops is the side-effect boundary of the machine flow: clouds and the
// agent registry. Implemented by the server; every method idempotent.
type Ops interface {
	// CreateCloud creates (or finds by machine id) the machine in the
	// cloud and returns its addresses.
	CreateCloud(machineId id.MachineId, src pipeline.CloudSource) ([]string, error)
	// DestroyCloud destroys the machine; not-found is not an error.
	DestroyCloud(machineId id.MachineId, src pipeline.CloudSource) error
	// AgentStatus reports whether the agent of the machine is currently
	// connected and, if so, the digest of its reported facts.
	AgentStatus(machineId id.MachineId) (connected bool, factsDigest string, err error)
}

// Activity names (registered by the server against its Ops).
const (
	CreateCloudActivity  = "machine.create-cloud"
	DestroyCloudActivity = "machine.destroy-cloud"
	AgentStatusActivity  = "machine.agent-status"
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
	return entdefine.New[pipeline.MachineSpec, State](Kind,
		entdefine.WithInit[pipeline.MachineSpec, State](func(ctx workflow.Context, spec pipeline.MachineSpec) (State, error) {
			return initMachine(ctx, opts, spec)
		}),
		entdefine.WithFinalize[pipeline.MachineSpec, State](finalizeMachine),
		entdefine.WithReconcileEvery[pipeline.MachineSpec, State](opts.ReconcileEvery, reconcileMachine),
		entdefine.WithSearchAttributes[pipeline.MachineSpec, State](true),
	)
}

type agentStatus struct {
	Connected   bool   `json:"connected"`
	FactsDigest string `json:"factsDigest"`
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

	if spec.Cloud != nil {
		if err := workflow.ExecuteActivity(actx, CreateCloudActivity, mid, *spec.Cloud).Get(ctx, &st.Addresses); err != nil {
			return st, fmt.Errorf("create in cloud: %w", err)
		}
		st.Cloud = spec.Cloud
	}

	// Readiness for both sources: the agent has connected.
	deadline := workflow.Now(ctx).Add(opts.ConnectTimeout)
	for workflow.Now(ctx).Before(deadline) {
		var status agentStatus
		if err := workflow.ExecuteActivity(actx, AgentStatusActivity, mid).Get(ctx, &status); err != nil {
			return st, fmt.Errorf("agent status: %w", err)
		}
		if status.Connected {
			st.AgentConnected = true
			st.ConnectedAt = workflow.Now(ctx)
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
	var status agentStatus
	if err := workflow.ExecuteActivity(activityCtx(ctx), AgentStatusActivity, machineId(ctx)).Get(ctx, &status); err != nil {
		return err
	}
	st := ec.State()
	st.AgentConnected = status.Connected
	if status.Connected {
		st.FactsDigest = status.FactsDigest
	}
	return nil
}

func finalizeMachine(ctx workflow.Context, st *State) error {
	// Recognized machines are not ours to destroy: no cloud source in
	// state — deleting the record leaves the machine untouched.
	if st.Cloud == nil {
		return nil
	}
	return workflow.ExecuteActivity(activityCtx(ctx), DestroyCloudActivity, machineId(ctx), *st.Cloud).Get(ctx, nil)
}
