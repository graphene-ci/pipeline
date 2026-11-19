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
package agent

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Kind is the entity kind name; workflow IDs are "agent/{agent-id}".
const Kind = entity.KindName("agent")

// State extends the shared observable state with flow-internal fields
// and the owned half of the tree.
type State struct {
	pipeline.AgentState
	ownership.State
	ConnectedAt time.Time `json:"connectedAt,omitempty"`
	// Facts is the machine's inventory as the agent last reported it —
	// on the RECORD, so every reader gets it from the state instead of
	// asking the registry.
	Facts *MachineFacts `json:"facts,omitempty"`
	// Leaks are what records left on the machine when their removal gave
	// up. A machine with leaks is dirty until they are cleared.
	Leaks []Leak `json:"leaks,omitempty"`
	// LeaksOverflow counts reports that arrived while Leaks was full. The
	// listed leaks are never pushed out — they still need cleaning — and
	// the overflow keeps the machine dirty until every leak is cleared.
	LeaksOverflow int `json:"leaksOverflow,omitempty"`
}

// Leak is one leftover on the machine.
type Leak struct {
	// Ref is the record that could not clean up ("kind/id").
	Ref   string    `json:"ref"`
	Error string    `json:"error"`
	At    time.Time `json:"at"`
}

// Ops is the side-effect boundary of the machine flow. Implemented by the
// server; every method idempotent.
type Ops interface {
	// InstallSSH goes to the existing machine over ssh and runs the agent
	// install script — the same bytes a fresh VM gets through user-data.
	InstallSSH(agentId id.AgentId, install pipeline.SSHInstall) error
	// AgentStatus reports whether the agent of the machine is currently
	// connected and, if so, its addresses and the digest of its facts.
	AgentStatus(agentId id.AgentId) (AgentStatus, error)
}

// AgentStatus is what the agent registry reports about a machine.
type AgentStatus struct {
	Connected   bool     `json:"connected"`
	Addresses   []string `json:"addresses,omitempty"`
	FactsDigest string   `json:"factsDigest,omitempty"`
	// Facts is the full inventory behind the digest; the reconcile
	// writes it onto the record's state.
	Facts *MachineFacts `json:"facts,omitempty"`
}

// MachineFacts is the machine's inventory: what the box IS, from the
// agent's hello — hardware, distribution, and addresses WITH their
// interface names so a consumer filters by name, not by heuristics.
type MachineFacts struct {
	Hostname    string `json:"hostname,omitempty"`
	OS          string `json:"os,omitempty"`
	Arch        string `json:"arch,omitempty"`
	Cpus        int    `json:"cpus,omitempty"`
	MemoryBytes uint64 `json:"memoryBytes,omitempty"`
	// The distribution identity from /etc/os-release.
	OSReleaseId      string           `json:"osReleaseId,omitempty"`
	OSReleaseLike    string           `json:"osReleaseLike,omitempty"`
	OSReleaseVersion string           `json:"osReleaseVersion,omitempty"`
	Interfaces       []InterfaceAddrs `json:"interfaces,omitempty"`
	// Disks are the physical block devices: the machine's hardware key
	// and what a mount record names by id.
	Disks []Disk `json:"disks,omitempty"`
}

// Disk is one physical block device of the machine.
type Disk struct {
	// Name is the kernel name: "nvme0n1", "sda", "vdb".
	Name       string `json:"name"`
	SizeBytes  uint64 `json:"sizeBytes"`
	Rotational bool   `json:"rotational,omitempty"`
	// Transport is the bus: "nvme", "virtio", "scsi"...
	Transport string `json:"transport,omitempty"`
	Model     string `json:"model,omitempty"`
	Serial    string `json:"serial,omitempty"`
	// ById are the /dev/disk/by-id names of the whole disk — stable
	// across reboots and renumbering.
	ById []string `json:"byId,omitempty"`
}

// InterfaceAddrs is one interface and its addresses.
type InterfaceAddrs struct {
	Name      string   `json:"name"`
	Addresses []string `json:"addresses,omitempty"`
}

// Activity names (registered by the server against its Ops).
const (
	InstallSSHActivity  = "agent.install-ssh"
	AgentStatusActivity = "agent.status"
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
func Definition(opts Options) *entdefine.Definition[pipeline.AgentSpec, State] {
	opts.defaults()
	def := entdefine.New[pipeline.AgentSpec, State](Kind,
		entdefine.WithInit[pipeline.AgentSpec, State](func(ctx workflow.Context, spec pipeline.AgentSpec) (State, error) {
			return initMachine(ctx, opts, spec)
		}),
		// The record owns no machine — it never created one, and deleting
		// it leaves the real machine to whoever made it. The finalizer only
		// closes the record's own books.
		entdefine.WithFinalize[pipeline.AgentSpec, State](finalizeMachine),
		entdefine.WithReconcileEvery[pipeline.AgentSpec, State](opts.ReconcileEvery, reconcileMachine),
		entdefine.WithSearchAttributes[pipeline.AgentSpec, State](true),
	)
	entdefine.Handle(def, publishCapability)
	entdefine.Handle(def, reportLeak)
	entdefine.Handle(def, clearLeaks)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })
	return def
}

// ReportLeakCmd records a leftover on the machine; the same record
// reporting again replaces its entry.
type ReportLeakCmd struct {
	Ref   string `json:"ref"`
	Error string `json:"error"`
}

// Name is the command's wire identity.
func (ReportLeakCmd) Name() entity.CommandName { return "report-leak" }

// Result binds the response type.
func (ReportLeakCmd) Result() LeaksRes { return LeaksRes{} }

// Validate rejects a leak that names no record.
func (c ReportLeakCmd) Validate() error {
	if c.Ref == "" {
		return fmt.Errorf("leak needs the record that left it")
	}
	return nil
}

// ClearLeaksCmd says leftovers are gone: one record's, or all with an
// empty Ref. Whoever clears has cleaned the machine (or accepts it).
type ClearLeaksCmd struct {
	Ref string `json:"ref,omitempty"`
}

// Name is the command's wire identity.
func (ClearLeaksCmd) Name() entity.CommandName { return "clear-leaks" }

// Result binds the response type.
func (ClearLeaksCmd) Result() LeaksRes { return LeaksRes{} }

// LeaksRes reports the leaks left on the record, overflow included.
type LeaksRes struct {
	Count int `json:"count"`
}

// Leak bounds: the record's state rides every Continue-as-New, so what a
// caller sends is capped. Past MaxLeaks a report is counted, not listed.
const (
	MaxLeaks     = 100
	maxLeakRef   = 256
	maxLeakError = 1024
)

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func reportLeak(ctx workflow.Context, ec *entdefine.Ctx[pipeline.AgentSpec, State], cmd ReportLeakCmd) (LeaksRes, error) {
	st := ec.State()
	l := Leak{Ref: clip(cmd.Ref, maxLeakRef), Error: clip(cmd.Error, maxLeakError), At: workflow.Now(ctx)}
	for i, have := range st.Leaks {
		if have.Ref == l.Ref {
			st.Leaks[i] = l
			return LeaksRes{Count: len(st.Leaks)}, nil
		}
	}
	if len(st.Leaks) >= MaxLeaks {
		st.LeaksOverflow++
		return LeaksRes{Count: len(st.Leaks) + st.LeaksOverflow}, nil
	}
	st.Leaks = append(st.Leaks, l)
	return LeaksRes{Count: len(st.Leaks) + st.LeaksOverflow}, nil
}

func clearLeaks(_ workflow.Context, ec *entdefine.Ctx[pipeline.AgentSpec, State], cmd ClearLeaksCmd) (LeaksRes, error) {
	st := ec.State()
	if cmd.Ref == "" {
		st.Leaks = nil
		st.LeaksOverflow = 0
		return LeaksRes{}, nil
	}
	kept := st.Leaks[:0]
	found := false
	for _, l := range st.Leaks {
		if l.Ref == cmd.Ref {
			found = true
			continue
		}
		kept = append(kept, l)
	}
	if !found {
		return LeaksRes{Count: len(st.Leaks) + st.LeaksOverflow}, fmt.Errorf("no leak of %s on this machine", cmd.Ref)
	}
	st.Leaks = kept
	return LeaksRes{Count: len(st.Leaks) + st.LeaksOverflow}, nil
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
func publishCapability(_ workflow.Context, ec *entdefine.Ctx[pipeline.AgentSpec, State], cmd PublishCapabilityCmd) (PublishCapabilityRes, error) {
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

// agentId derives the machine id from the entity workflow ID
// ("machine/{id}").
func agentId(ctx workflow.Context) id.AgentId {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return id.AgentId(full[len(prefix):])
	}
	return id.AgentId(full)
}

// ServerNode is the topology's external node for the control plane —
// the far end of every agent's virtual edges.
const ServerNode = "graphene-server"

// virtualAgentFlows are the edges an agent ALWAYS has to the server:
// its telemetry (obs), the command channel that runs its work, and the
// interactive shell. They are not declared by the user — the system
// knows they exist — so the topology draws them for free.
func virtualAgentFlows() []ownership.Flow {
	return []ownership.Flow{
		{To: ServerNode, Protocol: ownership.OTLP, Label: "obs", Virtual: true},
		{To: ServerNode, Protocol: ownership.Command, Label: "commands", Virtual: true},
		{To: ServerNode, Protocol: ownership.TTY, Label: "shell", Virtual: true},
	}
}

// recordFlows are the record's edges: the ones the pipeline declared from
// this machine (WithFlow, WithFlowTo), then the virtual ones every agent
// has to the server.
func recordFlows(spec pipeline.AgentSpec) []ownership.Flow {
	return append(append([]ownership.Flow{}, spec.Flows...), virtualAgentFlows()...)
}

func initMachine(ctx workflow.Context, opts Options, spec pipeline.AgentSpec) (State, error) {
	var st State
	// Set BEFORE Init, which mirrors the edges into visibility.
	st.Flows = recordFlows(spec)
	// Set the owner at INIT, before the (possibly long) wait for the agent
	// to connect: a run that declares this agent owns it from the first
	// moment, so a run torn down while the machine is still coming up still
	// reaches the record in its cascade. Without this the record has no
	// owner until a later adopt lands — which it cannot, because an entity
	// runs commands only after init returns — leaving a "creating" agent
	// nobody owns. Only NewAgent (a machine the run brings up) sets an
	// owner; those records live and die with the run, so no long-lived
	// history replays this across a deploy.
	if spec.Owner != "" {
		ownership.Init(ctx, &st.State, spec.Owner)
	}
	mid := agentId(ctx)
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
			st.Facts = status.Facts
			return st, nil
		}
		if err := workflow.Sleep(ctx, opts.PollInterval); err != nil {
			return st, err
		}
	}
	return st, fmt.Errorf("agent did not connect within %s", opts.ConnectTimeout)
}

// finalizeMachine is the record's last word. A deleted record is still
// read by name, and "connected" there means "this record can take work":
// left at its last live value, a record that is gone would go on saying
// yes. The agent PROCESS may well still be connected — that is the
// machine's business; what it knew (addresses, facts, capabilities) stays
// as the record's history. No activity runs here: nothing on the machine
// is touched.
func finalizeMachine(_ workflow.Context, st *State) error {
	st.AgentConnected = false
	return nil
}

func reconcileMachine(ctx workflow.Context, ec *entdefine.Ctx[pipeline.AgentSpec, State]) error {
	if ec.Phase() != entity.PhaseReady {
		return nil
	}
	var status AgentStatus
	if err := workflow.ExecuteActivity(activityCtx(ctx), AgentStatusActivity, agentId(ctx)).Get(ctx, &status); err != nil {
		return err
	}
	st := ec.State()
	st.AgentConnected = status.Connected
	if status.Connected {
		st.Addresses = status.Addresses
		st.FactsDigest = status.FactsDigest
		st.Facts = status.Facts
	}
	return nil
}
