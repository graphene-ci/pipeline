package pipeline

import (
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Agent is OUR resource on a machine: the record and identity of the
// process we run there. The machine itself (the real hardware) is a
// different resource we do not operate — the agent's identity goes into
// whatever creates or already runs the machine (cloud user-data, ssh
// install), and readiness means the agent has connected.
//

// Agent is the target surface of an agent — OURS or FOREIGN: consumers
// (activities, artifact sources, libraries) take this interface and
// never distinguish the two. The only difference lives outside it: an
// attached agent has no ResourceRef and cannot enter the ownership
// tree.
type Agent interface {
	AgentId() id.AgentId
}

// AgentHandle is the agent resource handle; Ready when the agent of the
// machine has connected. It is the target of activities.
type AgentHandle struct {
	Resource[AgentState]

	agentId  id.AgentId
	userData workflow.Future
	ctx      Context
}

// AgentId names the agent's record — the routing key of its activity
// queue.
func (a AgentHandle) AgentId() id.AgentId { return a.agentId }

// CloudInit returns the install script for the machine's user-data: the
// agent proves itself with what the machine IS — the identity its
// platform gives it — no long-lived secret leaks through metadata. The
// same bytes drive the ssh install; two scripts would drift.
func (a AgentHandle) CloudInit() string {
	if a.ctx.Recording() || a.userData == nil {
		return ""
	}
	var script string
	if err := a.userData.Get(a.ctx, &script); err != nil {
		panic(resourceFailure{err: err})
	}
	return script
}

// AttachedAgent is a FOREIGN agent: recognized, never created, never
// owned. Everything a consumer can do with an agent works on it.
type AttachedAgent struct {
	Attached[AgentState]
	agentId id.AgentId
}

// AgentId names the agent's record — the routing key of its activity
// queue.
func (a AttachedAgent) AgentId() id.AgentId { return a.agentId }

// AttachAgent recognizes an EXISTING agent created outside this run: a
// missing record is an error, never a creation. Ready waits for the
// agent AND for the Need requirements — the refusal comes before work
// is dispatched. Only Need options apply.
func AttachAgent(ctx Context, name string, opts ...ResourceOption) AttachedAgent {
	agentId := id.AgentId(name)
	h := AttachedAgent{agentId: agentId}
	if ctx.Recording() {
		ctx.RecordStep("attach", "agent/"+name, "", "foreign")
		h.Attached = NewAttached[AgentState](ctx, nil)
		return h
	}
	var o ResourceOptions
	for _, opt := range opts {
		opt(&o)
	}
	fut := workflow.ExecuteActivity(serverCtx(ctx), wire.AttachAgentActivity, agentId, o.Needs)
	h.Attached = NewAttached[AgentState](ctx, fut)
	return h
}

// SelectAgents picks the agents matching the selector — record labels
// plus capability needs — as a SNAPSHOT at call time. Foreign by
// construction: the selection takes no ownership.
func SelectAgents(ctx Context, opts ...ResourceOption) ([]Agent, error) {
	if ctx.Recording() {
		return nil, nil
	}
	var o ResourceOptions
	for _, opt := range opts {
		opt(&o)
	}
	sel := wire.AgentSelector{Labels: o.Labels, Needs: o.Needs}
	var ids []id.AgentId
	if err := workflow.ExecuteActivity(serverCtx(ctx), wire.SelectAgentsActivity, sel).Get(ctx, &ids); err != nil {
		return nil, err
	}
	out := make([]Agent, 0, len(ids))
	for _, agentId := range ids {
		out = append(out, AttachAgent(ctx, string(agentId)))
	}
	return out, nil
}

// PublishCapability writes what the machine now CAN onto its record.
// Libraries publish from their own activity bodies (capabilityapi);
// this is the workflow-side form for what a person or an image brought.
func PublishCapability(ctx Context, agent Agent, capability Capability) error {
	if ctx.Recording() {
		return nil
	}
	return workflow.ExecuteActivity(serverCtx(ctx), wire.PublishCapabilityActivity, agent.AgentId(), capability).Get(ctx, nil)
}

// NewAgent declares the agent record and returns its handle without
// blocking. The real machine comes from whatever the user chose — a
// crossplane resource with CloudInit in user-data, a person, an ssh
// install; Ready blocks until the agent connects.
func NewAgent(ctx Context, name string, opts ...ResourceOption) AgentHandle {
	agentId := id.AgentId(name)
	self := ref.OwnerRef("agent/" + name)
	h := AgentHandle{agentId: agentId, ctx: ctx}
	if ctx.Recording() {
		ctx.RecordDeclare(self, BuildResourceOptions(ctx, opts))
		h.Resource = NewResource[AgentState](ctx, self, nil)
		return h
	}
	o := BuildResourceOptions(ctx, opts)
	// Own the record from the RUN by default (unless an explicit parent was
	// given): NewAgent's machine is one the run brings up (a crossplane vm),
	// so the record must die with the run. Without an owner set AT INIT the
	// record is orphaned — the later Children/adopt transfer only lands
	// after the agent's init finishes (it connects), so a run torn down
	// while the machine is still coming up leaves a "creating" agent nobody
	// owns and the teardown cascade never reaches. A parent given by
	// Children re-homes it later; if that never lands, run-ownership still
	// gets it cleaned.
	owner := o.Parent
	if owner == "" {
		owner = ref.RunOwner(ctx.RunId())
	}
	spec := AgentSpec{Owner: owner, Labels: o.Labels, Needs: o.Needs}
	sctx := serverCtx(ctx)
	h.Resource = NewResource[AgentState](ctx, self, workflow.ExecuteActivity(sctx, wire.DeclareAgentActivity, agentId, spec))
	h.userData = workflow.ExecuteActivity(sctx, wire.AgentUserDataActivity, agentId)
	adoptChildren(ctx, self, o.Children)
	return h
}

// NewAgentViaSSH declares an agent whose machine already exists: the
// system installs the agent over ssh — the only case where it touches
// the machine.
func NewAgentViaSSH(ctx Context, name string, install SSHInstall, opts ...ResourceOption) AgentHandle {
	agentId := id.AgentId(name)
	self := ref.OwnerRef("agent/" + name)
	h := AgentHandle{agentId: agentId, ctx: ctx}
	if ctx.Recording() {
		ctx.RecordDeclare(self, BuildResourceOptions(ctx, opts))
		h.Resource = NewResource[AgentState](ctx, self, nil)
		return h
	}
	o := BuildResourceOptions(ctx, opts)
	spec := AgentSpec{SSH: &install, Owner: o.Parent, Labels: o.Labels, Needs: o.Needs}
	sctx := serverCtx(ctx)
	h.Resource = NewResource[AgentState](ctx, self, workflow.ExecuteActivity(sctx, wire.DeclareAgentActivity, agentId, spec))
	h.userData = workflow.ExecuteActivity(sctx, wire.AgentUserDataActivity, agentId)
	adoptChildren(ctx, self, o.Children)
	return h
}

// AdoptChildren hands existing resources to a new parent — the
// library-author surface of pipeline.Children: a kind implemented
// outside this package (k8slib and friends) claims the declared
// children the same way the built-in kinds do.
func AdoptChildren(ctx Context, parent ref.OwnerRef, children []ref.OwnerRef) {
	adoptChildren(ctx, parent, children)
}

// adoptChildren hands existing resources to a new parent. Fire-and-
// forget ON PURPOSE: the child may still be INITIALIZING (an agent
// waiting for its machine to connect), and an entity executes commands
// only after init — a blocking claim here deadlocks the very flow that
// is about to create that machine (run waits for the transfer, the
// transfer waits for the agent's init, the agent waits for the vm the
// run has not declared yet). The server activity retries until the
// child can take the command; a terminal failure is logged — the tree
// edge is advisory, the run's own resources fail loudly elsewhere.
func adoptChildren(ctx Context, parent ref.OwnerRef, children []ref.OwnerRef) {
	if len(children) == 0 {
		return
	}
	workflow.Go(ctx, func(gctx workflow.Context) {
		for _, child := range children {
			req := wire.TransferResourceRequest{Resource: child, NewOwner: parent}
			if err := workflow.ExecuteActivity(serverCtx(gctx), wire.TransferResourceActivity, req).Get(gctx, nil); err != nil {
				workflow.GetLogger(gctx).Error("child claim failed",
					"parent", string(parent), "child", string(child), "error", err)
			}
		}
	})
}
