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
// NOTE: on the wire the record is still the "machine" kind served by the
// server; the rename to agent is a pending server-side refactor.

// AgentState is what the connected agent reported.
type AgentState = MachineState

// AgentHandle is the agent resource handle; Ready when the agent of the
// machine has connected. It is the target of activities.
type AgentHandle struct {
	Resource[AgentState]

	agentId  id.MachineId
	userData workflow.Future
	ctx      Context
}

// AgentId names the agent's record — the routing key of its activity
// queue.
func (a AgentHandle) AgentId() id.MachineId { return a.agentId }

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

// NewAgent declares the agent record and returns its handle without
// blocking. The real machine comes from whatever the user chose — a
// crossplane resource with CloudInit in user-data, a person, an ssh
// install; Ready blocks until the agent connects.
func NewAgent(ctx Context, name string, opts ...ResourceOption) AgentHandle {
	agentId := id.MachineId(name)
	self := ref.OwnerRef("machine/" + name)
	h := AgentHandle{agentId: agentId, ctx: ctx}
	if ctx.Recording() {
		h.Resource = NewResource[AgentState](ctx, self, nil)
		return h
	}
	o := BuildResourceOptions(ctx, opts)
	spec := MachineSpec{Owner: o.Parent}
	sctx := serverCtx(ctx)
	h.Resource = NewResource[AgentState](ctx, self, workflow.ExecuteActivity(sctx, wire.DeclareMachineActivity, agentId, spec))
	h.userData = workflow.ExecuteActivity(sctx, wire.AgentUserDataActivity, agentId)
	adoptChildren(ctx, self, o.Children)
	return h
}

// NewAgentViaSSH declares an agent whose machine already exists: the
// system installs the agent over ssh — the only case where it touches
// the machine.
func NewAgentViaSSH(ctx Context, name string, install SSHInstall, opts ...ResourceOption) AgentHandle {
	agentId := id.MachineId(name)
	self := ref.OwnerRef("machine/" + name)
	h := AgentHandle{agentId: agentId, ctx: ctx}
	if ctx.Recording() {
		h.Resource = NewResource[AgentState](ctx, self, nil)
		return h
	}
	o := BuildResourceOptions(ctx, opts)
	spec := MachineSpec{SSH: &install, Owner: o.Parent}
	sctx := serverCtx(ctx)
	h.Resource = NewResource[AgentState](ctx, self, workflow.ExecuteActivity(sctx, wire.DeclareMachineActivity, agentId, spec))
	h.userData = workflow.ExecuteActivity(sctx, wire.AgentUserDataActivity, agentId)
	adoptChildren(ctx, self, o.Children)
	return h
}

// adoptChildren hands existing resources to a new parent — blocking
// server calls; the records already exist, the transfer is quick.
func adoptChildren(ctx Context, parent ref.OwnerRef, children []ref.OwnerRef) {
	for _, child := range children {
		req := wire.TransferResourceRequest{Resource: child, NewOwner: parent}
		if err := workflow.ExecuteActivity(serverCtx(ctx), wire.TransferResourceActivity, req).Get(ctx, nil); err != nil {
			panic(resourceFailure{err: err})
		}
	}
}
