package pipeline

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Resource is the handle primitive: declaring a resource returns one
// immediately, without blocking. Outputs are reachable only through
// Ready — the first read blocks until the resource converges, so an
// unready resource cannot be used by construction. Libraries bring their
// own constructors (k8s client, docker, agents); they all return this
// handle, and everything systemic — the ownership tree, cascade,
// visibility in CLI/UI — is a property of the handle, not of the verb.
type Resource[Out any] struct {
	self ref.OwnerRef
	fut  workflow.Future
	rec  bool
}

// Handle is the untyped view of any resource — what tree options accept.
type Handle interface {
	// ResourceRef addresses the resource record as "kind/id".
	ResourceRef() ref.OwnerRef
}

// ResourceRef addresses this resource in the ownership tree.
func (r Resource[Out]) ResourceRef() ref.OwnerRef { return r.self }

// Ready blocks until the resource is ready and returns its outputs;
// subsequent calls return the same outcome. A resource that failed to
// converge fails the run here (Main translates it into the workflow's
// error) — use TryReady to handle the error in code instead.
func (r Resource[Out]) Ready(ctx Context) Out {
	out, err := r.TryReady(ctx)
	if err != nil {
		panic(resourceFailure{err: fmt.Errorf("%s: %w", r.self, err)})
	}
	return out
}

// TryReady is Ready with the error in hand. During the recording pass
// it returns an OPTIMISTIC zero (bools true), so readiness guards do
// not end the discovery walk.
func (r Resource[Out]) TryReady(ctx Context) (Out, error) {
	if r.rec || r.fut == nil {
		if ctx.Recording() {
			ctx.RecordUse(r.self)
			return optimisticZero[Out](), nil
		}
		var out Out
		return out, nil
	}
	var out Out
	err := r.fut.Get(ctx, &out)
	return out, err
}

// resourceFailure travels from Ready to Main's wrapper, which returns it
// as the workflow's error — user code stays free of plumbing.
type resourceFailure struct{ err error }

// Attached is the handle of a FOREIGN resource: recognized, never
// created. It reads like any handle — Ready blocks, outputs are typed —
// but it deliberately has no ResourceRef, so it cannot be a Parent or a
// Child: what is not yours cannot be burdened or given away. Nothing
// else distinguishes it; consumers never check types.
type Attached[Out any] struct {
	fut workflow.Future
	rec bool
}

// Ready blocks until the foreign resource is ready and returns its
// outputs; a failure fails the run here (TryReady to handle in code).
func (r Attached[Out]) Ready(ctx Context) Out {
	out, err := r.TryReady(ctx)
	if err != nil {
		panic(resourceFailure{err: err})
	}
	return out
}

// TryReady is Ready with the error in hand; optimistic zero during the
// recording pass, like every handle.
func (r Attached[Out]) TryReady(ctx Context) (Out, error) {
	if r.rec || r.fut == nil {
		if ctx.Recording() {
			return optimisticZero[Out](), nil
		}
		var out Out
		return out, nil
	}
	var out Out
	err := r.fut.Get(ctx, &out)
	return out, err
}

// NewAttached wraps a future into a foreign-resource handle — for
// libraries.
func NewAttached[Out any](ctx Context, fut workflow.Future) Attached[Out] {
	return Attached[Out]{fut: fut, rec: ctx.Recording()}
}

// NewResource wraps a future into a handle — the constructor RESOURCE
// LIBRARIES use; user code receives handles, never builds them.
func NewResource[Out any](ctx Context, self ref.OwnerRef, fut workflow.Future) Resource[Out] {
	return Resource[Out]{self: self, fut: fut, rec: ctx.Recording()}
}

// ResourceOptions is the resolved option set of a declaration.
type ResourceOptions struct {
	// Parent owns the new resource; zero means the run.
	Parent ref.OwnerRef
	// Children are existing resources the new one claims: the declaring
	// code is their current owner and GIVES them — ownership is given
	// away, never taken.
	Children []ref.OwnerRef
	// Labels are the record's own labels — selection by equality,
	// k8s-style; the system never interprets values.
	Labels map[string]string
	// Needs are capability requirements: readiness of the resource
	// additionally waits for them (agents today).
	Needs []wire.NeedSpec
	// Flows are the OUTGOING data-flow edges the resource declares — the
	// topology's second axis (Р-Н25). Annotation for the UI; the system
	// does not act on them.
	Flows []Flow
}

// Flow is a declared outgoing edge — re-exported from ownership so
// callers use one type.
type Flow = ownership.Flow

// WithFlow declares one outgoing edge: this resource talks TO target
// over protocol, carrying label. target is a resource handle's ref or
// an external endpoint string.
func WithFlow(to string, protocol, label string) ResourceOption {
	return func(o *ResourceOptions) {
		o.Flows = append(o.Flows, Flow{To: to, Protocol: protocol, Label: label})
	}
}

// WithFlowTo is WithFlow to a resource HANDLE (the common case — an
// edge to another declared record).
func WithFlowTo(h Handle, protocol, label string) ResourceOption {
	return func(o *ResourceOptions) {
		o.Flows = append(o.Flows, Flow{To: string(h.ResourceRef()), Protocol: protocol, Label: label})
	}
}

// ResourceOption tunes a resource declaration.
type ResourceOption func(*ResourceOptions)

// Parent makes the new resource die with an existing one instead of the
// run; the cascade follows the tree.
func Parent(h Handle) ResourceOption {
	return func(o *ResourceOptions) { o.Parent = h.ResourceRef() }
}

// Children hands existing resources to the new one — the link declared
// from whichever side exists later.
func Children(hs ...Handle) ResourceOption {
	return func(o *ResourceOptions) {
		for _, h := range hs {
			o.Children = append(o.Children, h.ResourceRef())
		}
	}
}

// WithLabels labels the record: selection by equality, never
// interpretation.
func WithLabels(labels map[string]string) ResourceOption {
	return func(o *ResourceOptions) {
		if o.Labels == nil {
			o.Labels = map[string]string{}
		}
		for k, v := range labels {
			o.Labels[k] = v
		}
	}
}

// Need requires a capability on the resource's machine: it must exist,
// be ready, and match the constraints. Readiness waits for it — the
// refusal comes before work is dispatched, not after it fails.
func Need(name string, constraints ...NeedConstraint) ResourceOption {
	spec := wire.NeedSpec{Name: name}
	for _, c := range constraints {
		c(&spec)
	}
	return func(o *ResourceOptions) { o.Needs = append(o.Needs, spec) }
}

// NeedConstraint narrows a capability requirement.
type NeedConstraint func(*wire.NeedSpec)

// WhereLabel requires equality on one capability label.
func WhereLabel(key, value string) NeedConstraint {
	return func(s *wire.NeedSpec) {
		if s.MatchLabels == nil {
			s.MatchLabels = map[string]string{}
		}
		s.MatchLabels[key] = value
	}
}

// WhereIn requires the capability label's value to be one of the given.
func WhereIn(key string, values ...string) NeedConstraint {
	return func(s *wire.NeedSpec) {
		if s.In == nil {
			s.In = map[string][]string{}
		}
		s.In[key] = append(s.In[key], values...)
	}
}

// BuildResourceOptions resolves options against the run as the default
// owner — for library constructors.
func BuildResourceOptions(ctx Context, opts []ResourceOption) ResourceOptions {
	o := ResourceOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.Parent == "" && !ctx.Recording() {
		o.Parent = ref.RunOwner(ctx.RunId())
	}
	return o
}

// TransferOption tunes a transfer of ownership.
type TransferOption func(*transferOptions)

type transferOptions struct {
	keep time.Duration
}

// KeepFor bounds the stay under the new owner: the owner deletes the
// subtree when the TTL expires. Without it the resources live until an
// explicit delete.
func KeepFor(d time.Duration) TransferOption {
	return func(o *transferOptions) { o.keep = d }
}
