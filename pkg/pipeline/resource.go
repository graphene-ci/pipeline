package pipeline

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/ref"
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

// TryReady is Ready with the error in hand.
func (r Resource[Out]) TryReady(ctx Context) (Out, error) {
	var out Out
	if r.rec || r.fut == nil {
		return out, nil
	}
	err := r.fut.Get(ctx, &out)
	return out, err
}

// resourceFailure travels from Ready to Main's wrapper, which returns it
// as the workflow's error — user code stays free of plumbing.
type resourceFailure struct{ err error }

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
