// Package activity is the execution primitive of a pipeline: run a piece
// of code on an execution site — an agent's machine through its
// per-(agent × run) container, or the run's own worker. The code value
// is self-described (name, body, arguments); libraries export ready-made
// values (dockerlib.Install()), user code wraps its own with ActivityFn.
// Registration is Main's business: declarations are DISCOVERED, never
// listed.
package activity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Target is where an activity runs — any agent, ours or foreign: the
// same interface every consumer sees, an alias so selections plug into
// fan-out without conversion.
type Target = pipeline.Agent

// Call is a self-described piece of code with its arguments bound: the
// unit Activity executes. Build one with ActivityFn or take one from a
// library.
type Call[Res any] struct {
	name string
	fn   any
	args []any
}

// Fn builds a Call from a named body and its argument. The name is the
// wire identity of the code — the only place it appears. Declare calls
// unconditionally reachable (the registration pass walks the zero-value
// path); arguments travel only through this binding — a closure must not
// capture workflow state.
func Fn[In, Res any](name string, fn func(context.Context, In) (Res, error), in In) Call[Res] {
	return Call[Res]{name: name, fn: fn, args: []any{in}}
}

// ActivityFn is Fn under its historical name.
func ActivityFn[In, Res any](name string, fn func(context.Context, In) (Res, error), in In) Call[Res] {
	return Fn(name, fn, in)
}

// Fn0 builds a Call from a body with no arguments — the shape libraries
// usually export.
func Fn0[Res any](name string, fn func(context.Context) (Res, error)) Call[Res] {
	return Call[Res]{name: name, fn: fn}
}

// Guarantee is the execution guarantee of one Activity call.
type Guarantee int

// Execution guarantees.
const (
	// AtLeastOnce (default): converging work, retried by policy — make
	// the body idempotent.
	AtLeastOnce Guarantee = iota
	// AtMostOnce: one-shot work, never retried; an undeterminable
	// outcome surfaces as pipeline.ErrUnknown, never as a re-execution.
	AtMostOnce
)

// Option tunes one Activity call.
type Option func(*options)

type options struct {
	guarantee Guarantee
	timeout   time.Duration
	heartbeat time.Duration
}

// WithGuarantee sets the execution guarantee.
func WithGuarantee(g Guarantee) Option {
	return func(o *options) { o.guarantee = g }
}

// WithTimeout bounds a single execution (default 10m).
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithHeartbeat distinguishes "still running" from "died" (default 30s).
func WithHeartbeat(d time.Duration) Option {
	return func(o *options) { o.heartbeat = d }
}

// ActivityAll executes the call on EVERY target in parallel — "run it on
// all who are marked": pair it with pipeline.SelectAgents. Results align
// with targets; failures are joined into one error naming each agent —
// partial success is visible, not hidden.
func ActivityAll[Res any](ctx pipeline.Context, targets []Target, call Call[Res], opts ...Option) ([]Res, error) {
	var zero []Res
	if ctx.Recording() {
		ctx.RecordActivity(call.name, call.fn)
		ctx.RecordStep("activity-all", call.name, "<selection>", guaranteeNote(opts))
		return zero, nil
	}
	futures := make([]workflow.Future, len(targets))
	for i, target := range targets {
		futures[i] = dispatch(ctx, target, call, opts)
	}
	results := make([]Res, len(targets))
	var errs []error
	for i, fut := range futures {
		if err := fut.Get(ctx, &results[i]); err != nil {
			errs = append(errs, fmt.Errorf("agent %s: %w", targets[i].AgentId(), err))
		}
	}
	return results, errors.Join(errs...)
}

// Activity executes the call on the target and returns its typed result.
// The first touch of an agent by the run brings its container up; the
// call blocks until the result — declare resources beforehand for
// parallel convergence, activities are the sequential story of the run.
func Activity[Res any](ctx pipeline.Context, target Target, call Call[Res], opts ...Option) (Res, error) {
	var zero Res
	if ctx.Recording() {
		ctx.RecordActivity(call.name, call.fn)
		ctx.RecordStep("activity", call.name, "agent/"+string(target.AgentId()), guaranteeNote(opts))
		return zero, nil
	}
	fut := dispatch(ctx, target, call, opts)
	var res Res
	err := fut.Get(ctx, &res)
	return res, err
}

// dispatch starts the call on one target, wrapping the future with the
// guarantee semantics.
func dispatch[Res any](ctx pipeline.Context, target Target, call Call[Res], opts []Option) workflow.Future {
	o := options{timeout: 10 * time.Minute, heartbeat: 30 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: o.timeout,
		HeartbeatTimeout:    o.heartbeat,
	}
	if o.guarantee == AtMostOnce {
		actOpts.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	} else {
		actOpts.RetryPolicy = &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		}
	}
	fut := pipeline.DispatchOnAgent(ctx, target.AgentId(), actOpts, call.name, call.args...)
	if o.guarantee == AtMostOnce {
		return unknownClassifier{fut}
	}
	return fut
}

// unknownClassifier translates undeterminable at-most-once outcomes
// (timeouts) into pipeline.ErrUnknown — never a silent re-execution.
type unknownClassifier struct {
	workflow.Future
}

// Get resolves the future, classifying timeout-shaped failures.
func (u unknownClassifier) Get(ctx workflow.Context, valuePtr any) error {
	err := u.Future.Get(ctx, valuePtr)
	if err == nil {
		return nil
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return errors.Join(pipeline.ErrUnknown, err)
	}
	return err
}

// guaranteeNote renders the execution guarantee for the plan.
func guaranteeNote(opts []Option) string {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.guarantee == AtMostOnce {
		return "at-most-once"
	}
	return ""
}
