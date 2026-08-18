// Package pipeline is the library a pipeline author writes against. A
// pipeline is an ordinary Temporal workflow; this library adds what plain
// Temporal does not have: declaring resources with owned lifetimes,
// running functions on machines, one-shot actions with "at most once"
// semantics, and reference types for secrets and blobs.
//
// The same user binary serves every execution site: the run worker
// (managed container or inplace process) and the per-(machine × run)
// container hosted by the agent. Dispatch is plain Temporal — a function
// passed to the machine-execution helpers must be a named, registered
// function, not a closure (closures do not serialize).
package pipeline

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Re-exported reference types: what travels instead of values.
type (
	// SecretRef names a secret; the value is resolved at the point of use.
	SecretRef = ref.SecretRef
	// BlobRef points at bytes in external storage.
	BlobRef = ref.BlobRef
)

// RunId derives the current run id from the workflow ID.
func RunId(ctx workflow.Context) id.RunId {
	return id.RunId(workflow.GetInfo(ctx).WorkflowExecution.ID)
}

// Delete explicitly deletes a resource the run owns; the implicit path —
// the run's end tearing down everything it owns — needs no call.
func Delete(ctx workflow.Context, owner ref.OwnerRef) error {
	return workflow.ExecuteActivity(serverCtx(ctx), wire.DeleteResourceActivity, owner).Get(ctx, nil)
}

func serverCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: wire.ServerQueue,
		// Declaration waits for readiness inside the server's activity;
		// heartbeats distinguish "still converging" from "lost".
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// --- execution on machines ---
//
// NOTE: the names OnMachine/Action are PROVISIONAL — the final naming of
// the machine-execution pair is deliberately not decided yet.

// ExecOptions tune a machine-bound call.
type ExecOptions struct {
	// Timeout bounds a single execution (start-to-close). Zero means 10m.
	Timeout time.Duration
	// HeartbeatTimeout distinguishes "still running" from "died". Zero
	// disables heartbeating.
	HeartbeatTimeout time.Duration
}

func (o *ExecOptions) defaults() {
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Minute
	}
}

// OnMachine executes a registered function on the machine, inside the
// per-(machine × run) container hosted by the agent: an ordinary Temporal
// activity on the machine's run queue, at-least-once, retried by policy.
// Use for converging operations that are idempotent by construction.
func OnMachine(ctx workflow.Context, machineId id.MachineId, opts ExecOptions, fn any, args ...any) workflow.Future {
	opts.defaults()
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.MachineRunQueue(machineId, RunId(ctx)),
		StartToCloseTimeout: opts.Timeout,
		HeartbeatTimeout:    opts.HeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
	return workflow.ExecuteActivity(actx, fn, args...)
}

// ErrUnknown reports that a one-shot action was dispatched but its outcome
// could not be established: it may or may not have executed. There is no
// silent retry — the caller decides by policy (retry under a NEW action,
// ask a human, fail the run).
var ErrUnknown = errors.New("action outcome unknown")

// Action executes a registered function on the machine AT MOST ONCE:
// MaximumAttempts=1, no retries. A timeout or worker loss surfaces as
// ErrUnknown (wrapped), never as a re-execution. Use for one-shot work —
// a performance test, a migration; use OnMachine for converging work.
func Action(ctx workflow.Context, machineId id.MachineId, opts ExecOptions, fn any, args ...any) workflow.Future {
	opts.defaults()
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.MachineRunQueue(machineId, RunId(ctx)),
		StartToCloseTimeout: opts.Timeout,
		HeartbeatTimeout:    opts.HeartbeatTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return unknownClassifier{workflow.ExecuteActivity(actx, fn, args...)}
}

// unknownClassifier wraps an at-most-once activity future, translating
// undeterminable outcomes (timeouts) into ErrUnknown.
type unknownClassifier struct {
	workflow.Future
}

// Get resolves the underlying future, classifying timeout-shaped failures
// as ErrUnknown.
func (u unknownClassifier) Get(ctx workflow.Context, valuePtr any) error {
	err := u.Future.Get(ctx, valuePtr)
	if err == nil {
		return nil
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return errors.Join(ErrUnknown, err)
	}
	return err
}
