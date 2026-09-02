// Package pipeline is the library a pipeline author writes against. User
// code is a mix of Resource (handles with owned lifetimes in one tree),
// Activity (code executed on an execution site — see pkg/activity), and
// plain control flow; everything else — registration, containers,
// delivery — is wiring the library hides.
//
// The same user binary serves every execution site: the run worker
// (managed container or inplace process) and the per-(agent × run)
// container hosted by the agent on a machine.
package pipeline

import (
	"errors"
	"os"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// SecretRef re-export: what travels instead of a secret value.
type SecretRef = ref.SecretRef

// BlobRef re-export: what travels instead of bytes.
type BlobRef = ref.BlobRef

// Secret builds a reference into this pipeline's secret set — the values
// are assigned to the pipeline on the server. Only the name ever
// travels; the value resolves inside activities at the point of use and
// never comes back.
func Secret(_ Context, name string) SecretRef {
	return SecretRef{Name: id.SecretId(name)}
}

// UseSecret is Secret for declarations written OUTSIDE a run — trigger
// params, specs. Same reference, no Context.
func UseSecret(name string) SecretRef { return ref.Secret(name) }

// Var references an installation VARIABLE — the visible sibling of a
// secret: environment configuration (cloud folder ids, hosts) that does
// not belong in pipeline code but is not sensitive. The door replaces
// the placeholder with the variable's value when the run starts, BEFORE
// schema validation — so a declaration (a cron's params) carries no
// environment literals. Usable in any string-typed params field.
func Var(name string) string { return "${var:" + name + "}" }

// ErrUnknown reports that an at-most-once activity was dispatched but
// its outcome could not be established: it may or may not have executed.
// There is no silent retry — the caller decides by policy.
var ErrUnknown = errors.New("outcome unknown")

// serverCtx targets the server's activity queue.
func serverCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// ensureCtx bounds the ONE activity that can hang the whole run: bringing
// the (agent × run) container up. Without an overall deadline a container
// that never starts — a wrong server address, a stale runc state, a dead
// agent — retries every minute forever, and the run stays Running,
// holding every cloud resource it declared. ScheduleToCloseTimeout caps
// the total wait across retries: the container must come up within it or
// the activity fails and the run tears down instead of bleeding.
func ensureCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              wire.ServerQueue,
		StartToCloseTimeout:    30 * time.Minute,
		ScheduleToCloseTimeout: 15 * time.Minute,
		HeartbeatTimeout:       time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// DispatchOnAgent runs a named activity inside the per-(agent × run)
// container: the first touch of an agent by the run brings the container
// up (an idempotent server call precedes the activity), exposed as one
// future so independent agents converge in parallel. Library-author
// surface — user code goes through pkg/activity.
func DispatchOnAgent(ctx Context, agentId id.AgentId, actOpts workflow.ActivityOptions, name string, args ...any) workflow.Future {
	if ctx.Recording() {
		// Callers check Recording() and register instead of dispatching;
		// a nil future here would be a caller bug.
		return nil
	}
	if actOpts.TaskQueue == "" {
		actOpts.TaskQueue = wire.AgentRunQueue(agentId, ctx.RunId())
	}
	image := workerImage(ctx)
	fut, set := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(gctx workflow.Context) {
		req := wire.EnsureContainerRequest{AgentId: agentId, RunId: ctx.RunId(), Image: image}
		if err := workflow.ExecuteActivity(ensureCtx(gctx), wire.EnsureContainerActivity, req).Get(gctx, nil); err != nil {
			set.SetError(err)
			return
		}
		set.Chain(workflow.ExecuteActivity(workflow.WithActivityOptions(gctx, actOpts), name, args...))
	})
	return fut
}

// workerImage reads the run's own image ref through a side effect:
// recorded in history once, stable across replays.
func workerImage(ctx Context) string {
	var image string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		return os.Getenv(wire.EnvImage)
	}).Get(&image); err != nil {
		return ""
	}
	return image
}
