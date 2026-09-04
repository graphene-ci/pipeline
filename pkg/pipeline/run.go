package pipeline

import (
	"encoding/json"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Run starts ANOTHER pipeline as a CHILD of this run and returns a
// resource-like handle immediately: the child's id is "<parent>-<cell>",
// its owner is this run, and .Ready(ctx) blocks until the child reaches a
// terminal state — returning its typed result R, or an error if the child
// failed or was cancelled.
//
// The child is a full managed run of ITS OWN pipeline on ITS OWN active
// revision (not this pipeline's) — the same start the server does for any
// run, so the tree, RBAC, obs and teardown all work with no exceptions.
// Because the child's EntityOwner is this run, cancelling the parent
// cascades to the child (which tears its own infra down via its finalize),
// and one Tree(run/<parent>) shows every child and its resources.
//
// Determinism/resurrection: the child id is derived from (parent, cell),
// and the start is idempotent (USE_EXISTING) — a parent crash and replay
// re-attaches to the live child instead of starting a second one.
func Run[R any](ctx Context, pipeline, cell string, params any, opts ...ResourceOption) Resource[R] {
	childRunId := string(ctx.RunId()) + "-" + cell
	self := ref.OwnerRef("run/" + childRunId)
	if ctx.Recording() {
		// One op node per Run call, subject = the child pipeline. The child
		// count on the zero-path is unknown (a matrix is a runtime value),
		// so a fan-out shows as a single node here.
		ctx.RecordStep("run", "pipeline/"+pipeline, "", cell)
		return NewResource[R](ctx, self, nil)
	}
	o := BuildResourceOptions(ctx, opts)
	fut := startChild[R](ctx, childRunId, pipeline, params, o.Labels, nil)
	return NewResource[R](ctx, self, fut)
}

// RunAll fans this run out across a set of cells of ONE child pipeline,
// with at most `concurrency` children running at once (the rest wait) —
// the bound a tenant's quota and single cloud account need. Each returned
// handle's .Ready(ctx) yields that cell's typed result R or its error; a
// failed cell does NOT abort its siblings — the caller decides what a
// partial result means.
func RunAll[R any](ctx Context, pipeline string, cells []Cell, concurrency int, opts ...ResourceOption) []Resource[R] {
	if ctx.Recording() {
		ctx.RecordStep("run", "pipeline/"+pipeline, "", "× N cells")
		return nil
	}
	o := BuildResourceOptions(ctx, opts)
	if concurrency <= 0 || concurrency > len(cells) {
		concurrency = len(cells)
	}
	var sem workflow.Semaphore
	if concurrency > 0 {
		sem = workflow.NewSemaphore(ctx, int64(concurrency))
	}
	out := make([]Resource[R], len(cells))
	for i := range cells {
		cell := cells[i]
		childRunId := string(ctx.RunId()) + "-" + cell.ID
		self := ref.OwnerRef("run/" + childRunId)
		out[i] = NewResource[R](ctx, self, startChild[R](ctx, childRunId, pipeline, cell.Params, o.Labels, sem))
	}
	return out
}

// Cell is one point of a RunAll fan-out: an id (used to derive the child
// run id) and the child pipeline's typed params.
type Cell struct {
	ID     string
	Params any
}

// startChild builds the future behind a child handle: start the child (or
// attach to the live one), then await its terminal result. When sem is
// non-nil it is held across BOTH — so at most `concurrency` children are
// alive at once. The whole thing runs in its own workflow goroutine so the
// handle returns without blocking.
func startChild[R any](ctx Context, childRunId, pipeline string, params any, labels map[string]string, sem workflow.Semaphore) workflow.Future {
	raw, err := json.Marshal(params)
	fut, set := workflow.NewFuture(ctx)
	if err != nil {
		set.SetError(err)
		return fut
	}
	workflow.Go(ctx, func(gctx workflow.Context) {
		if sem != nil {
			if err := sem.Acquire(gctx, 1); err != nil {
				set.SetError(err)
				return
			}
			defer sem.Release(1)
		}
		startReq := wire.StartChildRunRequest{RunId: childRunId, Pipeline: pipeline, Params: json.RawMessage(raw), Labels: labels}
		if err := workflow.ExecuteActivity(serverCtx(gctx), wire.StartChildRunActivity, startReq).Get(gctx, nil); err != nil {
			set.SetError(err)
			return
		}
		// Await returns the child's result JSON; Get decodes it into R.
		set.Chain(workflow.ExecuteActivity(awaitChildCtx(gctx), wire.AwaitChildRunActivity, wire.AwaitChildRunRequest{RunId: childRunId}))
	})
	return fut
}

// awaitChildCtx bounds the await activity generously — a child run may
// take a long time — and heartbeats, so a worker restart re-attaches to
// the still-running child rather than the whole handle failing.
func awaitChildCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: 168 * time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}
