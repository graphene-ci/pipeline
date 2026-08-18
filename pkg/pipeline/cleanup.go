package pipeline

import (
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

// cleanupInterceptor guarantees the run's teardown: whatever way the run
// workflow ends — return, error, cancellation — the server's cleanup
// activity runs, deleting everything the run owns and stopping its
// machine containers. This is the "teardown is guaranteed on every
// outcome" invariant for the exit paths the worker survives to see; a
// worker lost forever is the server watcher's job (not built yet).
type cleanupInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (c *cleanupInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &cleanupInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type cleanupInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (c *cleanupInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	result, err := c.Next.ExecuteWorkflow(ctx, in)
	cleanup(ctx)
	return result, err
}

func cleanup(ctx workflow.Context) {
	// The workflow may be ending through cancellation — a cancelled
	// context cannot run activities, a disconnected one can.
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	actx := workflow.WithActivityOptions(dctx, workflow.ActivityOptions{
		TaskQueue:           wire.ServerQueue,
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
	// A cleanup failure must not mask the run's own result; the activity
	// retries hard before giving up, and the error is visible in history.
	_ = workflow.ExecuteActivity(actx, wire.RunCleanupActivity, RunId(ctx)).Get(dctx, nil)
}
