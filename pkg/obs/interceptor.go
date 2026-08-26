package obs

// The worker-side half of entity observability. Dimensions 3-5 of a
// record — its logs, metrics and traces — exist only if something
// EMITS them, and emission must not depend on anyone remembering to
// instrument the next activity. One interceptor covers every worker:
// the server's (system records), the run's (the run itself), and the
// machine executor's (library resources) — because every activity
// executes inside SOME record's history, and that record is the
// signal's subject.

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
)

// Metric names of the activity contour. Names are STABLE — a dashboard
// outlives a refactor; the worker is told apart by the "contour"
// attribute, the activity and entity by the automatic ones.
const (
	// MetricActivity counts executions by outcome.
	MetricActivity = "graphene.activity"
	// MetricActivityDuration measures them, in seconds.
	MetricActivityDuration = "graphene.activity.seconds"
	// MetricActivityRetry counts RETRIED attempts — the signal behind
	// the silent backoff that costs a day of debugging when invisible.
	MetricActivityRetry = "graphene.activity.retry"
)

// Interceptor stamps every activity with the entity whose history it
// runs in, logs its failures and retries, and measures it. Spans are
// NOT emitted here — the Temporal OTel tracing interceptor owns them;
// installing both on one worker double-spans nothing.
type Interceptor struct {
	interceptor.WorkerInterceptorBase
	// Contour names the worker: "server", "run", "machine".
	Contour string
}

// InterceptActivity wraps the activity chain.
func (i *Interceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityInbound{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		contour:                        i.Contour,
	}
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	contour string
}

// ExecuteActivity is where an activity becomes observable. The
// workflow id it executes under IS the entity whose history it belongs
// to ("pipeline/x", "docker/db", "run/r-1") — the automatic subject; a
// body that works on a DIFFERENT record refines it with WithEntity.
func (a *activityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	if Entity(ctx) == "" && info.WorkflowExecution.ID != "" {
		ctx = WithEntity(ctx, info.WorkflowExecution.ID)
	}
	contour := Str("contour", a.contour)
	if info.Attempt > 1 {
		// A retry is a fact about the ENTITY, not only the worker: it
		// is why a record sits in "creating" with nothing apparently
		// happening.
		Count(ctx, MetricActivityRetry, 1, contour)
		Warn(ctx, "activity retrying",
			Str("activity", info.ActivityType.Name), Int("attempt", int(info.Attempt)), contour)
	}
	started := time.Now()
	res, err := a.Next.ExecuteActivity(ctx, in)
	outcome := Str("outcome", "ok")
	if err != nil {
		outcome = Str("outcome", "error")
	}
	Count(ctx, MetricActivity, 1, contour, outcome)
	Measure(ctx, MetricActivityDuration, time.Since(started).Seconds(), contour, outcome)
	if err != nil {
		Error(ctx, "activity failed",
			Str("activity", info.ActivityType.Name), Err(err), contour)
	}
	return res, err
}
