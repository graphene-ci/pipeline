package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/obs"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Main is the entry point of a pipeline binary: one main == one
// pipeline. The pipeline function is an ordinary function of Context and
// typed Params — the params type is the source of the UI form and submit
// validation; its workflow type on the wire is the pipeline id, never
// the Go function name.
//
// The role and the wiring come from the environment (set by the server
// or the agent when launching this binary as a container, or by the CLI
// for an inplace run):
//   - "run" (default) — the run worker: executes the pipeline workflow
//     and run-local activities;
//   - "machine" — the per-(agent × run) container hosted by the agent:
//     executes the activities targeted at that agent.
//
// Before serving, every role walks the pipeline function once in a
// recording pass to discover inline activity declarations — that is how
// a body written next to its call site reaches the machine container
// without a registration list. Declare activities unconditionally
// (not behind branches on runtime values): the pass sees the zero-value
// path.
func Main[P, R any](pipelineId id.PipelineId, fn func(Context, P) (R, error)) {
	if err := serve(pipelineId, fn); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
}

func serve[P, R any](pipelineId id.PipelineId, fn func(Context, P) (R, error)) error {
	if err := pipelineId.Validate(); err != nil {
		return err
	}
	rec, err := record(pipelineId, fn)
	if err != nil {
		return err
	}

	role := os.Getenv(wire.EnvRole)
	if role == "" {
		role = "run"
	}
	runId, err := id.ParseRunId(os.Getenv(wire.EnvRunId))
	if err != nil {
		return fmt.Errorf("%s: %w", wire.EnvRunId, err)
	}

	copts := client.Options{HostPort: os.Getenv(wire.EnvAddress)}
	// The namespace is the run's isolation unit, symmetric to Temporal's.
	if namespace := os.Getenv(wire.EnvNamespace); namespace != "" {
		copts.Namespace = namespace
	}
	// The address is the server's gRPC proxy — the single door; the
	// run-scoped token authenticates every Temporal call through it.
	if token := os.Getenv(wire.EnvToken); token != "" {
		copts.Credentials = client.NewAPIKeyStaticCredentials(token)
	}
	if insecure, _ := strconv.ParseBool(os.Getenv(wire.EnvInsecure)); insecure {
		copts.ConnectionOptions.TLSDisabled = true
	}
	// Observability: OTLP exporters at the same door, the graphene
	// correlation attributes on every signal, the tracing interceptor
	// spanning workflow and activities — all wired here, zero user code.
	shutdownObs, err := obs.Setup(context.Background(), obs.FromEnv())
	if err != nil {
		return fmt.Errorf("observability: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownObs(flushCtx)
	}()
	tracing, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{})
	if err != nil {
		return fmt.Errorf("tracing interceptor: %w", err)
	}

	c, err := client.Dial(copts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer c.Close()

	switch role {
	case "run":
		w := worker.New(c, wire.RunQueue(runId), worker.Options{
			// Guaranteed teardown: every exit path of the run workflow
			// triggers the server's cleanup.
			Interceptors: []interceptor.WorkerInterceptor{&cleanupInterceptor{}, tracing},
		})
		w.RegisterWorkflowWithOptions(wrap(pipelineId, fn), workflow.RegisterOptions{Name: string(pipelineId)})
		if err := registerRecorded(w, c, rec); err != nil {
			return err
		}
		return w.Run(worker.InterruptCh())
	case "machine":
		agentId, err := id.ParseAgentId(os.Getenv(wire.EnvAgentId))
		if err != nil {
			return fmt.Errorf("%s: %w", wire.EnvAgentId, err)
		}
		w := worker.New(c, wire.AgentRunQueue(agentId, runId), worker.Options{
			Interceptors: []interceptor.WorkerInterceptor{tracing},
		})
		if err := registerRecorded(w, c, rec); err != nil {
			return err
		}
		return w.Run(worker.InterruptCh())
	default:
		return fmt.Errorf("unknown role %q (%s)", role, wire.EnvRole)
	}
}

// wrap adapts the pipeline function to a Temporal workflow: builds the
// Context and translates resource failures (Ready panics) into the
// workflow's error.
func wrap[P, R any](pipelineId id.PipelineId, fn func(Context, P) (R, error)) func(workflow.Context, P) (R, error) {
	return func(wctx workflow.Context, params P) (result R, err error) {
		defer func() {
			if p := recover(); p != nil {
				if rf, ok := p.(resourceFailure); ok {
					err = rf.err
					return
				}
				panic(p)
			}
		}()
		return fn(Context{Context: wctx, pipelineId: pipelineId}, params)
	}
}

// record walks the pipeline function once with a recording Context: no
// workflow underneath, resources resolve to zero values, Activity calls
// register their bodies instead of executing. A panic ends the walk
// early — anything declared after it stays unregistered, so declarations
// must not sit behind code that needs live values.
func record[P, R any](pipelineId id.PipelineId, fn func(Context, P) (R, error)) (rec *recorder, err error) {
	rec = newRecorder()
	func() {
		defer func() {
			// The zero-value walk may die on user logic; that is fine —
			// everything declared up to that point is recorded.
			_ = recover()
		}()
		var zero P
		_, _ = fn(Context{pipelineId: pipelineId, rec: rec}, zero)
	}()
	if len(rec.errs) > 0 {
		return nil, errors.Join(rec.errs...)
	}
	return rec, nil
}

// registerRecorded registers the discovered activity bodies, the worker
// hooks (library workflows — entity definitions), and the builtins on a
// worker. Every role gets all of them: work runs on whichever queue its
// dispatch targets, registration is harmless everywhere else.
func registerRecorded(w worker.Worker, cl client.Client, rec *recorder) error {
	for name, fn := range rec.activities {
		w.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	var errs []error
	for _, hook := range rec.workerHooks {
		errs = append(errs, hook(w, cl))
	}
	w.RegisterActivityWithOptions(uploadFileActivity, activity.RegisterOptions{Name: uploadFileActivityName})
	w.RegisterActivityWithOptions(uploadBytesActivity, activity.RegisterOptions{Name: uploadBytesActivityName})
	// The safety net for the discovery pass: an activity that was NOT
	// discovered fails loudly and immediately instead of retrying into
	// silence — the error names the fix.
	w.RegisterDynamicActivity(undiscoveredActivity, activity.DynamicRegisterOptions{})
	return errors.Join(errs...)
}

// undiscoveredActivity answers for any activity name the discovery pass
// did not see.
func undiscoveredActivity(ctx context.Context, _ converter.EncodedValues) (any, error) {
	name := activity.GetInfo(ctx).ActivityType.Name
	return nil, temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("activity %q was not discovered by the registration pass: keep its declaration reachable on the optimistic zero path (no branches on live values before it)", name),
		"undiscovered-activity", nil)
}
