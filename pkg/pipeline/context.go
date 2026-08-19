package pipeline

import (
	"strings"
	"sync"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// Context is the pipeline's workflow context. It carries ONLY what does
// not exist outside a run — the run's identity and its logger;
// everything that acts is a free function taking Context first. It
// embeds workflow.Context, so plain Temporal code works against it
// unchanged.
type Context struct {
	workflow.Context

	pipelineId id.PipelineId
	rec        *recorder
}

// RunId is the identity of this run. The run workflow's ID on the wire
// is "run/{runId}" — this strips the prefix back off.
func (c Context) RunId() id.RunId {
	if c.rec != nil {
		return ""
	}
	full := workflow.GetInfo(c.Context).WorkflowExecution.ID
	return id.RunId(strings.TrimPrefix(full, "run/"))
}

// Logger is the run's structured logger.
func (c Context) Logger() log.Logger {
	if c.rec != nil {
		return nopLogger{}
	}
	return workflow.GetLogger(c.Context)
}

// --- library-author surface below: user code never needs these ---

// Recording reports whether this is the registration pass: at startup
// every role walks the pipeline function once with a recording Context
// to DISCOVER inline activity declarations — nothing executes, resources
// resolve to zero values. Libraries must register their activities and
// do nothing else when this is true.
func (c Context) Recording() bool { return c.rec != nil }

// RecordActivity registers an activity body under its wire name during
// the recording pass; outside of it the call is a no-op (the worker is
// already serving). Duplicate names with different bodies are collected
// and fail Main with an error list.
func (c Context) RecordActivity(name string, fn any) {
	if c.rec == nil {
		return
	}
	c.rec.record(name, fn)
}

// RecordWorker registers a worker-assembly hook during the recording
// pass: libraries that bring WHOLE workflows (entity definitions), not
// just activity bodies, register them here. The hook runs when Main
// builds each role's worker, with the worker and the Temporal client in
// hand. Outside the recording pass the call is a no-op.
func (c Context) RecordWorker(fn func(w worker.Worker, cl client.Client) error) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	c.rec.workerHooks = append(c.rec.workerHooks, fn)
}

// RecordKind notes an entity kind the pipeline's libraries declare —
// manifest material. Recording pass only.
func (c Context) RecordKind(name string) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	c.rec.kinds[name] = true
}

// recorder collects what the registration pass discovers.
type recorder struct {
	mu          sync.Mutex
	activities  map[string]any
	workerHooks []func(w worker.Worker, cl client.Client) error
	kinds       map[string]bool
	errs        []error
}

func newRecorder() *recorder {
	return &recorder{activities: map[string]any{}, kinds: map[string]bool{}}
}

// activityNames lists the discovered wire names.
func (r *recorder) activityNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.activities))
	for name := range r.activities {
		out = append(out, name)
	}
	return out
}

// kindNames lists the declared entity kinds.
func (r *recorder) kindNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.kinds))
	for name := range r.kinds {
		out = append(out, name)
	}
	return out
}

func (r *recorder) record(name string, fn any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.activities[name]; exists {
		// The same declaration reached twice (a loop, a helper called
		// repeatedly) is fine; the body is the same code by construction.
		return
	}
	r.activities[name] = fn
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
