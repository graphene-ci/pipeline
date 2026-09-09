package pipeline

import (
	"maps"

	"github.com/graphene-ci/pipeline/pkg/id"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Definition is a prepared pipeline, usable by workers and local test harnesses.
// Preparation discovers declarations without executing their activity bodies.
type Definition[P, R any] struct {
	Workflow func(workflow.Context, P) (R, error)
	rec      *recorder
}

// Prepare uses Main's discovery and workflow wrapper, including parameter
// validation and conversion of Ready failures to workflow errors.
func Prepare[P, R any](name id.PipelineId, fn func(Context, P) (R, error)) (*Definition[P, R], error) {
	if err := name.Validate(); err != nil {
		return nil, err
	}
	rec, err := record(name, fn)
	if err != nil {
		return nil, err
	}
	return &Definition[P, R]{Workflow: wrap(name, fn), rec: rec}, nil
}

// Activities returns a copy of the discovered name-to-body registry.
func (d *Definition[P, R]) Activities() map[string]any { return maps.Clone(d.rec.activities) }

// Registration returns library-owned metadata from this preparation.
// Treat the returned value as immutable after Prepare.
func (d *Definition[P, R]) Registration(key string) any { return d.rec.values[key] }

// RegisterActivities assembles the same activities and library workflows as Main.
func (d *Definition[P, R]) RegisterActivities(w worker.Worker, cl client.Client) error {
	return registerRecorded(w, cl, d.rec)
}

// RunInterceptor installs Main's cleanup behavior on a run worker or test
// environment. Do not install it on workers serving resource entity workflows.
func RunInterceptor() interceptor.WorkerInterceptor { return &cleanupInterceptor{} }
