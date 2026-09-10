package pipeline_test

import (
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// RunAll must keep at most `concurrency` children ALIVE at once, not merely
// bound their starts: the semaphore has to be held across the whole child
// life (start + await), which is the tenant-quota guarantee. The await
// simulator counts how many children are simultaneously awaiting; with the
// bug (release right after start) all cells run at once.
func TestRunAllConcurrencyBound(t *testing.T) {
	const cells, concurrency = 3, 2

	var mu sync.Mutex
	var current, peak int

	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())

	// The start is a no-op success; the child "runs" for as long as its
	// await sleeps in virtual time.
	pipelinetest.Handle1(w, wire.StartChildRunActivity, func(_ workflow.Context, _ wire.StartChildRunRequest) (any, error) {
		return nil, nil
	})
	pipelinetest.Handle1(w, wire.AwaitChildRunActivity, func(ctx workflow.Context, _ wire.AwaitChildRunRequest) (int, error) {
		mu.Lock()
		current++
		if current > peak {
			peak = current
		}
		mu.Unlock()
		_ = workflow.Sleep(ctx, time.Second) // the child's "lifetime"
		mu.Lock()
		current--
		mu.Unlock()
		return 1, nil
	})

	parent := func(ctx pipeline.Context, _ struct{}) (int, error) {
		cs := make([]pipeline.Cell, cells)
		for i := range cs {
			cs[i] = pipeline.Cell{ID: string(rune('a' + i)), Params: map[string]int{"n": i}}
		}
		handles := pipeline.RunAll[int](ctx, "child", cs, concurrency)
		total := 0
		for _, h := range handles {
			total += h.Ready(ctx)
		}
		return total, nil
	}

	wf := pipelinetest.Workflow(w, "parent", parent)
	w.Env.ExecuteWorkflow(wf, struct{}{})

	if err := w.Env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var total int
	if err := w.Env.GetWorkflowResult(&total); err != nil {
		t.Fatalf("result: %v", err)
	}
	if total != cells {
		t.Errorf("want all %d children awaited, got total %d", cells, total)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak > concurrency {
		t.Errorf("RunAll ran %d children at once, concurrency bound is %d", peak, concurrency)
	}
	if peak != concurrency {
		t.Errorf("expected the bound %d to be saturated, peak was %d", concurrency, peak)
	}
}
