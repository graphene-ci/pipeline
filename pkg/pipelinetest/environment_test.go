package pipelinetest_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	pa "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

func newWorld(t *testing.T) *pipelinetest.World {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	return pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
}

func dangerous(context.Context, string) (string, error) { panic("production activity body executed") }

func machineRun(ctx pipeline.Context, _ struct{}) (bool, error) {
	a := pipeline.NewAgent(ctx, "machine")
	_, err := pa.Activity(ctx, a, pa.Fn("work", dangerous, "input"), pa.WithGuarantee(pa.AtMostOnce))
	if errors.Is(err, pipeline.ErrUnknown) {
		return true, nil
	}
	return false, err
}

func TestUnmockedActivityNeverExecutes(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	w.Env.ExecuteWorkflow(pipelinetest.Workflow(w, "guard", machineRun), struct{}{})
	require.ErrorContains(t, w.Env.GetWorkflowError(), "has no test replacement")
	require.Equal(t, "failure", w.Outcome("run/test-guard"))
	w.AssertNoLeaks(t)
}

func TestAtMostOnceUnknown(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	wf := pipelinetest.Workflow(w, "unknown", machineRun)
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("", temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil)).Once()
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var unknown bool
	require.NoError(t, w.Env.GetWorkflowResult(&unknown))
	require.True(t, unknown)
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
}

func TestActivityRetryAndRouting(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	wf := pipelinetest.Workflow(w, "retry", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "machine")
		return pa.Activity(ctx, a, pa.Fn("work", dangerous, "input"))
	})
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("", errors.New("transient")).Once()
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("ok", nil).Once()
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var result string
	require.NoError(t, w.Env.GetWorkflowResult(&result))
	require.Equal(t, "ok", result)
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
}

func TestSameActivityHasIndependentAgentExpectations(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("first", 0)
	w.ConnectAfter("second", 0)
	wf := pipelinetest.Workflow(w, "routes", func(ctx pipeline.Context, _ struct{}) ([]string, error) {
		first := pipeline.NewAgent(ctx, "first")
		second := pipeline.NewAgent(ctx, "second")
		return pa.ActivityAll(ctx, []pa.Target{first, second}, pa.Fn("work", dangerous, "input"))
	})
	w.OnAgentActivity("first", "work", mock.Anything, "input").Return("first result", nil).Once()
	w.OnAgentActivity("second", "work", mock.Anything, "input").Return("second result", nil).Once()
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var results []string
	require.NoError(t, w.Env.GetWorkflowResult(&results))
	require.Equal(t, []string{"first result", "second result"}, results)
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
}

func TestDispatchFreezesInputBeforeUserMutation(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	wf := pipelinetest.Workflow(w, "payload", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		labels := map[string]string{"role": "original"}
		a := pipeline.NewAgent(ctx, "machine", pipeline.WithLabels(labels))
		labels["role"] = "mutated"
		_ = a.Ready(ctx)
		selected, err := pipeline.SelectAgents(ctx, pipeline.WithLabels(map[string]string{"role": "original"}))
		return len(selected) == 1, err
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var frozen bool
	require.NoError(t, w.Env.GetWorkflowResult(&frozen))
	require.True(t, frozen)
}

type validated struct {
	Name string `validate:"required"`
}

func TestValidationUsesProductionWrapper(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wf := pipelinetest.Workflow(w, "validation", func(ctx pipeline.Context, _ validated) (string, error) {
		pipeline.NewAgent(ctx, "must-not-exist")
		return "", nil
	})
	w.Env.ExecuteWorkflow(wf, validated{})
	require.Error(t, w.Env.GetWorkflowError())
	_, exists := w.Resource("agent/must-not-exist")
	require.False(t, exists)
	require.Equal(t, "failure", w.Outcome("run/test-validation"))
}

func TestSelectionCapabilitiesAndForeignOwnership(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.SeedAgent("foreign", map[string]string{"role": "edge"}, pipeline.AgentState{AgentConnected: true})
	w.ConnectAfter("new", 5*time.Second)
	wf := pipelinetest.Workflow(w, "selection", func(ctx pipeline.Context, _ struct{}) ([]id.AgentId, error) {
		a := pipeline.NewAgent(ctx, "new", pipeline.WithLabels(map[string]string{"role": "edge"}))
		if ctx.Recording() {
			return nil, nil
		}
		early, err := pipeline.SelectAgents(ctx, pipeline.WithLabels(map[string]string{"role": "edge"}))
		if err != nil {
			return nil, err
		}
		if len(early) != 1 || early[0].AgentId() != "foreign" {
			return nil, errors.New("creating agent selected")
		}
		_ = a.Ready(ctx)
		if err := pipeline.PublishCapability(ctx, a, pipeline.Capability{Name: "docker", Ready: true, Labels: map[string]string{"abi": "v1"}}); err != nil {
			return nil, err
		}
		selected, err := pipeline.SelectAgents(ctx, pipeline.Need("docker", pipeline.WhereLabel("abi", "v1")))
		if err != nil {
			return nil, err
		}
		out := make([]id.AgentId, len(selected))
		for i, a := range selected {
			out[i] = a.AgentId()
		}
		return out, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var result []id.AgentId
	require.NoError(t, w.Env.GetWorkflowResult(&result))
	require.Equal(t, []id.AgentId{"new"}, result)
	r, _ := w.Resource("agent/foreign")
	require.True(t, r.Foreign)
	require.Equal(t, "ready", r.Phase)
	w.AssertNoLeaks(t)
}

func TestMissingAgentTimesOutInVirtualTime(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.Env.ExecuteWorkflow(pipelinetest.Workflow(w, "timeout", machineRun), struct{}{})
	require.ErrorContains(t, w.Env.GetWorkflowError(), "did not converge")
	w.AssertNoLeaks(t)
}

func TestArtifactBytesAndTTLOwnership(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wf := pipelinetest.Workflow(w, "artifact", func(ctx pipeline.Context, _ struct{}) (pipeline.ArtifactState, error) {
		a := pipeline.NewArtifact(ctx, "report", artifact.FromBytes([]byte("content")))
		state := a.Ready(ctx)
		pipeline.ToStand(ctx, a, pipeline.KeepFor(time.Hour))
		return state, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var state pipeline.ArtifactState
	require.NoError(t, w.Env.GetWorkflowResult(&state))
	data, ok := w.Blob(state.Blob)
	require.True(t, ok)
	require.Equal(t, []byte("content"), data)
	w.Advance(30 * time.Minute)
	w.AssertOwner(t, "artifact/report", "stand/artifact")
	w.Advance(30 * time.Minute)
	r, _ := w.Resource("artifact/report")
	require.Equal(t, "deleted", r.Phase)
	_, ok = w.Blob(state.Blob)
	require.False(t, ok, "unreferenced artifact content must be reclaimed")
	w.AssertNoLeaks(t)
}

func TestOwnedArtifactDeletionPreservesSharedForeignContent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	baseline := w.SeedArtifact("baseline", []byte("shared"))
	wf := pipelinetest.Workflow(w, "shared", func(ctx pipeline.Context, _ struct{}) (pipeline.ArtifactState, error) {
		return pipeline.NewArtifact(ctx, "owned", artifact.FromBytes([]byte("shared"))).Ready(ctx), nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	owned, _ := w.Resource("artifact/owned")
	require.Equal(t, "deleted", owned.Phase)
	data, ok := w.Blob(baseline.Blob)
	require.True(t, ok)
	require.Equal(t, []byte("shared"), data)
	w.AssertNoLeaks(t)
}

type ownerHandle ref.OwnerRef

func (h ownerHandle) ResourceRef() ref.OwnerRef { return ref.OwnerRef(h) }

func TestCyclicDeclarationFailsWithoutBreakingCleanup(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wf := pipelinetest.Workflow(w, "cycle", func(ctx pipeline.Context, _ struct{}) (pipeline.ArtifactState, error) {
		return pipeline.NewArtifact(ctx, "cycle", artifact.FromBytes([]byte("data")), pipeline.Parent(ownerHandle("artifact/cycle"))).Ready(ctx), nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.ErrorContains(t, w.Env.GetWorkflowError(), "ownership cycle")
	require.Equal(t, "failure", w.Outcome("run/test-cycle"))
	w.AssertNoLeaks(t)
}

func TestTemporalCancellationCleansUnreadyAgent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.Env.RegisterDelayedCallback(w.Env.CancelWorkflow, time.Second)
	w.Env.ExecuteWorkflow(pipelinetest.Workflow(w, "cancel", machineRun), struct{}{})
	require.True(t, temporal.IsCanceledError(w.Env.GetWorkflowError()))
	require.Equal(t, "canceled", w.Outcome("run/test-cancel"))
	w.AssertNoLeaks(t)
}

func TestPlainTemporalWorkflowCode(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wf := pipelinetest.Workflow(w, "signals", func(ctx pipeline.Context, _ struct{}) (string, error) {
		if ctx.Recording() {
			return "", nil
		}
		var signal string
		workflow.GetSignalChannel(ctx, "input").Receive(ctx, &signal)
		return signal, nil
	})
	w.Env.RegisterDelayedCallback(func() { w.Env.SignalWorkflow("input", "payload") }, time.Hour)
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var result string
	require.NoError(t, w.Env.GetWorkflowResult(&result))
	require.Equal(t, "payload", result)
}

func callsNamed(w *pipelinetest.World, name string) []pipelinetest.Call {
	var out []pipelinetest.Call
	for _, call := range w.Calls() {
		if call.Name == name {
			out = append(out, call)
		}
	}
	return out
}

// The simulator answers below every other interceptor, so Calls is where an
// activity's outcome is read — on the handler path and the mock path alike.
func TestCallsRecordOutcomes(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	w.Handle("handled", func(workflow.Context, []any) (any, error) { return map[string]int{"rows": 7}, nil })
	w.Handle("refused", func(workflow.Context, []any) (any, error) { return nil, errors.New("disk is full") })
	wf := pipelinetest.Workflow(w, "outcomes", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "machine")
		mocked, err := pa.Activity(ctx, a, pa.Fn("work", dangerous, "input"))
		if err != nil {
			return "", err
		}
		opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		var rows map[string]int
		if err := workflow.ExecuteActivity(opts, "handled").Get(ctx, &rows); err != nil {
			return "", err
		}
		_ = workflow.ExecuteActivity(opts, "refused").Get(ctx, nil)
		return mocked, nil
	})
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("ok", nil).Once()
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())

	mocked := callsNamed(w, "work")
	require.Len(t, mocked, 1)
	require.JSONEq(t, `"ok"`, string(mocked[0].Result))
	require.Empty(t, mocked[0].Err)
	require.False(t, mocked[0].Done.IsZero())

	handled := callsNamed(w, "handled")
	require.Len(t, handled, 1)
	require.JSONEq(t, `{"rows":7}`, string(handled[0].Result))

	refused := callsNamed(w, "refused")
	require.Len(t, refused, 1)
	require.Contains(t, refused[0].Err, "disk is full")
	require.Empty(t, refused[0].Result)
	require.False(t, refused[0].Canceled)

	// One counter orders dispatches and completions: every call is done
	// after it was sent, and no two events share a number.
	seen := map[int64]bool{}
	for _, call := range w.Calls() {
		require.Greater(t, call.DoneSeq, call.Seq, "%s", call.Name)
		for _, n := range []int64{call.Seq, call.DoneSeq} {
			require.False(t, seen[n], "sequence number %d used twice", n)
			seen[n] = true
		}
	}
}

// Virtual time stands still inside a workflow task: sequential activities all
// carry one Time, and only the counter tells which ended before the next began.
func TestCallsOrderEventsOfOneVirtualInstant(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for _, name := range []string{"first", "second", "third"} {
		w.Handle(name, func(workflow.Context, []any) (any, error) { return name, nil })
	}
	wf := pipelinetest.Workflow(w, "instant", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		for _, name := range []string{"first", "second"} {
			if err := workflow.ExecuteActivity(opts, name).Get(ctx, nil); err != nil {
				return false, err
			}
		}
		// Two in flight at once: both are sent before either is done.
		a, b := workflow.ExecuteActivity(opts, "third"), workflow.ExecuteActivity(opts, "third")
		return true, errors.Join(a.Get(ctx, nil), b.Get(ctx, nil))
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())

	first, second, third := callsNamed(w, "first")[0], callsNamed(w, "second")[0], callsNamed(w, "third")
	require.Equal(t, first.Time, second.Time, "the test is about events sharing one instant")
	require.Less(t, first.DoneSeq, second.Seq, "sequential: the first ended before the second was sent")
	require.Len(t, third, 2)
	require.Less(t, third[1].Seq, third[0].DoneSeq, "concurrent: the second was sent before the first ended")
}

// Attempts happen under the future: a retried dispatch is ONE call, and its
// outcome is the final one.
func TestCallsFoldRetriesIntoOneCall(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.ConnectAfter("machine", 0)
	wf := pipelinetest.Workflow(w, "folded", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "machine")
		return pa.Activity(ctx, a, pa.Fn("work", dangerous, "input"))
	})
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("", errors.New("transient")).Once()
	w.OnAgentActivity("machine", "work", mock.Anything, "input").Return("ok", nil).Once()
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	calls := callsNamed(w, "work")
	require.Len(t, calls, 1)
	require.JSONEq(t, `"ok"`, string(calls[0].Result))
	require.Empty(t, calls[0].Err)
}

// A cancelled future says so; a call the run never waited out stays pending.
func TestCallsMarkCancellation(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.Handle("slow", func(ctx workflow.Context, _ []any) (any, error) {
		return nil, workflow.Sleep(ctx, time.Hour)
	})
	w.Env.RegisterDelayedCallback(w.Env.CancelWorkflow, time.Second)
	wf := pipelinetest.Workflow(w, "cancelled", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Hour})
		return false, workflow.ExecuteActivity(opts, "slow").Get(ctx, nil)
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.True(t, temporal.IsCanceledError(w.Env.GetWorkflowError()))
	slow := callsNamed(w, "slow")
	require.Len(t, slow, 1)
	require.True(t, slow[0].Canceled)
	require.NotEmpty(t, slow[0].Err)
	require.Equal(t, time.Second, slow[0].Done.Sub(slow[0].Time))
}

// A holding names the run that handed it over: whoever manages the stand
// tells one run's leftovers from the next's.
func TestToStandNamesTheHolder(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wf := pipelinetest.Workflow(w, "holder", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		a := pipeline.NewArtifact(ctx, "report", artifact.FromBytes([]byte("x")))
		a.Ready(ctx)
		pipeline.ToStand(ctx, a, pipeline.KeepFor(time.Hour))
		return true, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	r, ok := w.Resource("artifact/report")
	require.True(t, ok)
	require.Equal(t, ref.OwnerRef("stand/holder"), r.Owner)
	require.Equal(t, ref.OwnerRef("run/test-holder"), r.From)
}

// A test that does not know the names a RunSpec will produce meets them as
// they are declared — and can list what the run left behind.
func TestOnDeclareMeetsUnnamedRecords(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	var declared []ref.OwnerRef
	w.OnDeclare(func(r pipelinetest.Resource) {
		declared = append(declared, r.Ref)
		if strings.HasPrefix(string(r.Ref), "agent/") {
			w.Connect(id.AgentId(strings.TrimPrefix(string(r.Ref), "agent/")))
		}
	})
	wf := pipelinetest.Workflow(w, "meet", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		a := pipeline.NewAgent(ctx, "box-7")
		a.Ready(ctx)
		return true, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError(), "the hook connected the agent nobody named in the test")
	require.Equal(t, []ref.OwnerRef{"agent/box-7"}, declared)
	all := w.Resources()
	require.Len(t, all, 1)
	require.Equal(t, ref.OwnerRef("agent/box-7"), all[0].Ref)
	require.Equal(t, "deleted", all[0].Phase, "Resources lists deleted records too")
}

// A stand's TTL runs in virtual time: a holding expires while the run still
// goes, the way it does on a real stand.
func TestStandTTLExpiresDuringTheRun(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	var midway string
	wf := pipelinetest.Workflow(w, "ttl", func(ctx pipeline.Context, _ struct{}) (bool, error) {
		a := pipeline.NewArtifact(ctx, "report", artifact.FromBytes([]byte("x")))
		a.Ready(ctx)
		pipeline.ToStand(ctx, a, pipeline.KeepFor(time.Hour))
		if err := workflow.Sleep(ctx, 2*time.Hour); err != nil {
			return false, err
		}
		r, _ := w.Resource("artifact/report")
		midway = r.Phase
		return true, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	require.Equal(t, "deleted", midway, "the holding expired an hour into a two-hour run")
	w.AssertNoLeaks(t)
}

// A failed run keeps what it collected: the partial result rides in the
// failure's details, the error stays readable and its cause stays wrapped.
func TestFailedRunCarriesPartialResult(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	type result struct {
		Passed int `json:"passed"`
	}
	sentinel := errors.New("bench exploded")
	wf := pipelinetest.Workflow(w, "partial", func(ctx pipeline.Context, _ struct{}) (result, error) {
		return result{Passed: 3}, sentinel
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	err := w.Env.GetWorkflowError()
	require.Error(t, err)
	var app *temporal.ApplicationError
	require.ErrorAs(t, err, &app)
	require.Equal(t, pipeline.FailureType, app.Type())
	require.Contains(t, app.Message(), "bench exploded")
	var partial result
	require.NoError(t, app.Details(&partial))
	require.Equal(t, 3, partial.Passed)
	require.Equal(t, "failure", w.Outcome("run/test-partial"))
}
