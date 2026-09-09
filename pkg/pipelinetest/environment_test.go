package pipelinetest_test

import (
	"context"
	"errors"
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
