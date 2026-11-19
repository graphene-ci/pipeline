package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// The record's policy decides what its deletion does to the agent on the
// machine: nothing, a bounded try, or a try until it is done.
func TestDecommissionOnDelete(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		policy   pipeline.Decommission
		failures int
		calls    int
		remains  bool
	}{
		{name: "none leaves the agent", policy: pipeline.DecommissionNone},
		{name: "best effort succeeds", policy: pipeline.DecommissionBestEffort, calls: 1},
		{name: "best effort gives up", policy: pipeline.DecommissionBestEffort, failures: 1000, remains: true},
		{name: "guaranteed retries until done", policy: pipeline.DecommissionGuaranteed, failures: 6, calls: 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			if err := Definition(Options{}).Register(env); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivityWithOptions(func(context.Context, id.AgentId) (AgentStatus, error) {
				return AgentStatus{Connected: true}, nil
			}, activity.RegisterOptions{Name: AgentStatusActivity})
			calls := 0
			env.RegisterActivityWithOptions(func(_ context.Context, agent id.AgentId, ssh *pipeline.SSHInstall) error {
				calls++
				if agent != "manual-1" || ssh == nil || ssh.Address != "10.0.0.5" {
					return errors.New("wrong machine")
				}
				if calls <= c.failures {
					return errors.New("machine unreachable")
				}
				return nil
			}, activity.RegisterOptions{Name: DecommissionActivity})
			env.RegisterActivityWithOptions(func(context.Context, id.AgentId, pipeline.SSHInstall) error { return nil },
				activity.RegisterOptions{Name: InstallSSHActivity})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, time.Minute)
			spec := pipeline.AgentSpec{Decommission: c.policy, SSH: &pipeline.SSHInstall{Address: "10.0.0.5", User: "root", KeyRef: ref.SecretRef{Name: "key"}, HostKey: "ssh-ed25519 AAAA"}}
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent/manual-1"})
			env.ExecuteWorkflow(string(Kind), map[string]any{"spec": spec})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			value, err := env.QueryWorkflow("describe")
			if err != nil {
				t.Fatal(err)
			}
			var described struct{ State State }
			if err := value.Get(&described); err != nil {
				t.Fatal(err)
			}
			final := described.State
			if c.policy != pipeline.DecommissionBestEffort || c.failures == 0 {
				if calls != c.calls {
					t.Fatalf("decommission called %d times, want %d", calls, c.calls)
				}
			}
			if final.AgentMayRemain != c.remains || final.AgentConnected {
				t.Fatalf("state remains=%v connected=%v, want remains=%v", final.AgentMayRemain, final.AgentConnected, c.remains)
			}
		})
	}
}
