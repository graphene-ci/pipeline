package leak

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

func finalizing(ctx workflow.Context) error {
	Report(ctx, errors.New("daemon gone"))
	return nil
}

func TestReport(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		queue    string
		serverUp bool
		want     *wire.ReportLeakRequest
	}{
		{
			name: "a machine record reports to its agent", queue: wire.AgentRunQueue("db", "run-1"), serverUp: true,
			want: &wire.ReportLeakRequest{Agent: "db", Ref: "docker/pg", Error: "daemon gone"},
		},
		{name: "a record off any machine only logs", queue: wire.RunQueue("run-1"), serverUp: true},
		{name: "an unreachable server does not fail the finalize", queue: wire.AgentRunQueue("db", "run-1")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(finalizing)
			var got *wire.ReportLeakRequest
			env.RegisterActivityWithOptions(func(_ context.Context, req wire.ReportLeakRequest) error {
				if !c.serverUp {
					return errors.New("unavailable")
				}
				got = &req
				return nil
			}, activity.RegisterOptions{Name: wire.ReportLeakActivity})
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "docker/pg", TaskQueue: c.queue})
			env.ExecuteWorkflow(finalizing)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("the finalize must not fail: %v", err)
			}
			if (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
				t.Fatalf("reported %+v, want %+v", got, c.want)
			}
		})
	}
}
