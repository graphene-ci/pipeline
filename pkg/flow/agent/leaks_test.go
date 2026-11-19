package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// step is one command sent to a live agent record and what it must do.
type step struct {
	name    string
	payload any
	count   int
	fails   bool
}

// runLeaks drives a connected agent record through steps and returns the
// leaks it holds at the end.
func runLeaks(t *testing.T, steps []step) []Leak {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if err := Definition(Options{}).Register(env); err != nil {
		t.Fatal(err)
	}
	env.RegisterActivityWithOptions(func(context.Context, id.AgentId) (AgentStatus, error) {
		return AgentStatus{Connected: true}, nil
	}, activity.RegisterOptions{Name: AgentStatusActivity})
	for i, s := range steps {
		env.RegisterDelayedCallback(func() {
			payload, err := json.Marshal(s.payload)
			if err != nil {
				t.Fatal(err)
			}
			env.UpdateWorkflow(s.name, fmt.Sprint(i), &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					if !s.fails {
						t.Errorf("step %d %s rejected: %v", i, s.name, err)
					}
				},
				OnComplete: func(value any, err error) {
					if (err != nil) != s.fails {
						t.Errorf("step %d %s: err=%v, want fails=%v", i, s.name, err, s.fails)
						return
					}
					if err != nil {
						return
					}
					data, _ := json.Marshal(value)
					var res LeaksRes
					if err := json.Unmarshal(data, &res); err != nil {
						t.Fatal(err)
					}
					if res.Count != s.count {
						t.Errorf("step %d %s: count %d, want %d", i, s.name, res.Count, s.count)
					}
				},
			}, map[string]any{"requestId": fmt.Sprint(i), "payload": json.RawMessage(payload)})
		}, time.Duration(i+1)*time.Second)
	}
	var leaks []Leak
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow("describe")
		if err != nil {
			t.Fatal(err)
		}
		var described struct{ State State }
		if err := value.Get(&described); err != nil {
			t.Fatal(err)
		}
		leaks = described.State.Leaks
		env.CancelWorkflow()
	}, time.Duration(len(steps)+1)*time.Second)
	env.ExecuteWorkflow(string(Kind), map[string]any{"spec": pipeline.AgentSpec{}})
	return leaks
}

func TestLeaks(t *testing.T) {
	t.Parallel()
	report := func(ref, err string) any { return ReportLeakCmd{Ref: ref, Error: err} }
	cases := []struct {
		name  string
		steps []step
		want  []string
	}{
		{
			name: "reports accumulate by record",
			steps: []step{
				{name: "report-leak", payload: report("docker/pg", "daemon gone"), count: 1},
				{name: "report-leak", payload: report("host-dir/m-data", "busy"), count: 2},
				{name: "report-leak", payload: report("docker/pg", "still gone"), count: 2},
			},
			want: []string{"docker/pg=still gone", "host-dir/m-data=busy"},
		},
		{
			name: "clear one",
			steps: []step{
				{name: "report-leak", payload: report("docker/pg", "x"), count: 1},
				{name: "report-leak", payload: report("file/m-cfg", "y"), count: 2},
				{name: "clear-leaks", payload: ClearLeaksCmd{Ref: "docker/pg"}, count: 1},
			},
			want: []string{"file/m-cfg=y"},
		},
		{
			name: "clear all",
			steps: []step{
				{name: "report-leak", payload: report("docker/pg", "x"), count: 1},
				{name: "report-leak", payload: report("file/m-cfg", "y"), count: 2},
				{name: "clear-leaks", payload: ClearLeaksCmd{}, count: 0},
			},
		},
		{
			name: "clearing an unknown leak is refused",
			steps: []step{
				{name: "report-leak", payload: report("docker/pg", "x"), count: 1},
				{name: "clear-leaks", payload: ClearLeaksCmd{Ref: "docker/other"}, fails: true},
			},
			want: []string{"docker/pg=x"},
		},
		{
			name:  "a leak names its record",
			steps: []step{{name: "report-leak", payload: report("", "x"), fails: true}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			for _, l := range runLeaks(t, c.steps) {
				if l.At.IsZero() {
					t.Errorf("leak %s has no time", l.Ref)
				}
				got = append(got, l.Ref+"="+l.Error)
			}
			if fmt.Sprint(got) != fmt.Sprint(c.want) {
				t.Fatalf("leaks %v, want %v", got, c.want)
			}
		})
	}
}
