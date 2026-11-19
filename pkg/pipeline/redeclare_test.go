package pipeline

import (
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

type testSpec struct {
	Image string            `json:"image"`
	Env   map[string]string `json:"env,omitempty"`
	Owner ref.OwnerRef      `json:"owner,omitempty"`
	Flows []ownership.Flow  `json:"flows,omitempty"`
}

func TestCheckRedeclare(t *testing.T) {
	t.Parallel()
	base := testSpec{Image: "postgres:17", Env: map[string]string{"A": "1"}, Owner: "stand/lab"}
	cases := []struct {
		name      string
		requested testSpec
		mismatch  bool
	}{
		{name: "same spec, another owner", requested: testSpec{Image: "postgres:17", Env: map[string]string{"A": "1"}, Owner: "run/r2"}},
		{name: "same spec, flows added", requested: testSpec{Image: "postgres:17", Env: map[string]string{"A": "1"}, Flows: []ownership.Flow{{To: "x"}}}},
		{name: "another image", requested: testSpec{Image: "postgres:18", Env: map[string]string{"A": "1"}}, mismatch: true},
		{name: "another env", requested: testSpec{Image: "postgres:17", Env: map[string]string{"A": "2"}}, mismatch: true},
		{name: "env dropped", requested: testSpec{Image: "postgres:17"}, mismatch: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := CheckRedeclare("docker", "pg", base, c.requested)
			if !c.mismatch {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != CodeSpecMismatch || !app.NonRetryable() {
				t.Fatalf("err = %v, want a non-retryable %s", err, CodeSpecMismatch)
			}
		})
	}
}

func BenchmarkCheckRedeclare(b *testing.B) {
	s := testSpec{Image: "postgres:17", Env: map[string]string{"A": "1", "B": "2"}, Owner: "stand/lab"}
	for b.Loop() {
		if err := CheckRedeclare("docker", "pg", s, s); err != nil {
			b.Fatal(err)
		}
	}
}
