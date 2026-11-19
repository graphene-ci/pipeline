package id

import "testing"

func TestValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{name: "agent ok", err: AgentId("db-1.example").Validate()},
		{name: "run ok", err: RunId("run-42-cell").Validate()},
		{name: "pipeline ok", err: PipelineId("stroppy").Validate()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.err != nil {
				t.Fatalf("valid id refused: %v", c.err)
			}
		})
	}
	bad := []struct {
		name string
		err  error
	}{
		{name: "empty agent", err: AgentId("").Validate()},
		{name: "whitespace run", err: RunId("run 1").Validate()},
		{name: "slash agent", err: AgentId("a/run/b").Validate()},
		{name: "slash run", err: RunId("r/1").Validate()},
		{name: "slash pipeline", err: PipelineId("team/app").Validate()},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.err == nil {
				t.Fatal("invalid id accepted")
			}
		})
	}
}

func BenchmarkValidate(b *testing.B) {
	for b.Loop() {
		_ = AgentId("db-1.example").Validate()
	}
}
