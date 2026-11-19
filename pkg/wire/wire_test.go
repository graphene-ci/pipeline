package wire

import "testing"

func TestQueueNames(t *testing.T) {
	if got := AgentRunQueue("m-1", "r-1"); got != "agent/m-1/run/r-1" {
		t.Fatalf("AgentRunQueue: %q", got)
	}
	if got := RunQueue("r-1"); got != "run/r-1" {
		t.Fatalf("RunQueue: %q", got)
	}
}

func TestParseAgentRunQueue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		queue      string
		agent, run string
		ok         bool
	}{
		{queue: AgentRunQueue("m-1", "r-1"), agent: "m-1", run: "r-1", ok: true},
		{queue: "agent/db.example/run/run-42-cell", agent: "db.example", run: "run-42-cell", ok: true},
		{queue: RunQueue("r-1")},
		{queue: ServerQueue},
		{queue: "agent//run/r"},
		{queue: "agent/m/run/"},
		{queue: "agent/a/run/b/run/c"},
		{queue: "agent/a/b/run/c"},
	}
	for _, c := range cases {
		t.Run(c.queue, func(t *testing.T) {
			t.Parallel()
			agent, run, ok := ParseAgentRunQueue(c.queue)
			if ok != c.ok || string(agent) != c.agent || string(run) != c.run {
				t.Fatalf("got %q %q %v", agent, run, ok)
			}
		})
	}
}

func BenchmarkParseAgentRunQueue(b *testing.B) {
	q := AgentRunQueue("machine-1", "run-1")
	for b.Loop() {
		ParseAgentRunQueue(q)
	}
}

// The TypeScript SDK and Studio send this exact header; the value is wire.
func TestNamespaceHeader(t *testing.T) {
	t.Parallel()
	if NamespaceHeader != "x-graphene-namespace" {
		t.Fatalf("NamespaceHeader: %q", NamespaceHeader)
	}
}
