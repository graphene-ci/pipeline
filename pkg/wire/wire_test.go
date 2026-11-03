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

// The TypeScript SDK and Studio send this exact header; the value is wire.
func TestNamespaceHeader(t *testing.T) {
	t.Parallel()
	if NamespaceHeader != "x-graphene-namespace" {
		t.Fatalf("NamespaceHeader: %q", NamespaceHeader)
	}
}
