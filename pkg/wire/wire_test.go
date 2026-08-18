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
