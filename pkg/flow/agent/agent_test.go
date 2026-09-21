package agent

import (
	"testing"

	"go.temporal.io/sdk/workflow"
)

type fakeRegistry struct{ names []string }

func (f *fakeRegistry) RegisterWorkflow(any) {}
func (f *fakeRegistry) RegisterWorkflowWithOptions(_ any, opts workflow.RegisterOptions) {
	f.names = append(f.names, opts.Name)
}
func (f *fakeRegistry) RegisterDynamicWorkflow(any, workflow.DynamicRegisterOptions) {}

func TestDefinitionRegisters(t *testing.T) {
	reg := &fakeRegistry{}
	if err := Definition(Options{}).Register(reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(reg.names) != 1 || reg.names[0] != string(Kind) {
		t.Fatalf("registered names: %v", reg.names)
	}
}

// A deleted record is still read by name: it must not go on saying that it
// can take work. What the machine was stays as history.
func TestFinalizeClosesTheRecord(t *testing.T) {
	st := State{}
	st.AgentConnected = true
	st.Addresses = []string{"10.10.0.7"}
	if err := finalizeMachine(nil, &st); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if st.AgentConnected {
		t.Fatal("a deleted record still says connected")
	}
	if len(st.Addresses) != 1 {
		t.Fatalf("history was wiped: %v", st.Addresses)
	}
}
