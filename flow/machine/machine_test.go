package machine

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
