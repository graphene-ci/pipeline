package pipeline

import (
	"errors"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

// CloudSource asks graphene to create the machine in a cloud. The record
// owns it: deleting the record destroys the machine.
type CloudSource struct {
	Provider string `json:"provider"`
	// Params are provider-specific creation parameters, interpreted by the
	// provider implementation in the server.
	Params map[string]string `json:"params,omitempty"`
}

// SSHSource asks graphene to recognize an existing machine over ssh. The
// record does not own it: nothing is created and nothing will ever be
// destroyed.
type SSHSource struct {
	Host   string        `json:"host"`
	Port   int           `json:"port,omitempty"`
	User   string        `json:"user"`
	KeyRef ref.SecretRef `json:"keyRef"`
}

// MachineSpec is the desired state of a machine record. Exactly one
// source must be set.
type MachineSpec struct {
	Cloud *CloudSource `json:"cloud,omitempty"`
	SSH   *SSHSource   `json:"ssh,omitempty"`

	Owner  ref.OwnerRef      `json:"owner,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Validate checks the spec structurally (deterministic).
func (s MachineSpec) Validate() error {
	if (s.Cloud == nil) == (s.SSH == nil) {
		return errors.New("exactly one of cloud or ssh must be set")
	}
	if s.SSH != nil && (s.SSH.Host == "" || s.SSH.User == "") {
		return errors.New("ssh source requires host and user")
	}
	if s.Cloud != nil && s.Cloud.Provider == "" {
		return errors.New("cloud source requires provider")
	}
	if s.Owner != "" {
		return s.Owner.Validate()
	}
	return nil
}

// Owned reports whether graphene owns the machine (may destroy it).
func (s MachineSpec) Owned() bool { return s.Cloud != nil }

// MachineState is the observed state of a machine record.
type MachineState struct {
	Addresses      []string `json:"addresses,omitempty"`
	AgentConnected bool     `json:"agentConnected"`
	// FactsDigest references the machine facts blob; the facts themselves
	// live outside the record.
	FactsDigest string `json:"factsDigest,omitempty"`
}
