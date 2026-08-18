package pipeline

import (
	"errors"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

// SSHInstall asks the system to put the agent on a machine that already
// exists, over ssh. This is the ONLY case where the machine record acts;
// in every other case the machine is created by whatever the user chose
// (crossplane through a resource library, a person, a cloud console) and
// the record just waits for its agent.
type SSHInstall struct {
	// Address is host:port; port may be omitted, then it is 22.
	Address string `json:"address"`
	// User to log in as.
	User string `json:"user"`
	// KeyRef names the secret holding the private key.
	KeyRef ref.SecretRef `json:"keyRef"`
	// HostKey is the machine's public key (one line of known_hosts).
	// Required, deliberately: trust-on-first-use is what a person at a
	// terminal does; this is a control plane opening a root shell and
	// feeding it a script with an installation token in it.
	HostKey string `json:"hostKey"`
}

// Validate checks the install request structurally.
func (s SSHInstall) Validate() error {
	if s.Address == "" || s.User == "" {
		return errors.New("ssh install requires address and user")
	}
	if s.KeyRef.Name == "" {
		return errors.New("ssh install requires a key secret name")
	}
	if s.HostKey == "" {
		return errors.New("ssh install requires the machine host key")
	}
	return nil
}

// MachineSpec is the desired state of a machine record. The record is a
// LINK between a real machine and its agent: it never creates machines.
// With SSH set, the system installs the agent over ssh first; otherwise
// the record simply waits for the agent to connect (a fresh VM brings the
// agent through its user-data — see AgentUserData).
type MachineSpec struct {
	SSH *SSHInstall `json:"ssh,omitempty"`

	Owner  ref.OwnerRef      `json:"owner,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Validate checks the spec structurally (deterministic).
func (s MachineSpec) Validate() error {
	if s.SSH != nil {
		if err := s.SSH.Validate(); err != nil {
			return err
		}
	}
	if s.Owner != "" {
		return s.Owner.Validate()
	}
	return nil
}

// MachineState is the observed state of a machine record: what the agent
// reported about the real machine behind it.
type MachineState struct {
	Addresses      []string `json:"addresses,omitempty"`
	AgentConnected bool     `json:"agentConnected"`
	// FactsDigest references the machine facts blob; the facts themselves
	// live outside the record.
	FactsDigest string `json:"factsDigest,omitempty"`
}
