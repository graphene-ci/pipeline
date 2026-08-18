// Package artifact declares artifact SOURCES: where the bytes of an
// artifact are is part of its declaration; the upload — an activity on
// the right execution site — is the library's business, not the user's.
package artifact

import (
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Agent is the slice of an agent handle a source needs — satisfied by
// pipeline.AgentHandle.
type Agent interface {
	AgentId() id.AgentId
}

// Source says where the artifact's bytes are.
type Source struct {
	// AgentFile: the bytes are a file on the agent's machine; the upload
	// activity runs inside the per-(agent × run) container.
	AgentFile *AgentFileSource
	// Bytes: the run computed the bytes itself; the upload activity runs
	// on the run's worker.
	Bytes []byte
}

// AgentFileSource locates a file on a machine.
type AgentFileSource struct {
	AgentId id.AgentId
	Path    string
}

// FromAgentFile declares bytes lying in a file on the agent's machine.
func FromAgentFile(agent Agent, path string) Source {
	return Source{AgentFile: &AgentFileSource{AgentId: agent.AgentId(), Path: path}}
}

// FromBytes declares bytes the run computed itself.
func FromBytes(b []byte) Source {
	return Source{Bytes: b}
}
