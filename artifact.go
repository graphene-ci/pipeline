package pipeline

import (
	"errors"

	"github.com/graphene-ci/pipeline/ref"
)

// ArtifactSpec is the desired state of an artifact record: a durable
// record about bytes stored elsewhere. Deleting an owned record deletes
// its bytes too.
type ArtifactSpec struct {
	Blob      ref.BlobRef       `json:"blob"`
	MediaType string            `json:"mediaType,omitempty"`
	Owner     ref.OwnerRef      `json:"owner,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Validate checks the spec structurally.
func (s ArtifactSpec) Validate() error {
	if s.Blob.Digest == "" || s.Blob.Location == "" {
		return errors.New("artifact requires blob digest and location")
	}
	if s.Owner != "" {
		return s.Owner.Validate()
	}
	return nil
}

// ArtifactState is the observed state of an artifact record.
type ArtifactState struct {
	Verified bool `json:"verified"`
}
