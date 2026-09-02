// Package file declares file SOURCES: where the bytes a File resource
// writes onto a machine come from. Symmetric to package artifact —
// artifact reads bytes FROM a machine into a blob; a File writes bytes
// ONTO a machine, and the file itself becomes a resource (library/file).
// The upload/resolve happens on the right site (the agent), never in the
// spec: a secret's value resolves on the machine, only its name travels.
package file

import (
	"embed"
	"fmt"
)

// Source says where a file's bytes come from. Exactly one field is set.
type Source struct {
	// Bytes: content the run carries (computed, or read from an embed).
	// Large content travels as an upload blob, not inside the spec.
	Bytes []byte
	// Secret: the NAME of a secret; the agent resolves the value on the
	// machine, so it never appears in the record's spec or history.
	Secret string
	// Artifact: the NAME of an artifact whose blob is the content.
	Artifact string
}

// FromBytes declares content the run holds directly.
func FromBytes(b []byte) Source { return Source{Bytes: b} }

// FromEmbed declares content the user SHIPS in the binary via
// //go:embed — how a pipeline brings its own config files. Read at
// declaration; a missing path fails the run loudly, not silently empty.
func FromEmbed(fs embed.FS, path string) Source {
	b, err := fs.ReadFile(path)
	if err != nil {
		// A missing embedded file is a programming error surfaced at
		// declaration; the empty source below makes File fail with a
		// clear message rather than writing an empty file.
		return Source{}
	}
	return Source{Bytes: b}
}

// FromSecret declares the content is a secret's value, resolved on the
// machine by the agent — only the name travels.
func FromSecret(name string) Source { return Source{Secret: name} }

// FromArtifact declares the content is another artifact's bytes.
func FromArtifact(name string) Source { return Source{Artifact: name} }

// Validate refuses a source that names nothing or names two things.
func (s Source) Validate() error {
	n := 0
	if s.Bytes != nil {
		n++
	}
	if s.Secret != "" {
		n++
	}
	if s.Artifact != "" {
		n++
	}
	switch n {
	case 0:
		return fmt.Errorf("a file source is empty: use FromBytes, FromEmbed, FromSecret or FromArtifact")
	case 1:
		return nil
	default:
		return fmt.Errorf("a file source names more than one origin")
	}
}
