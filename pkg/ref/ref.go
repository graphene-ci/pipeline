// Package ref holds reference types: values that stand for something
// stored elsewhere. A reference is what travels through specs, workflow
// history, and logs — the referent (secret value, blob bytes, owning
// record) is resolved at the point of use and never travels back.
package ref

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// SecretRef names a secret. The value is resolved by the consumer (agent
// or worker) at the moment of use; it never appears in specs, logs, or
// Temporal history. On the wire the reference is the bare NAME — a
// params field typed SecretRef is a plain string in JSON, the schema
// marks it secret, and the door checks the name exists before a run
// starts.
type SecretRef struct {
	Name id.SecretId `json:"name"`
}

// Secret builds a reference by name — the declaration-side constructor
// (trigger params, specs written outside a run).
func Secret(name string) SecretRef { return SecretRef{Name: id.SecretId(name)} }

// MarshalJSON writes the bare name.
func (r SecretRef) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(r.Name))
}

// UnmarshalJSON accepts the bare name, and the legacy {"name": ...}
// object still present in stored specs.
func (r *SecretRef) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		r.Name = id.SecretId(s)
		return nil
	}
	var obj struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("secret ref: want a name string, got %s", b)
	}
	r.Name = id.SecretId(obj.Name)
	return nil
}

// BlobRef points at bytes in external storage. Digest pins the content;
// Location says where to fetch it.
type BlobRef struct {
	Digest   string `json:"digest"`
	Location string `json:"location"`
	Size     int64  `json:"size,omitempty"`
}

// OwnerRef points at the owning record as "kind/id" (an entity workflow
// address). Ownership can be given away, never taken; the owner must have
// a reason to die.
type OwnerRef string

// RunOwner builds the owner reference for a run.
func RunOwner(r id.RunId) OwnerRef { return OwnerRef("run/" + string(r)) }

// Validate reports whether the owner reference is well-formed.
func (o OwnerRef) Validate() error {
	s := string(o)
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return fmt.Errorf("owner ref %q: want kind/id", s)
	}
	return nil
}
