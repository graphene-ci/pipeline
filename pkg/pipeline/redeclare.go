package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"

	"go.temporal.io/sdk/temporal"
)

// CodeSpecMismatch is the stable error type of a redeclaration refused
// because the record already exists with a different spec.
const CodeSpecMismatch = "spec_mismatch"

// CheckRedeclare guards the reuse of a live record by name. Declaring a
// name that exists attaches to the record — and the record keeps the spec
// it was created with. A redeclaration with a DIFFERENT spec (another
// image, another config) would otherwise run the old one silently, so it
// is refused with CodeSpecMismatch. Replacing the record is a deletion
// through the API — an act the installation's rights govern — after which
// the next declaration creates it anew. existing and requested are the
// record's stored spec and the declared one, of the same type.
func CheckRedeclare(kind, name string, existing, requested any) error {
	have, err := comparable(existing)
	if err != nil {
		return err
	}
	want, err := comparable(requested)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(have, want) {
		return nil
	}
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(
		"%s/%s already exists with another spec; declare it the same way, or delete the record to replace it",
		kind, name), CodeSpecMismatch, nil)
}

// comparable is a spec as plain JSON values without its tree fields.
func comparable(spec any) (map[string]any, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	// The record's place in the tree is not what it is: a record a stand
	// keeps has another owner than the run that declares it again.
	delete(out, "owner")
	delete(out, "flows")
	return out, nil
}
