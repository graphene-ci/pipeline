package pipeline

import (
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
)

// paramsValidator is the shared go-playground/validator engine: it reads
// the `validate:"..."` field tags. The same tag vocabulary the manifest
// maps into the schema (so the door and the UI enforce what they can) is
// enforced here in full at run start — including whatever the schema
// mapping does not yet cover.
var paramsValidator = validator.New(validator.WithRequiredStructEnabled())

// Validatable is implemented by a Params type whose validity needs
// CROSS-FIELD or otherwise imperative checks the schema cannot express
// (e.g. "end must be after start"). It runs at run START — the door has
// already checked the schema by then — and a returned error fails the run
// at once, before any resource is touched. Prefer schema/tag constraints
// (the door catches those before a container even starts); reach for
// Validate only for what they cannot say.
type Validatable interface {
	Validate() error
}

// checkParams enforces, at the start of a run, everything the door's
// schema pass could not: the full validator tag set on the fields, then
// the type's own Validate() if it has one.
func checkParams(p any) error {
	if err := paramsValidator.Struct(p); err != nil {
		// A non-struct Params (a map, a slice) has no field tags to check —
		// that is not a validation failure, just nothing to do here.
		var invalid *validator.InvalidValidationError
		if !errors.As(err, &invalid) {
			return fmt.Errorf("params: %w", err)
		}
	}
	if v, ok := p.(Validatable); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("params: %w", err)
		}
	}
	return nil
}
