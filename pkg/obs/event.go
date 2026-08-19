package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/graphene-ci/pipeline/pkg/workerapi"
)

// Event puts a domain event into the history of the entity the context
// works on (WithEntity) — the plane of TRUTH: durable, replayed by
// Events(ref), no telemetry delivery involved. A milestone, not a log
// line: every event is a history event and costs the entity's
// Continue-as-New budget; streams belong in Info/Pipe.
func Event(ctx context.Context, name string, payload any) error {
	ref := Entity(ctx)
	if ref == "" {
		return errors.New("obs.Event: no entity on the context (WithEntity) — use EventFor")
	}
	return EventFor(ctx, ref, name, payload)
}

// EventFor is Event with an explicit entity ref.
func EventFor(ctx context.Context, ref, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("obs.Event %s: %w", name, err)
	}
	return workerapi.EmitEvent(ctx, ref, name, raw)
}
