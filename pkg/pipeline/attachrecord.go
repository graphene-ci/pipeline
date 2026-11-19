package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// Stable error types of an attach that finds nothing to attach to.
const (
	// CodeRecordMissing — no record of that name was ever declared.
	CodeRecordMissing = "record_missing"
	// CodeRecordGone — the record failed or is being deleted.
	CodeRecordGone = "record_gone"
)

// attachPoll is how often a still-creating record is looked at again.
const attachPoll = 2 * time.Second

// AwaitRecord recognizes an EXISTING library record — a container or a
// directory a stand keeps — and returns its state once it is ready. It
// never creates: a missing record is CodeRecordMissing, a failed or
// departing one CodeRecordGone, both final. Libraries call it from the
// activity behind their Attach constructors.
func AwaitRecord[Spec, State any](ctx context.Context, cl *entclient.Client[Spec, State], kind string, name entity.ResourceID) (State, error) {
	var zero State
	for {
		desc, err := cl.Describe(ctx, name)
		var notFound *serviceerror.NotFound
		switch {
		case errors.As(err, &notFound):
			return zero, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("%s/%s does not exist; attach finds records, it never creates them", kind, name), CodeRecordMissing, nil)
		case err != nil:
			return zero, err
		}
		switch desc.Phase {
		case entity.PhaseReady:
			return desc.State, nil
		case entity.PhaseCreateFailed, entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return zero, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("%s/%s is %s", kind, name, desc.Phase), CodeRecordGone, nil)
		case entity.PhaseCreating:
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, string(desc.Phase))
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(attachPoll):
		}
	}
}
