// Package artifact holds the temporal flow of the Artifact system
// resource. The user-facing types live in the pipeline root package; side
// effects sit behind Ops, implemented by the graphene server.
package artifact

import (
	"errors"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Kind is the entity kind name; workflow IDs are "artifact/{artifact-id}".
const Kind = entity.KindName("artifact")

// State extends the shared observable state with teardown data the
// finalizer needs and the owned half of the tree.
type State struct {
	pipeline.ArtifactState
	ownership.State
	Blob *ref.BlobRef `json:"blob,omitempty"`
}

// Ops is the side-effect boundary: the blob store behind the records.
// Implemented by the server; every method idempotent.
type Ops interface {
	// Stat checks that the blob exists and matches the digest.
	Stat(artifactId id.ArtifactId, blob ref.BlobRef) (exists bool, err error)
	// Delete removes the bytes; not-found is not an error.
	Delete(artifactId id.ArtifactId, blob ref.BlobRef) error
}

// Activity names (registered by the server against its Ops).
const (
	StatActivity   = "artifact.stat"
	DeleteActivity = "artifact.delete"
)

// Definition builds the artifact entity definition.
func Definition() *entdefine.Definition[pipeline.ArtifactSpec, State] {
	def := entdefine.New[pipeline.ArtifactSpec, State](Kind,
		entdefine.WithInit[pipeline.ArtifactSpec, State](initArtifact),
		entdefine.WithFinalize[pipeline.ArtifactSpec, State](finalizeArtifact),
		entdefine.WithSearchAttributes[pipeline.ArtifactSpec, State](true),
	)
	ownership.Register(def, func(st *State) *ownership.State { return &st.State })
	return def
}

func initArtifact(ctx workflow.Context, spec pipeline.ArtifactSpec) (State, error) {
	var st State
	var exists bool
	if err := workflow.ExecuteActivity(activityCtx(ctx), StatActivity, artifactId(ctx), spec.Blob).Get(ctx, &exists); err != nil {
		return st, err
	}
	if !exists {
		return st, errors.New("blob not found at location")
	}
	st.Verified = true
	st.ArtifactState.Blob = spec.Blob
	st.Blob = &spec.Blob
	ownership.Init(ctx, &st.State, spec.Owner)
	return st, nil
}

func finalizeArtifact(ctx workflow.Context, st *State) error {
	// Deleting the record deletes the bytes: owned data dies with its
	// record.
	if st.Blob == nil {
		return nil
	}
	return workflow.ExecuteActivity(activityCtx(ctx), DeleteActivity, artifactId(ctx), *st.Blob).Get(ctx, nil)
}

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
		},
	})
}

func artifactId(ctx workflow.Context) id.ArtifactId {
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := string(Kind) + "/"
	if len(full) > len(prefix) {
		return id.ArtifactId(full[len(prefix):])
	}
	return id.ArtifactId(full)
}
