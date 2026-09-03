package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/graphene-ci/pipeline/pkg/workerapi"
)

// NewArtifact declares an artifact from its source: the bytes are
// uploaded by a builtin activity running where they are (the agent's
// container for a file on a machine, the run's worker for computed
// bytes), then the record is declared with the resulting blob ref. The
// handle returns immediately; Ready blocks until the record is verified.
func NewArtifact(ctx Context, name string, src artifact.Source, opts ...ResourceOption) Resource[ArtifactState] {
	self := ref.OwnerRef("artifact/" + name)
	if ctx.Recording() {
		ctx.RecordDeclare(self, BuildResourceOptions(ctx, opts))
		return NewResource[ArtifactState](ctx, self, nil)
	}
	o := BuildResourceOptions(ctx, opts)
	fut, set := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(gctx workflow.Context) {
		gpctx := Context{Context: gctx, pipelineId: ctx.pipelineId}
		var blob ref.BlobRef
		var err error
		switch {
		case src.AgentFile != nil:
			err = DispatchOnAgent(gpctx, src.AgentFile.AgentId, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Minute,
				HeartbeatTimeout:    time.Minute,
			}, uploadFileActivityName, src.AgentFile.Path).Get(gctx, &blob)
		case src.Bytes != nil:
			actx := workflow.WithActivityOptions(gctx, workflow.ActivityOptions{
				TaskQueue:           wire.RunQueue(gpctx.RunId()),
				StartToCloseTimeout: 30 * time.Minute,
				RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute},
			})
			err = workflow.ExecuteActivity(actx, uploadBytesActivityName, src.Bytes).Get(gctx, &blob)
		default:
			err = fmt.Errorf("artifact %q: empty source", name)
		}
		if err != nil {
			set.SetError(fmt.Errorf("upload artifact %q: %w", name, err))
			return
		}
		spec := ArtifactSpec{Blob: blob, Owner: o.Parent}
		set.Chain(workflow.ExecuteActivity(serverCtx(gctx), wire.DeclareArtifactActivity, id.ArtifactId(name), spec))
	})
	res := NewResource[ArtifactState](ctx, self, fut)
	adoptChildren(ctx, self, o.Children)
	return res
}

// AttachArtifact recognizes an artifact made OUTSIDE this run — by
// another pipeline, an earlier run, a person: a missing record is an
// error, never a creation, and no ownership is taken. Ready returns the
// verified state with the blob ref to fetch.
func AttachArtifact(ctx Context, name string) Attached[ArtifactState] {
	if ctx.Recording() {
		ctx.RecordStep("attach", "artifact/"+name, "", "foreign")
		return NewAttached[ArtifactState](ctx, nil)
	}
	fut := workflow.ExecuteActivity(serverCtx(ctx), wire.AttachArtifactActivity, id.ArtifactId(name))
	return NewAttached[ArtifactState](ctx, fut)
}

// Builtin blob activities: registered by Main on every role — upload
// happens on whichever site holds the bytes.
const (
	uploadFileActivityName  = "graphene.blob.upload-file"
	uploadBytesActivityName = "graphene.blob.upload-bytes"
)

// uploadFileActivity reads a local file and puts it into the server's
// blob store.
func uploadFileActivity(ctx context.Context, path string) (ref.BlobRef, error) {
	// The path names a MACHINE file; in an agent-hosted container the
	// machine lives under the machine root.
	f, err := os.Open(machine.Path(path))
	if err != nil {
		return ref.BlobRef{}, err
	}
	defer func() { _ = f.Close() }()
	return uploadBlob(ctx, f)
}

// uploadBytesActivity puts computed bytes into the server's blob store.
func uploadBytesActivity(ctx context.Context, b []byte) (ref.BlobRef, error) {
	return uploadBlob(ctx, bytes.NewReader(b))
}

// uploadBlob streams bytes through the worker plane into the store —
// content-addressed, the digest computed by the server.
func uploadBlob(ctx context.Context, r io.Reader) (ref.BlobRef, error) {
	digest, location, size, err := workerapi.PutBlob(ctx, r)
	if err != nil {
		return ref.BlobRef{}, fmt.Errorf("blob upload: %w", err)
	}
	return ref.BlobRef{Digest: digest, Location: location, Size: size}, nil
}
