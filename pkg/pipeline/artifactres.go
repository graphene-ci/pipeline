package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// NewArtifact declares an artifact from its source: the bytes are
// uploaded by a builtin activity running where they are (the agent's
// container for a file on a machine, the run's worker for computed
// bytes), then the record is declared with the resulting blob ref. The
// handle returns immediately; Ready blocks until the record is verified.
func NewArtifact(ctx Context, name string, src artifact.Source, opts ...ResourceOption) Resource[ArtifactState] {
	self := ref.OwnerRef("artifact/" + name)
	if ctx.Recording() {
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

// Builtin blob activities: registered by Main on every role — upload
// happens on whichever site holds the bytes.
const (
	uploadFileActivityName  = "graphene.blob.upload-file"
	uploadBytesActivityName = "graphene.blob.upload-bytes"
)

// uploadFileActivity reads a local file and puts it into the server's
// blob store.
func uploadFileActivity(ctx context.Context, path string) (ref.BlobRef, error) {
	f, err := os.Open(path) //nolint:gosec // uploading the named file is the point
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

// uploadBlob streams bytes to the server blob API. The location is the
// content digest — content-addressed, idempotent by construction.
func uploadBlob(ctx context.Context, r io.Reader) (ref.BlobRef, error) {
	base := os.Getenv(wire.EnvHTTP)
	if base == "" {
		return ref.BlobRef{}, fmt.Errorf("%s is not set", wire.EnvHTTP)
	}
	// Digest first: the blob API is content-addressed.
	buf, err := io.ReadAll(r)
	if err != nil {
		return ref.BlobRef{}, err
	}
	sum := sha256.Sum256(buf)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	location := "blobs/" + hex.EncodeToString(sum[:])

	//nolint:gosec // the base URL is the installation's own server from the env — the only door
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base+"/api/v1/blobs/"+location, bytes.NewReader(buf))
	if err != nil {
		return ref.BlobRef{}, err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv(wire.EnvToken))
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // see request construction above
	if err != nil {
		return ref.BlobRef{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return ref.BlobRef{}, fmt.Errorf("blob upload: %s: %s", resp.Status, raw)
	}
	return ref.BlobRef{Digest: digest, Location: location, Size: int64(len(buf))}, nil
}
