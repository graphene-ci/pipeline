package pipeline

import (
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

// ToStand hands a resource (with its subtree) to the pipeline's Stand —
// the permanent owner every pipeline has. The run's ownership ends, the
// workflow may return, the resources stay. KeepFor bounds the stay: the
// Stand deletes the subtree when the TTL expires; without it the
// resources live until an explicit delete. The managed run container
// keeps serving these resources until the last of them is gone.
func ToStand(ctx Context, h Handle, opts ...TransferOption) {
	if ctx.Recording() {
		ctx.RecordStep("transfer", string(h.ResourceRef()), "", "to stand")
		return
	}
	var o transferOptions
	for _, opt := range opts {
		opt(&o)
	}
	req := wire.TransferResourceRequest{
		Resource: h.ResourceRef(),
		NewOwner: wire.StandOwner(ctx.pipelineId),
		Keep:     o.keep,
	}
	if err := workflow.ExecuteActivity(serverCtx(ctx), wire.TransferResourceActivity, req).Get(ctx, nil); err != nil {
		panic(resourceFailure{err: err})
	}
}
