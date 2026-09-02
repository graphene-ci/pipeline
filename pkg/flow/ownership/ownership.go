// Package ownership is the tree mechanics every owned system resource
// shares: the CURRENT owner lives in entity State (ownership is given
// away, never taken — mutable by one command), and is mirrored into the
// EntityOwner search attribute so the cascade is one visibility query.
// A transfer may bound the stay under the new owner (stand TTL) — the
// deadline mirrors into EntityKeepUntil for the server's sweeper.
package ownership

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// State is the owned half of a resource's entity state — embed it.
type State struct {
	// Owner is the CURRENT owner ("kind/id").
	Owner ref.OwnerRef `json:"owner,omitempty"`
	// KeepUntil bounds the stay under the current owner; nil means
	// until an explicit delete.
	KeepUntil *time.Time `json:"keepUntil,omitempty"`
	// Flows are the OUTGOING data-flow edges this record declares — the
	// second axis, orthogonal to ownership: who it talks to and how.
	// A UI draws the topology from them (Р-Н25). Declared intent, not
	// verified traffic. Empty on records that talk to nothing.
	Flows []Flow `json:"flows,omitempty"`
}

// Flow is one declared outgoing edge: this record initiates a
// connection TO another, over a protocol, carrying something.
type Flow struct {
	// To is the target — a record ref ("agent/edge-1") or an external
	// endpoint ("stroppy-server", "10.0.0.5:5432").
	To string `json:"to"`
	// Protocol names how ("http", "tcp", "prometheus_pull").
	Protocol string `json:"protocol"`
	// Label is the human note on the edge ("logs", "node 9100").
	Label string `json:"label,omitempty"`
	// Port is the target port, when it clarifies the edge.
	Port int `json:"port,omitempty"`
}

// TransferCmd gives the resource to a new owner.
type TransferCmd struct {
	NewOwner ref.OwnerRef  `json:"newOwner"`
	Keep     time.Duration `json:"keep,omitempty"`
}

// Name is the command's wire identity — one for every owned kind.
func (TransferCmd) Name() entity.CommandName {
	return entity.CommandName(wire.TransferOwnerCmdName)
}

// Result binds the response type.
func (TransferCmd) Result() TransferRes { return TransferRes{} }

// Validate rejects a malformed owner before anything runs.
func (c TransferCmd) Validate() error {
	if err := c.NewOwner.Validate(); err != nil {
		return err
	}
	if c.Keep < 0 {
		return fmt.Errorf("keep must not be negative")
	}
	return nil
}

// TransferRes reports the resulting owner.
type TransferRes struct {
	Owner ref.OwnerRef `json:"owner"`
}

// Init sets the initial owner at entity init and mirrors it into the
// search attributes.
func Init(ctx workflow.Context, st *State, owner ref.OwnerRef) {
	st.Owner = owner
	upsert(ctx, st)
}

// Register wires the transfer command into a definition; get locates the
// owned half inside the flow's own State.
func Register[Spec, S any](def *entdefine.Definition[Spec, S], get func(*S) *State) {
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[Spec, S], cmd TransferCmd) (TransferRes, error) {
		st := get(ec.State())
		st.Owner = cmd.NewOwner
		if cmd.Keep > 0 {
			deadline := workflow.Now(ctx).Add(cmd.Keep)
			st.KeepUntil = &deadline
		} else {
			st.KeepUntil = nil
		}
		upsert(ctx, st)
		return TransferRes{Owner: st.Owner}, nil
	})
}

func upsert(ctx workflow.Context, st *State) {
	attrs := []temporal.SearchAttributeUpdate{
		wire.SearchAttrOwner.ValueSet(string(st.Owner)),
	}
	if st.KeepUntil != nil {
		attrs = append(attrs, wire.SearchAttrKeepUntil.ValueSet(*st.KeepUntil))
	} else {
		attrs = append(attrs, wire.SearchAttrKeepUntil.ValueUnset())
	}
	// Visibility is a mirror, not the record: an installation without
	// the custom attributes still works, only listing degrades.
	_ = workflow.UpsertTypedSearchAttributes(ctx, attrs...)
}
