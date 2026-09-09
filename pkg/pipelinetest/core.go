package pipelinetest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/workflow"
)

// Handle1 installs a typed single-argument contract adapter.
func Handle1[In, Out any](w *World, name string, fn func(workflow.Context, In) (Out, error)) {
	w.Handle(name, func(ctx workflow.Context, args []any) (any, error) {
		var in In
		if err := Decode(args, 0, &in); err != nil {
			return nil, err
		}
		return fn(ctx, in)
	})
}

func (w *World) installCore() {
	w.Handle(wire.DeclareAgentActivity, w.declareAgent)
	w.Handle(wire.AttachAgentActivity, w.attachAgent)
	Handle1(w, wire.AgentUserDataActivity, func(_ workflow.Context, agent id.AgentId) (string, error) {
		return "# pipelinetest agent " + string(agent) + "\n", nil
	})
	Handle1(w, wire.EnsureContainerActivity, func(ctx workflow.Context, req wire.EnsureContainerRequest) (any, error) {
		_, err := w.waitAgent(ctx, req.AgentId, nil, 15*time.Minute)
		return nil, err
	})
	Handle1(w, wire.SelectAgentsActivity, w.selectAgents)
	w.Handle(wire.PublishCapabilityActivity, func(_ workflow.Context, args []any) (any, error) {
		var agent id.AgentId
		var capability pipeline.Capability
		if err := Decode(args, 0, &agent); err != nil {
			return nil, err
		}
		if err := Decode(args, 1, &capability); err != nil {
			return nil, err
		}
		return nil, w.PublishCapability(agent, capability)
	})
	Handle1(w, wire.TransferResourceActivity, func(ctx workflow.Context, req wire.TransferResourceRequest) (any, error) {
		return nil, w.transfer(ctx, req)
	})
	Handle1(w, wire.RunCleanupActivity, func(ctx workflow.Context, req wire.RunCleanupRequest) (any, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		owner := ref.RunOwner(req.RunId)
		w.outcomes[owner] = req.Outcome
		w.deleteTree(owner, workflow.Now(ctx))
		return nil, nil
	})
	Handle1(w, wire.DeleteResourceActivity, func(ctx workflow.Context, name ref.OwnerRef) (any, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if r := w.resources[name]; r != nil && (r.Foreign || r.Creator != RunOwner(ctx)) {
			return nil, fmt.Errorf("cannot delete foreign resource %s", name)
		}
		w.deleteTree(name, workflow.Now(ctx))
		return nil, nil
	})
	w.Handle(wire.DeclareArtifactActivity, w.declareArtifact)
	Handle1(w, wire.AttachArtifactActivity, func(_ workflow.Context, name id.ArtifactId) (pipeline.ArtifactState, error) {
		r, ok := w.Resource(ref.OwnerRef("artifact/" + string(name)))
		var out pipeline.ArtifactState
		if !ok || r.Phase != "ready" {
			return out, fmt.Errorf("artifact %s does not exist or is not ready", name)
		}
		err := json.Unmarshal(r.State, &out)
		return out, err
	})
	Handle1(w, "graphene.blob.upload-bytes", func(_ workflow.Context, data []byte) (ref.BlobRef, error) { return w.PutBlob(data), nil })
	Handle1(w, "graphene.blob.upload-file", func(ctx workflow.Context, path string) (ref.BlobRef, error) {
		w.mu.Lock()
		data, ok := w.files[Agent(ctx)][path]
		w.mu.Unlock()
		if !ok {
			return ref.BlobRef{}, fmt.Errorf("agent %s: no fixture file %q", Agent(ctx), path)
		}
		return w.PutBlob(data), nil
	})
}

// ConnectAfter connects an agent identity at a virtual delay from test start.
// It does not create the resource: the pipeline must declare or attach it.
func (w *World) ConnectAfter(agent id.AgentId, delay time.Duration) {
	w.Env.RegisterDelayedCallback(func() { w.Connect(agent) }, delay)
}

// Connect updates the observed connection state.
func (w *World) Connect(agent id.AgentId) {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.agents[agent]
	state.AgentConnected = true
	w.agents[agent] = state
	w.syncAgentRecord(agent, state)
}

// Disconnect models loss of connection, without deleting the record.
func (w *World) Disconnect(agent id.AgentId) {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.agents[agent]
	state.AgentConnected = false
	w.agents[agent] = state
	w.syncAgentRecord(agent, state)
}

// SeedAgent supplies an existing foreign resource for AttachAgent/SelectAgents.
func (w *World) SeedAgent(agent id.AgentId, labels map[string]string, state pipeline.AgentState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	name := ref.OwnerRef("agent/" + string(agent))
	w.agents[agent] = clone(state)
	raw, _ := json.Marshal(state)
	w.resources[name] = &Resource{Ref: name, Phase: "ready", Foreign: true, Labels: clone(labels), State: raw}
}

// AgentState returns a detached observed state.
func (w *World) AgentState(agent id.AgentId) pipeline.AgentState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return clone(w.agents[agent])
}

// PublishCapability updates the agent's capability set, just as an installer does.
func (w *World) PublishCapability(agent id.AgentId, capability pipeline.Capability) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	state, ok := w.agents[agent]
	r := w.resources[ref.OwnerRef("agent/"+string(agent))]
	if !ok || !state.AgentConnected || r == nil || r.Phase == "deleted" {
		return fmt.Errorf("agent %s is not connected", agent)
	}
	replaced := false
	for i, c := range state.Capabilities {
		if c.Name == capability.Name {
			state.Capabilities[i] = clone(capability)
			replaced = true
			break
		}
	}
	if !replaced {
		state.Capabilities = append(state.Capabilities, clone(capability))
	}
	w.agents[agent] = state
	w.syncAgentRecord(agent, state)
	return nil
}

// syncAgentRecord mirrors observed state while holding the world's lock.
func (w *World) syncAgentRecord(agent id.AgentId, state pipeline.AgentState) {
	if r := w.resources[ref.OwnerRef("agent/"+string(agent))]; r != nil && r.Phase != "deleted" {
		r.State, _ = json.Marshal(state)
	}
}

func (w *World) waitAgent(ctx workflow.Context, agent id.AgentId, needs []wire.NeedSpec, timeout time.Duration) (pipeline.AgentState, error) {
	name := ref.OwnerRef("agent/" + string(agent))
	err := Wait(ctx, timeout, func() bool {
		if w.Failure(name) != nil {
			return true
		}
		r, exists := w.Resource(name)
		state := w.AgentState(agent)
		return exists && (r.Phase == "deleted" || (state.AgentConnected && pipeline.NeedsSatisfied(needs, state.Capabilities)))
	})
	if err != nil {
		return pipeline.AgentState{}, fmt.Errorf("agent %s: %w", agent, err)
	}
	if err := w.Failure(name); err != nil {
		return pipeline.AgentState{}, err
	}
	if r, _ := w.Resource(name); r.Phase == "deleted" {
		return pipeline.AgentState{}, fmt.Errorf("agent %s deleted", agent)
	}
	return w.AgentState(agent), nil
}

func (w *World) declareAgent(ctx workflow.Context, args []any) (any, error) {
	var agent id.AgentId
	var spec pipeline.AgentSpec
	if err := Decode(args, 0, &agent); err != nil {
		return nil, err
	}
	if err := Decode(args, 1, &spec); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	name := ref.OwnerRef("agent/" + string(agent))
	raw, _ := json.Marshal(spec)
	if err := w.Declare(ctx, Resource{Ref: name, Owner: spec.Owner, Spec: raw, Labels: spec.Labels}); err != nil {
		return nil, err
	}
	state, err := w.waitAgent(ctx, agent, spec.Needs, 30*time.Minute)
	if err != nil {
		return nil, err
	}
	return state, w.Ready(ctx, name, state)
}

func (w *World) attachAgent(ctx workflow.Context, args []any) (any, error) {
	var agent id.AgentId
	var needs []wire.NeedSpec
	if err := Decode(args, 0, &agent); err != nil {
		return nil, err
	}
	if len(args) > 1 {
		if err := Decode(args, 1, &needs); err != nil {
			return nil, err
		}
	}
	if r, ok := w.Resource(ref.OwnerRef("agent/" + string(agent))); !ok || r.Phase == "deleted" {
		return nil, fmt.Errorf("agent %s does not exist", agent)
	}
	return w.waitAgent(ctx, agent, needs, 30*time.Minute)
}

func (w *World) selectAgents(_ workflow.Context, selector wire.AgentSelector) ([]id.AgentId, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var selected []id.AgentId
	for name, state := range w.agents {
		r := w.resources[ref.OwnerRef("agent/"+string(name))]
		if r == nil || r.Phase != "ready" || !state.AgentConnected || !pipeline.NeedsSatisfied(selector.Needs, state.Capabilities) {
			continue
		}
		matches := true
		for key, value := range selector.Labels {
			if got, ok := r.Labels[key]; !ok || got != value {
				matches = false
				break
			}
		}
		if matches {
			selected = append(selected, name)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
	return selected, nil
}

// File supplies bytes on one simulated machine. No host files are accessed.
func (w *World) File(agent id.AgentId, path string, data []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.files[agent] == nil {
		w.files[agent] = map[string][]byte{}
	}
	w.files[agent][path] = append([]byte{}, data...)
}

// PutBlob stores test bytes by SHA-256 and returns a content reference.
func (w *World) PutBlob(data []byte) ref.BlobRef {
	digest := sha256.Sum256(data)
	key := hex.EncodeToString(digest[:])
	w.mu.Lock()
	defer w.mu.Unlock()
	w.blobs[key] = append([]byte{}, data...)
	return ref.BlobRef{Digest: key, Location: "pipelinetest://" + key, Size: int64(len(data))}
}

// Blob returns a detached copy of stored bytes.
func (w *World) Blob(blob ref.BlobRef) ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, ok := w.blobs[blob.Digest]
	return append([]byte{}, data...), ok
}

// SeedArtifact supplies a foreign artifact and its content.
func (w *World) SeedArtifact(name id.ArtifactId, data []byte) pipeline.ArtifactState {
	state := pipeline.ArtifactState{Verified: true, Blob: w.PutBlob(data)}
	raw, _ := json.Marshal(state)
	resource := ref.OwnerRef("artifact/" + string(name))
	w.mu.Lock()
	defer w.mu.Unlock()
	w.resources[resource] = &Resource{Ref: resource, Phase: "ready", State: raw, Foreign: true}
	return state
}

func (w *World) declareArtifact(ctx workflow.Context, args []any) (any, error) {
	var name id.ArtifactId
	var spec pipeline.ArtifactSpec
	if err := Decode(args, 0, &name); err != nil {
		return nil, err
	}
	if err := Decode(args, 1, &spec); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	data, ok := w.Blob(spec.Blob)
	if !ok || int64(len(data)) != spec.Blob.Size || spec.Blob.Location != "pipelinetest://"+spec.Blob.Digest {
		return nil, fmt.Errorf("artifact %s references an unknown or invalid blob", name)
	}
	resource := ref.OwnerRef("artifact/" + string(name))
	raw, _ := json.Marshal(spec)
	if err := w.Declare(ctx, Resource{Ref: resource, Owner: spec.Owner, Spec: raw, Labels: spec.Labels}); err != nil {
		return nil, err
	}
	state := pipeline.ArtifactState{Verified: true, Blob: spec.Blob}
	return state, w.Ready(ctx, resource, state)
}
