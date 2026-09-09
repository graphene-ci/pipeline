package pipelinetest

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/workflow"
)

// Resource is the simulator's observable record. Deleted records remain for
// assertions; Spec and State contain detached JSON, not pointers into user code.
type Resource struct {
	Ref       ref.OwnerRef
	Owner     ref.OwnerRef
	Creator   ref.OwnerRef
	Agent     id.AgentId
	Phase     string
	Spec      json.RawMessage
	State     json.RawMessage
	Labels    map[string]string
	Flows     []pipeline.Flow
	KeepUntil time.Time
	Foreign   bool
}

// Event is a lifecycle transition, ordered by the workflow scheduler.
type Event struct {
	Resource ref.OwnerRef
	Action   string
	Time     time.Time
}

// Declare records a creating resource. Libraries call Ready after convergence.
// Repeating a declaration is idempotent and cannot adopt another run's record.
func (w *World) Declare(ctx workflow.Context, record Resource) error {
	if err := record.Ref.Validate(); err != nil {
		return err
	}
	if err := wire.ValidateUserLabels(record.Labels); err != nil {
		return err
	}
	if record.Owner == "" {
		record.Owner = RunOwner(ctx)
	}
	if err := record.Owner.Validate(); err != nil {
		return err
	}
	record.Creator, record.Phase = RunOwner(ctx), "creating"
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ended := w.outcomes[record.Creator]; ended {
		return fmt.Errorf("run %s has already ended", record.Creator)
	}
	if old, exists := w.resources[record.Ref]; exists {
		if old.Creator != record.Creator || old.Foreign || old.Phase == "deleted" {
			return fmt.Errorf("resource %s already exists outside this declaration", record.Ref)
		}
		return nil
	}
	for parent, seen := record.Owner, map[ref.OwnerRef]bool{}; parent != ""; {
		if parent == record.Ref || seen[parent] {
			return fmt.Errorf("ownership cycle at %s", parent)
		}
		seen[parent] = true
		next := w.resources[parent]
		if next == nil {
			break
		}
		if next.Foreign || next.Phase == "deleted" {
			return fmt.Errorf("cannot use %s as owner", parent)
		}
		parent = next.Owner
	}
	copy := clone(record)
	if w.failures[record.Ref] != nil {
		copy.Phase = "failed"
	}
	w.resources[record.Ref] = &copy
	w.events = append(w.events, Event{Resource: record.Ref, Action: "declare", Time: workflow.Now(ctx)})
	return nil
}

// Ready publishes outputs after the adapter's readiness checks have passed.
func (w *World) Ready(ctx workflow.Context, name ref.OwnerRef, state any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.resources[name]
	if r == nil || r.Phase == "deleted" {
		return fmt.Errorf("resource %s is missing or deleted", name)
	}
	if err := w.failures[name]; err != nil {
		r.Phase = "failed"
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	r.State, r.Phase = raw, "ready"
	w.events = append(w.events, Event{Resource: name, Action: "ready", Time: workflow.Now(ctx)})
	return nil
}

// FailResource injects a convergence failure, before execution or from a callback.
func (w *World) FailResource(name ref.OwnerRef, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures[name] = err
	if r := w.resources[name]; r != nil && err != nil && r.Phase == "creating" {
		r.Phase = "failed"
	}
}

// Failure returns an injected convergence failure.
func (w *World) Failure(name ref.OwnerRef) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failures[name]
}

// Wait waits in virtual workflow time, with cancellation and a finite deadline.
func Wait(ctx workflow.Context, timeout time.Duration, condition func() bool) error {
	if timeout <= 0 {
		return fmt.Errorf("simulation wait requires a positive timeout")
	}
	ok, err := workflow.AwaitWithTimeout(ctx, timeout, condition)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("simulation did not converge within %s", timeout)
	}
	return nil
}

// Resource returns a detached record snapshot.
func (w *World) Resource(name ref.OwnerRef) (Resource, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r, ok := w.resources[name]
	if !ok {
		return Resource{}, false
	}
	return clone(*r), true
}

// Events returns the ordered lifecycle journal.
func (w *World) Events() []Event { w.mu.Lock(); defer w.mu.Unlock(); return clone(w.events) }

// Outcome is the cleanup outcome of a run, or empty while it is running.
func (w *World) Outcome(run ref.OwnerRef) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.outcomes[run]
}

func (w *World) transfer(ctx workflow.Context, req wire.TransferResourceRequest) error {
	if err := req.NewOwner.Validate(); err != nil {
		return err
	}
	if req.Keep < 0 {
		return fmt.Errorf("negative KeepFor")
	}
	toStand := strings.HasPrefix(string(req.NewOwner), "stand/")
	if req.Keep > 0 && !toStand {
		return fmt.Errorf("KeepFor requires a stand")
	}
	err := Wait(ctx, 30*time.Minute, func() bool {
		r, ok := w.Resource(req.Resource)
		return ok && r.Phase != "creating"
	})
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.resources[req.Resource]
	if r.Foreign || r.Phase != "ready" {
		return fmt.Errorf("cannot transfer %s (%s)", req.Resource, r.Phase)
	}
	if toStand {
		seen := map[ref.OwnerRef]bool{}
		for parent := w.resources[r.Owner]; parent != nil; parent = w.resources[r.Owner] {
			if seen[r.Ref] {
				return fmt.Errorf("ownership cycle at %s", r.Ref)
			}
			seen[r.Ref] = true
			r = parent
		}
	}
	if r.Foreign || r.Creator != RunOwner(ctx) {
		return fmt.Errorf("run does not own %s", r.Ref)
	}
	// A previous handoff cannot be taken back or moved to a different owner.
	if strings.HasPrefix(string(r.Owner), "stand/") && r.Owner != req.NewOwner {
		return fmt.Errorf("resource %s was already handed away", r.Ref)
	}
	for parent, seen := req.NewOwner, map[ref.OwnerRef]bool{}; parent != ""; {
		if parent == r.Ref || seen[parent] {
			return fmt.Errorf("ownership cycle at %s", parent)
		}
		seen[parent] = true
		next := w.resources[parent]
		if next == nil {
			break
		}
		if next.Foreign || next.Creator != RunOwner(ctx) {
			return fmt.Errorf("cannot burden foreign owner %s", parent)
		}
		parent = next.Owner
	}
	r.Owner = req.NewOwner
	r.KeepUntil = time.Time{}
	if req.Keep > 0 {
		r.KeepUntil = workflow.Now(ctx).Add(req.Keep)
	}
	w.events = append(w.events, Event{Resource: r.Ref, Action: "transfer", Time: workflow.Now(ctx)})
	return nil
}

func (w *World) deleteTree(owner ref.OwnerRef, now time.Time) {
	children := make([]ref.OwnerRef, 0)
	for name, r := range w.resources {
		if r.Owner == owner && !r.Foreign && r.Phase != "deleted" {
			children = append(children, name)
		}
	}
	sort.Slice(children, func(i, j int) bool { return children[i] < children[j] })
	for _, child := range children {
		w.deleteTree(child, now)
	}
	if r := w.resources[owner]; r != nil && !r.Foreign && r.Phase != "deleted" {
		r.Phase = "deleted"
		if strings.HasPrefix(string(owner), "artifact/") {
			var state pipeline.ArtifactState
			if json.Unmarshal(r.State, &state) == nil {
				used := false
				for name, other := range w.resources {
					if !strings.HasPrefix(string(name), "artifact/") || other.Phase == "deleted" {
						continue
					}
					var live pipeline.ArtifactState
					if json.Unmarshal(other.State, &live) == nil && live.Blob.Digest == state.Blob.Digest {
						used = true
						break
					}
				}
				if !used {
					delete(w.blobs, state.Blob.Digest)
				}
			}
		}
		w.events = append(w.events, Event{Resource: owner, Action: "delete", Time: now})
	}
}

// Advance advances the model's stand TTL after the root workflow completed.
// It does not execute Temporal entity workflows or restart the closed environment.
func (w *World) Advance(d time.Duration) {
	w.t.Helper()
	if d < 0 || !w.Env.IsWorkflowCompleted() {
		w.t.Fatalf("Advance requires a completed workflow and nonnegative duration")
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance += d
	now := w.Env.Now().Add(w.advance)
	var expired []ref.OwnerRef
	for _, r := range w.resources {
		if !r.KeepUntil.IsZero() && !now.Before(r.KeepUntil) && r.Phase != "deleted" {
			expired = append(expired, r.Ref)
		}
	}
	sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
	for _, name := range expired {
		w.deleteTree(name, now)
	}
}

// AssertOwner compares the current owner, including for surviving stand trees.
func (w *World) AssertOwner(t TestingT, name, owner ref.OwnerRef) {
	t.Helper()
	r, ok := w.Resource(name)
	if !ok || r.Owner != owner || r.Phase == "deleted" {
		t.Errorf("resource %s: want live owner %s, got %+v", name, owner, r)
	}
}

// AssertNoLeaks rejects failed cleanup, missing parents, cycles, and live resources
// whose owner chain ends at a completed run. Foreign fixtures are excluded.
func (w *World) AssertNoLeaks(t TestingT) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, r := range w.resources {
		if r.Foreign || r.Phase == "deleted" {
			continue
		}
		seen := map[ref.OwnerRef]bool{name: true}
		for owner := r.Owner; ; {
			if strings.HasPrefix(string(owner), "stand/") {
				break
			}
			if seen[owner] {
				t.Errorf("ownership cycle from %s at %s", name, owner)
				break
			}
			seen[owner] = true
			if _, ended := w.outcomes[owner]; ended {
				t.Errorf("leaked %s under completed %s", name, owner)
				break
			}
			parent, ok := w.resources[owner]
			if !ok || parent.Phase == "deleted" {
				t.Errorf("orphan %s under %s", name, owner)
				break
			}
			owner = parent.Owner
		}
	}
}
