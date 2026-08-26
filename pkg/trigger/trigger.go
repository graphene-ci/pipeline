// Package trigger declares what starts a pipeline's runs besides a
// human: cron schedules and webhooks. Declarations are pipeline.Main
// options — they travel in the manifest, and a push applies them to
// the installation (a removed trigger is unapplied by the next push).
// Each declared trigger becomes a RECORD (kind "trigger", owned by the
// pipeline) with its own history of firings; the pipeline record
// arbitrates the concurrency policy.
package trigger

import (
	"encoding/json"
	"fmt"
	"strings"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// T is one declared trigger.
type T struct {
	pb  *manifestpb.Trigger
	err error
}

// Option refines a declaration.
type Option func(*T)

// Cron declares a schedule ("0 3 * * *", five-field cron). The default
// name is "cron"; a pipeline with several schedules names them apart
// with Name.
func Cron(spec string, opts ...Option) T {
	t := T{pb: &manifestpb.Trigger{Kind: "cron", Name: "cron", Spec: spec}}
	for _, o := range opts {
		o(&t)
	}
	if spec == "" {
		t.fail(fmt.Errorf("cron needs a spec"))
	}
	return t
}

// Upstream declares a CROSS-PIPELINE trigger: this pipeline fires when
// another one's run finishes. The industry's two shapes both reduce to
// this edge — GitHub's workflow_run and GitLab's pipeline-subscription
// both mean "when THAT pipeline completes (with this outcome), start
// me". The spec is "<pipeline>:<outcome>", outcome one of "success"
// (default), "failure", "any". The upstream run's identity arrives in
// the reserved "event" params field, so the downstream can read what
// triggered it.
func Upstream(pipeline string, opts ...Option) T {
	t := T{pb: &manifestpb.Trigger{Kind: "pipeline", Name: "after-" + pipeline, Spec: pipeline + ":success"}}
	for _, o := range opts {
		o(&t)
	}
	if pipeline == "" {
		t.fail(fmt.Errorf("an upstream trigger names a pipeline"))
	}
	return t
}

// OnOutcome refines an Upstream trigger: "success", "failure", "any".
func OnOutcome(outcome string) Option {
	return func(t *T) {
		if t.pb.GetKind() != "pipeline" {
			t.fail(fmt.Errorf("OnOutcome applies to Upstream triggers"))
			return
		}
		name, _, _ := strings.Cut(t.pb.GetSpec(), ":")
		switch outcome {
		case "success", "failure", "any":
			t.pb.Spec = name + ":" + outcome
		default:
			t.fail(fmt.Errorf("outcome %q: want success, failure or any", outcome))
		}
	}
}

// Webhook declares an HTTP entry: POST /hooks/{ns}/{pipeline}/{name}
// on the installation's door starts a run; the request body lands in
// the reserved "event" params field.
func Webhook(name string, opts ...Option) T {
	t := T{pb: &manifestpb.Trigger{Kind: "webhook", Name: name}}
	for _, o := range opts {
		o(&t)
	}
	if name == "" {
		t.fail(fmt.Errorf("webhook needs a name"))
	}
	return t
}

// Name renames a trigger (several crons on one pipeline).
func Name(name string) Option {
	return func(t *T) { t.pb.Name = name }
}

// HookSecret names the secret (in the installation's store) that
// authenticates the webhook: HMAC-SHA256 of the body in
// X-Hub-Signature-256, or the bare value as a Bearer token.
func HookSecret(secretName string) Option {
	return func(t *T) { t.pb.SecretName = secretName }
}

// Params fixes the typed params this trigger starts runs with — the
// SAME type the pipeline function takes; pipeline.Main verifies the
// match at startup, so a drifted declaration fails the binary, not the
// 03:00 run. Environment values come from references (pipeline.Var,
// pipeline.UseSecret), never literals.
func Params[P any](v P) Option {
	return func(t *T) {
		raw, err := json.Marshal(v)
		if err != nil {
			t.fail(fmt.Errorf("trigger params: %w", err))
			return
		}
		t.pb.Params = raw
	}
}

func (t *T) fail(err error) {
	if t.err == nil {
		t.err = err
	}
}

// Build validates a declaration set and renders it for the manifest.
// Errors accumulate — the registration style of the SDK.
func Build(triggers []T) ([]*manifestpb.Trigger, error) {
	seen := map[string]bool{}
	out := make([]*manifestpb.Trigger, 0, len(triggers))
	for _, t := range triggers {
		if t.err != nil {
			return nil, t.err
		}
		if seen[t.pb.GetName()] {
			return nil, fmt.Errorf("trigger %q declared twice (rename one with trigger.Name)", t.pb.GetName())
		}
		seen[t.pb.GetName()] = true
		out = append(out, t.pb)
	}
	return out, nil
}
