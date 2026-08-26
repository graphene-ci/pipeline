// Package obs is the observability toolkit of pipeline libraries and
// activity bodies: package-level verbs over context.Context — no
// logger to build, no meter to hold, no exporter to know.
//
//	obs.Info(ctx, "cloning", obs.Str("repo", url))
//	out, err := obs.RunTail(ctx, machine.Shell(ctx, script), 2048)
//	ctx, end := obs.Span(ctx, "apply"); defer end(nil)
//	ctx = obs.WithEntity(ctx, "vpc.network/net")
//
// Every signal is stamped automatically: the run, agent, namespace,
// and role (resource attributes set once by Setup), the entity the
// body works on (WithEntity), the current activity (from the Temporal
// context), and the trace context. Setup wires the OTLP exporters at
// the server's door; without Setup everything is a no-op.
package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/sdk/activity"
)

// Attribute keys of the graphene observability convention: the ONE
// correlation surface every UI/CLI query uses.
const (
	AttrNamespace = "graphene.namespace"
	AttrRun       = "graphene.run"
	AttrAgent     = "graphene.agent"
	AttrRole      = "graphene.role"
	AttrEntity    = "graphene.entity"
	AttrActivity  = "graphene.activity"
	AttrAttempt   = "graphene.attempt"
)

type entityKey struct{}

// WithEntity marks the context: everything below works on this entity
// ("kind/id"). Signals emitted with the returned context carry it.
func WithEntity(ctx context.Context, ref string) context.Context {
	return context.WithValue(ctx, entityKey{}, ref)
}

// Entity returns the entity the context works on, or "".
func Entity(ctx context.Context) string {
	ref, _ := ctx.Value(entityKey{}).(string)
	return ref
}

// Str builds a string attribute.
func Str(key, value string) attribute.KeyValue { return attribute.String(key, value) }

// Int builds an int attribute.
func Int(key string, value int) attribute.KeyValue { return attribute.Int(key, value) }

// Err builds the error attribute.
func Err(err error) attribute.KeyValue {
	if err == nil {
		return attribute.String("error", "")
	}
	return attribute.String("error", err.Error())
}

// ctxAttrs are the automatic per-signal attributes: the entity and the
// executing activity. Run/agent/namespace/role live on the Resource —
// stamped once at Setup, not per record.
func ctxAttrs(ctx context.Context) []attribute.KeyValue {
	var out []attribute.KeyValue
	if ref := Entity(ctx); ref != "" {
		out = append(out, attribute.String(AttrEntity, ref))
	}
	if activity.IsActivity(ctx) {
		info := activity.GetInfo(ctx)
		out = append(out,
			attribute.String(AttrActivity, info.ActivityType.Name),
			attribute.Int(AttrAttempt, int(info.Attempt)))
		// The namespace comes from the WORKFLOW the activity runs for,
		// not from the process: one server process serves every
		// namespace's bundles, and a resource-level stamp would brand
		// them all with the process's own.
		if info.WorkflowNamespace != "" {
			out = append(out, attribute.String(AttrNamespace, info.WorkflowNamespace))
		}
	}
	return out
}
