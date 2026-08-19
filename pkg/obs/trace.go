package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Span opens a child span; call end(err) when the piece of work is
// done — the error (if any) marks the span failed.
func Span(ctx context.Context, name string, attrs ...KV) (context.Context, func(error)) {
	ctx, span := otel.GetTracerProvider().Tracer("graphene.obs").
		Start(ctx, name, trace.WithAttributes(append(ctxAttrs(ctx), attrs...)...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
