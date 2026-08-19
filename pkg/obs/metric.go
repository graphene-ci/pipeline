package obs

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var instruments struct {
	sync.Mutex
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
}

// Count adds delta to a counter, stamped with the context's automatic
// attributes.
func Count(ctx context.Context, name string, delta int64, attrs ...KV) {
	instruments.Lock()
	if instruments.counters == nil {
		instruments.counters = map[string]metric.Int64Counter{}
	}
	c, ok := instruments.counters[name]
	if !ok {
		c, _ = otel.GetMeterProvider().Meter("graphene.obs").Int64Counter(name)
		instruments.counters[name] = c
	}
	instruments.Unlock()
	c.Add(ctx, delta, metric.WithAttributes(append(ctxAttrs(ctx), attrs...)...))
}

// Measure records a value on a histogram, stamped like Count.
func Measure(ctx context.Context, name string, value float64, attrs ...KV) {
	instruments.Lock()
	if instruments.histograms == nil {
		instruments.histograms = map[string]metric.Float64Histogram{}
	}
	h, ok := instruments.histograms[name]
	if !ok {
		h, _ = otel.GetMeterProvider().Meter("graphene.obs").Float64Histogram(name)
		instruments.histograms[name] = h
	}
	instruments.Unlock()
	h.Record(ctx, value, metric.WithAttributes(append(ctxAttrs(ctx), attrs...)...))
}
