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
	gauges     map[string]metric.Float64Gauge
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

// Gauge sets the CURRENT value of something — a queue depth, a memory
// footprint: the reading replaces the previous one instead of
// accumulating. Count accumulates, Measure distributes, Gauge states.
func Gauge(ctx context.Context, name string, value float64, attrs ...KV) {
	instruments.Lock()
	if instruments.gauges == nil {
		instruments.gauges = map[string]metric.Float64Gauge{}
	}
	g, ok := instruments.gauges[name]
	if !ok {
		g, _ = otel.GetMeterProvider().Meter("graphene.obs").Float64Gauge(name)
		instruments.gauges[name] = g
	}
	instruments.Unlock()
	g.Record(ctx, value, metric.WithAttributes(append(ctxAttrs(ctx), attrs...)...))
}
