package obs

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// A scraped or sampled measurement is a fact about a MOMENT: the value
// AND the time it was read, and whether it accumulates (a counter) or
// states (a gauge). Handing such a reading to an SDK gauge loses all
// three: the periodic reader re-exports the last reading at its own beat
// under its own clock, for as long as the process lives — a counter that
// stopped moving looks like one still being read, and rate() over the
// repeated points is anything but the source's rate. Ship exports
// readings as they were taken, and nothing after the last one.

// SeriesKind says what a series' values mean.
type SeriesKind int

const (
	// GaugeSeries states a current value: memory in use, a temperature.
	GaugeSeries SeriesKind = iota
	// CounterSeries accumulates and only grows since the source started:
	// seconds of CPU, bytes sent, requests served.
	CounterSeries
)

// Sample is one reading: the value, the moment it was read, its labels.
type Sample struct {
	At    time.Time
	Value float64
	Attrs []KV
}

// Series is one metric's readings of one moment or several.
type Series struct {
	Name   string
	Kind   SeriesKind
	Points []Sample
}

// sink is the exporter Ship writes through — the same OTLP exporter the
// periodic reader uses, with the same resource — set by Setup.
var sink struct {
	sync.Mutex
	exporter sdkmetric.Exporter
	resource *resource.Resource
	since    time.Time
}

func setSink(exp sdkmetric.Exporter, res *resource.Resource) {
	sink.Lock()
	defer sink.Unlock()
	sink.exporter, sink.resource, sink.since = exp, res, time.Now()
}

// Ship exports readings with their own time and kind, stamped with the
// context's correlation like every other signal. Without a configured
// endpoint it is a no-op, like the rest of the package.
func Ship(ctx context.Context, series ...Series) error {
	sink.Lock()
	exp, res, since := sink.exporter, sink.resource, sink.since
	sink.Unlock()
	if exp == nil || len(series) == 0 {
		return nil
	}
	stamp := ctxAttrs(ctx)
	metrics := make([]metricdata.Metrics, 0, len(series))
	for _, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		points := make([]metricdata.DataPoint[float64], 0, len(s.Points))
		for _, p := range s.Points {
			attrs := make([]attribute.KeyValue, 0, len(stamp)+len(p.Attrs))
			attrs = append(attrs, stamp...)
			attrs = append(attrs, p.Attrs...)
			points = append(points, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(attrs...),
				StartTime:  since,
				Time:       p.At,
				Value:      p.Value,
			})
		}
		m := metricdata.Metrics{Name: s.Name}
		switch s.Kind {
		case CounterSeries:
			m.Data = metricdata.Sum[float64]{DataPoints: points, Temporality: metricdata.CumulativeTemporality, IsMonotonic: true}
		case GaugeSeries:
			m.Data = metricdata.Gauge[float64]{DataPoints: points}
		default:
			continue // an unknown kind is not a series this package can name
		}
		metrics = append(metrics, m)
	}
	if len(metrics) == 0 {
		return nil
	}
	return exp.Export(ctx, &metricdata.ResourceMetrics{
		Resource: res,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope:   instrumentation.Scope{Name: "graphene.obs"},
			Metrics: metrics,
		}},
	})
}
