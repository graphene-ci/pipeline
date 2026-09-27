package obs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

type captureExporter struct{ got []*metricdata.ResourceMetrics }

func (c *captureExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (c *captureExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation { return nil }
func (c *captureExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	c.got = append(c.got, rm)
	return nil
}
func (c *captureExporter) ForceFlush(context.Context) error { return nil }
func (c *captureExporter) Shutdown(context.Context) error   { return nil }

// A reading goes out as it was taken: the counter a monotonic cumulative
// sum, the gauge a gauge, each point at ITS time with its labels and the
// context's correlation — and nothing is exported again afterwards.
func TestShipKeepsTimeKindAndLabels(t *testing.T) {
	exp := &captureExporter{}
	setSink(exp, resource.NewSchemaless(attribute.String("service.name", "t")))
	defer setSink(nil, nil)
	t0 := time.Date(2026, 9, 27, 17, 13, 0, 0, time.UTC)
	ctx := WithEntity(context.Background(), "docker/pg-exporter")
	require.NoError(t, Ship(ctx,
		Series{Name: "node_cpu_seconds_total", Kind: CounterSeries, Points: []Sample{
			{At: t0, Value: 632.30, Attrs: []KV{Str("cpu", "0"), Str("mode", "idle")}},
			{At: t0, Value: 12.5, Attrs: []KV{Str("cpu", "0"), Str("mode", "user")}},
		}},
		Series{Name: "pg_up", Kind: GaugeSeries, Points: []Sample{{At: t0.Add(time.Second), Value: 1}}},
		Series{Name: "empty", Kind: GaugeSeries},
	))
	require.Len(t, exp.got, 1, "one export per Ship, none later")
	rm := exp.got[0]
	require.Len(t, rm.ScopeMetrics, 1)
	metrics := rm.ScopeMetrics[0].Metrics
	require.Len(t, metrics, 2, "a series without points ships nothing")

	sum, ok := metrics[0].Data.(metricdata.Sum[float64])
	require.True(t, ok, "a counter is a Sum")
	require.True(t, sum.IsMonotonic)
	require.Equal(t, metricdata.CumulativeTemporality, sum.Temporality)
	require.Len(t, sum.DataPoints, 2)
	require.Equal(t, t0, sum.DataPoints[0].Time, "the reading's own time, not the exporter's")
	require.False(t, sum.DataPoints[0].StartTime.IsZero())
	require.InDelta(t, 632.30, sum.DataPoints[0].Value, 1e-9)
	mode, _ := sum.DataPoints[0].Attributes.Value("mode")
	require.Equal(t, "idle", mode.AsString())
	entity, _ := sum.DataPoints[0].Attributes.Value(AttrEntity)
	require.Equal(t, "docker/pg-exporter", entity.AsString(), "the context's correlation rides on every point")

	gauge, ok := metrics[1].Data.(metricdata.Gauge[float64])
	require.True(t, ok, "a gauge is a Gauge")
	require.Equal(t, t0.Add(time.Second), gauge.DataPoints[0].Time)

	setSink(nil, nil)
	require.NoError(t, Ship(ctx, Series{Name: "x", Kind: GaugeSeries, Points: []Sample{{At: t0, Value: 1}}}), "no endpoint: silence")
	require.Len(t, exp.got, 1)
}
