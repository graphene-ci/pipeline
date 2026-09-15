package obs

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type metricReceiver struct {
	collector.UnimplementedMetricsServiceServer
	mu       sync.Mutex
	requests []*collector.ExportMetricsServiceRequest
	tokens   []string
}

func (r *metricReceiver) Export(ctx context.Context, req *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	md, _ := metadata.FromIncomingContext(ctx)
	r.tokens = append(r.tokens, md.Get("authorization")...)
	return &collector.ExportMetricsServiceResponse{}, nil
}

// Exercise the real exporter against gRPC's default 4 MiB receive limit.
// A large scrape must preserve every series, including series sharing one
// instrument, while retaining resource identity and authentication per chunk.
func TestSetupExportsLargeMetricCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	receiver := &metricReceiver{}
	server := grpc.NewServer()
	collector.RegisterMetricsServiceServer(server, receiver)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	oldMetrics, oldTraces, oldLogs := otel.GetMeterProvider(), otel.GetTracerProvider(), global.GetLoggerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(oldMetrics)
		otel.SetTracerProvider(oldTraces)
		global.SetLoggerProvider(oldLogs)
	})
	shutdown, err := Setup(ctx, Config{Endpoint: listener.Addr().String(), Insecure: true,
		Token: "test-token", Namespace: "test-ns", RunId: "test-run", AgentId: "test-agent", Role: "worker"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdown(context.Background())) })

	const instruments, points = 3, 1024
	meter := otel.Meter("large-scrape")
	for i := range instruments {
		gauge, createErr := meter.Int64Gauge(fmt.Sprintf("scrape_%d", i))
		require.NoError(t, createErr)
		for j := range points {
			gauge.Record(ctx, int64(j), metric.WithAttributes(
				attribute.Int("series", j), attribute.String("label", strings.Repeat("x", 4000)),
				attribute.String(AttrEntity, "docker/test-db")))
		}
	}
	provider, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider)
	require.True(t, ok)
	require.NoError(t, provider.ForceFlush(ctx))

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	require.Greater(t, len(receiver.requests), 1)
	seen := map[string]bool{}
	totalBytes := 0
	for i, request := range receiver.requests {
		size := proto.Size(request)
		totalBytes += size
		require.LessOrEqual(t, size, 2*1024*1024)
		require.Equal(t, "Bearer test-token", receiver.tokens[i])
		for _, resource := range request.GetResourceMetrics() {
			attrs := map[string]string{}
			for _, attr := range resource.GetResource().GetAttributes() {
				attrs[attr.GetKey()] = attr.GetValue().GetStringValue()
			}
			require.Equal(t, "test-ns", attrs[AttrNamespace])
			require.Equal(t, "test-run", attrs[AttrRun])
			require.Equal(t, "test-agent", attrs[AttrAgent])
			for _, scope := range resource.GetScopeMetrics() {
				require.Equal(t, "large-scrape", scope.GetScope().GetName())
				for _, instrument := range scope.GetMetrics() {
					for _, point := range instrument.GetGauge().GetDataPoints() {
						key := fmt.Sprintf("%s/%d", instrument.GetName(), point.GetAsInt())
						require.False(t, seen[key], "duplicate point %s", key)
						seen[key] = true
						require.NotZero(t, point.GetTimeUnixNano())
					}
				}
			}
		}
	}
	require.Greater(t, totalBytes, 8*1024*1024)
	require.Len(t, seen, instruments*points)
}
