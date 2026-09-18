package obs

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricscollector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracecollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type logReceiver struct {
	collector.UnimplementedLogsServiceServer
	delay    time.Duration // a slow door
	mu       sync.Mutex
	requests []*collector.ExportLogsServiceRequest
	tokens   []string
}

func (r *logReceiver) Export(ctx context.Context, req *collector.ExportLogsServiceRequest) (*collector.ExportLogsServiceResponse, error) {
	time.Sleep(r.delay)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	md, _ := metadata.FromIncomingContext(ctx)
	r.tokens = append(r.tokens, md.Get("authorization")...)
	return &collector.ExportLogsServiceResponse{}, nil
}

type acceptTraces struct {
	tracecollector.UnimplementedTraceServiceServer
}

func (acceptTraces) Export(context.Context, *tracecollector.ExportTraceServiceRequest) (*tracecollector.ExportTraceServiceResponse, error) {
	return &tracecollector.ExportTraceServiceResponse{}, nil
}

func (r *logReceiver) bodies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, req := range r.requests {
		for _, res := range req.GetResourceLogs() {
			for _, scope := range res.GetScopeLogs() {
				for _, rec := range scope.GetLogRecords() {
					out = append(out, rec.GetBody().GetStringValue())
				}
			}
		}
	}
	return out
}

func setupAgainst(t *testing.T, delay time.Duration) (*logReceiver, *metricReceiver, func(context.Context) error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	receiver, metrics := &logReceiver{delay: delay}, &metricReceiver{}
	server := grpc.NewServer() // gRPC's default 4 MiB receive limit — the door's
	collector.RegisterLogsServiceServer(server, receiver)
	metricscollector.RegisterMetricsServiceServer(server, metrics)
	tracecollector.RegisterTraceServiceServer(server, acceptTraces{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	oldMetrics, oldTraces, oldLogs := otel.GetMeterProvider(), otel.GetTracerProvider(), global.GetLoggerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(oldMetrics)
		otel.SetTracerProvider(oldTraces)
		global.SetLoggerProvider(oldLogs)
	})
	shutdown, err := Setup(context.Background(), Config{Endpoint: listener.Addr().String(), Insecure: true,
		Token: "test-token", Namespace: "test-ns", RunId: "test-run", AgentId: "test-agent", Role: "machine"})
	require.NoError(t, err)
	return receiver, metrics, shutdown
}

// sum of a counter's points across everything the metrics receiver got; the
// last export wins for a cumulative counter, so take the maximum.
func (r *metricReceiver) counter(name string) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var value int64
	found := false
	for _, req := range r.requests {
		for _, res := range req.GetResourceMetrics() {
			for _, scope := range res.GetScopeMetrics() {
				for _, m := range scope.GetMetrics() {
					if m.GetName() != name {
						continue
					}
					for _, p := range m.GetSum().GetDataPoints() {
						found = true
						value = max(value, p.GetAsInt())
					}
				}
			}
		}
	}
	return value, found
}

// A chatty job: thousands of long lines in one breath. A batch of them is
// far beyond the door's 4 MiB gRPC limit — every line must still arrive, in
// order, in requests the door accepts, each carrying the token and the
// resource identity.
func TestSetupExportsLargeLogBurst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	receiver, _, shutdown := setupAgainst(t, 0)
	const lines = 3000
	for i := range lines {
		Info(ctx, fmt.Sprintf("%06d %s", i, strings.Repeat("x", 8000)), Str("job", "probe"))
	}
	require.NoError(t, shutdown(ctx))

	bodies := receiver.bodies()
	require.Len(t, bodies, lines, "every line arrives")
	for i, body := range bodies {
		require.Equal(t, fmt.Sprintf("%06d", i), body[:6], "in order")
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	require.Greater(t, len(receiver.requests), 1)
	for i, request := range receiver.requests {
		require.LessOrEqual(t, proto.Size(request), 2*1024*1024)
		require.Equal(t, "Bearer test-token", receiver.tokens[i])
		for _, res := range request.GetResourceLogs() {
			attrs := map[string]string{}
			for _, attr := range res.GetResource().GetAttributes() {
				attrs[attr.GetKey()] = attr.GetValue().GetStringValue()
			}
			require.Equal(t, "test-run", attrs[AttrRun])
			require.Equal(t, "test-agent", attrs[AttrAgent])
		}
	}
}

// Output that outruns the export is overwritten in the queue — by design, a
// slow door must not stall the code that logs. What must NOT happen is
// silence: the loss is counted exactly and said where the run is read.
func TestLogLossIsCountedAndSaid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	receiver, metrics, shutdown := setupAgainst(t, 150*time.Millisecond)
	const lines = 60_000 // far beyond the queue: emitted in a breath, exported at door speed
	for i := range lines {
		Info(ctx, fmt.Sprintf("line %d", i), Str("job", "flood"))
	}
	require.NoError(t, shutdown(ctx))

	bodies := receiver.bodies()
	require.NotEmpty(t, bodies)
	report := bodies[len(bodies)-1]
	require.Contains(t, report, "log records were lost", "the last record of the process names the loss")
	var lost, overwritten, failed int
	_, err := fmt.Sscanf(report, "obs: %d log records were lost — %d overwritten in the export queue (output outran the export), %d failed", &lost, &overwritten, &failed)
	require.NoError(t, err)
	delivered := len(bodies) - 1
	require.Positive(t, lost)
	require.Equal(t, lines, delivered+lost, "delivered + lost == emitted, exactly")
	require.Equal(t, lost, overwritten+failed)

	counted, ok := metrics.counter("graphene.obs.log.lost")
	require.True(t, ok, "the loss is a metric of the process too")
	require.EqualValues(t, lost, counted)
	emitted, _ := metrics.counter("graphene.obs.log.emitted")
	require.GreaterOrEqual(t, emitted, int64(lines))
}

// Nothing lost — nothing said: no warning record, no loss metric.
func TestNoLogLossIsQuiet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	receiver, metrics, shutdown := setupAgainst(t, 0)
	for i := range 500 {
		Info(ctx, fmt.Sprintf("line %d", i))
	}
	require.NoError(t, shutdown(ctx))
	bodies := receiver.bodies()
	require.Len(t, bodies, 500)
	for _, body := range bodies {
		require.NotContains(t, body, "were lost")
	}
	_, ok := metrics.counter("graphene.obs.log.lost")
	require.False(t, ok)
}

func TestCapBody(t *testing.T) {
	require.Equal(t, "short", capBody("short"))
	exact := strings.Repeat("x", maxLogBody)
	require.Equal(t, exact, capBody(exact))

	long := capBody(strings.Repeat("x", maxLogBody+1000))
	require.True(t, strings.HasPrefix(long, exact))
	require.True(t, strings.HasSuffix(long, "…[+1000 bytes]"))

	// A multi-byte rune straddling the cut is dropped whole, never halved.
	straddle := capBody(strings.Repeat("x", maxLogBody-1) + "ж" + strings.Repeat("y", 100))
	require.True(t, utf8.ValidString(straddle))
	require.True(t, strings.HasSuffix(straddle, "…[+102 bytes]"))
}

func TestLogChunks(t *testing.T) {
	record := func(size int) *logspb.LogRecord {
		return &logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: strings.Repeat("x", size)}}}
	}
	request := &collector.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{}}}}}
	for range 100 {
		request.ResourceLogs[0].ScopeLogs[0].LogRecords = append(request.ResourceLogs[0].ScopeLogs[0].LogRecords, record(1000))
	}
	chunks, err := logChunks(request, 10_000)
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1)
	total := 0
	for _, chunk := range chunks {
		require.LessOrEqual(t, proto.Size(chunk), 10_000)
		total += len(chunk.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords())
	}
	require.Equal(t, 100, total)
	require.Len(t, request.ResourceLogs[0].ScopeLogs[0].LogRecords, 100, "the caller's request is not mutated")

	// One record that cannot fit is an error, never a silent drop.
	_, err = logChunks(&collector.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record(20_000)}}}}}}, 10_000)
	require.ErrorContains(t, err, "exceeds")
}
