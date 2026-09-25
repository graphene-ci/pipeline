package obs

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

// fakeDoor is the server's OTLP intake as the receiver sees it: it keeps
// the bearer it was shown and the resources it was handed.
type fakeDoor struct {
	coltracepb.UnimplementedTraceServiceServer
	collogspb.UnimplementedLogsServiceServer
	colmetricspb.UnimplementedMetricsServiceServer
	mu        sync.Mutex
	bearers   []string
	resources []*resourcepb.Resource
}

func (d *fakeDoor) seen(ctx context.Context, res ...*resourcepb.Resource) {
	d.mu.Lock()
	defer d.mu.Unlock()
	md, _ := metadata.FromIncomingContext(ctx)
	d.bearers = append(d.bearers, md.Get("authorization")...)
	d.resources = append(d.resources, res...)
}

func (d *fakeDoor) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range req.GetResourceSpans() {
		d.seen(ctx, rs.GetResource())
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type doorLogs struct{ *fakeDoor }

func (d doorLogs) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	for _, rl := range req.GetResourceLogs() {
		d.seen(ctx, rl.GetResource())
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type doorMetrics struct{ *fakeDoor }

func (d doorMetrics) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	for _, rm := range req.GetResourceMetrics() {
		d.seen(ctx, rm.GetResource())
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func attrsOf(res *resourcepb.Resource) map[string]string {
	out := map[string]string{}
	for _, kv := range res.GetAttributes() {
		out[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	return out
}

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// A workload speaks OTLP over gRPC or over HTTP to one local port and
// knows nothing else; what reaches the door carries the run's correlation
// and the executor's bearer, and a correlation the tool made up is
// overwritten.
func TestReceiverStampsAndForwards(t *testing.T) {
	door := &fakeDoor{}
	gs := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(gs, door)
	collogspb.RegisterLogsServiceServer(gs, doorLogs{door})
	colmetricspb.RegisterMetricsServiceServer(gs, doorMetrics{door})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gs.Serve(ln) }()
	defer gs.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := StartReceiver(ctx, Config{
		Endpoint: ln.Addr().String(), Token: "run-token", Insecure: true,
		Namespace: "tenant", RunId: "nightly-1", AgentId: "db-1", Role: "machine",
	})
	require.NoError(t, err)
	defer func() { _ = r.Close(context.Background()) }()
	require.NotEmpty(t, r.Endpoint)
	t.Setenv(EnvOTLPEndpoint, r.Endpoint)

	// gRPC, the way an OTel SDK's default exporter speaks.
	conn, err := grpc.NewClient(r.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	spans := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			str("service.name", "stroppy"), str(AttrRun, "someone-elses"), str(AttrNamespace, "other"),
		}},
	}}}
	_, err = coltracepb.NewTraceServiceClient(conn).Export(ctx, spans)
	require.NoError(t, err)
	_, err = colmetricspb.NewMetricsServiceClient(conn).Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{Resource: &resourcepb.Resource{}}},
	})
	require.NoError(t, err)

	// HTTP/JSON, the way a tool without an SDK speaks.
	body, err := protojson.Marshal(&collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{str("service.name", "pytest")}},
	}}})
	require.NoError(t, err)
	resp, err := http.Post("http://"+r.Endpoint+"/v1/logs", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool { door.mu.Lock(); defer door.mu.Unlock(); return len(door.resources) == 3 }, 5*time.Second, 20*time.Millisecond)
	door.mu.Lock()
	defer door.mu.Unlock()
	for _, b := range door.bearers {
		require.Equal(t, "Bearer run-token", b)
	}
	require.Len(t, door.bearers, 3)
	for _, res := range door.resources {
		got := attrsOf(res)
		require.Equal(t, "tenant", got[AttrNamespace], "the tool's namespace is overwritten")
		require.Equal(t, "nightly-1", got[AttrRun], "the tool's run is overwritten")
		require.Equal(t, "db-1", got[AttrAgent])
		require.Equal(t, RoleWorkload, got[AttrRole])
	}
	require.Equal(t, "stroppy", attrsOf(door.resources[0])["service.name"], "the tool's own attributes stay")

	// An unknown path is not this intake's business.
	resp, err = http.Post("http://"+r.Endpoint+"/v1/profiles", "application/json", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// No door, no intake: a process outside an executor has nothing to give.
func TestReceiverWithoutDoorIsInert(t *testing.T) {
	r, err := StartReceiver(context.Background(), Config{})
	require.NoError(t, err)
	require.Empty(t, r.Endpoint)
	require.NoError(t, r.Close(context.Background()))
}
