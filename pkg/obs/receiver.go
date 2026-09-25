package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// A workload container — the tool a pipeline runs, stroppy, a test suite —
// speaks OTLP but must know neither the door nor a token. The executor
// gives it a LOCAL intake instead: a Receiver on the loopback (and on the
// docker bridge, for containers off the host network) that takes OTLP over
// gRPC and over HTTP on ONE port, stamps the run's correlation on every
// resource, and forwards to the door with the executor's own credential —
// the path its own telemetry takes. What the door sees is the run's.

// Env names through which the receiver's addresses reach libraries in
// this process (machine.OTLPEndpoint reads them).
const (
	EnvOTLPEndpoint       = "GRAPHENE_OTLP_ENDPOINT"
	EnvOTLPBridgeEndpoint = "GRAPHENE_OTLP_ENDPOINT_BRIDGE"
	// RoleWorkload marks telemetry that came through the receiver: the
	// tool's, not the executor's.
	RoleWorkload = "workload"
	// bridgeInterface is docker's default bridge on the machine; a
	// container in a bridge network reaches the host through its address.
	bridgeInterface = "docker0"
)

// Receiver is the local OTLP intake of one executor.
type Receiver struct {
	cfg      Config
	conn     *grpc.ClientConn
	servers  []*http.Server
	Endpoint string // host:port on the loopback
	Bridge   string // host:port on the docker bridge; "" without a bridge
}

// StartReceiver opens the intake and publishes its addresses in this
// process's environment. Empty cfg.Endpoint (no door) starts nothing.
func StartReceiver(ctx context.Context, cfg Config) (*Receiver, error) {
	if cfg.Endpoint == "" {
		return &Receiver{}, nil
	}
	creds := credentials.NewTLS(nil)
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("otlp receiver: dial the door: %w", err)
	}
	r := &Receiver{cfg: cfg, conn: conn}
	handler := r.handler()
	loop, err := r.listen(ctx, "127.0.0.1:0", handler)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	r.Endpoint = loop
	// The bridge listener takes the loopback's port: one number to tell.
	if ip := bridgeIP(); ip != "" {
		_, port, _ := net.SplitHostPort(loop)
		if bridge, err := r.listen(ctx, net.JoinHostPort(ip, port), handler); err == nil {
			r.Bridge = bridge
		}
	}
	_ = os.Setenv(EnvOTLPEndpoint, r.Endpoint)
	_ = os.Setenv(EnvOTLPBridgeEndpoint, r.Bridge)
	return r, nil
}

// Close stops the listeners and the connection to the door.
func (r *Receiver) Close(ctx context.Context) error {
	var errs []error
	for _, s := range r.servers {
		errs = append(errs, s.Shutdown(ctx))
	}
	if r.conn != nil {
		errs = append(errs, r.conn.Close())
	}
	return errors.Join(errs...)
}

// listen serves gRPC (over h2c) and HTTP/1.1 on one plaintext port.
func (r *Receiver) listen(ctx context.Context, addr string, handler http.Handler) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("otlp receiver: listen %s: %w", addr, err)
	}
	// Plaintext HTTP/1.1 and HTTP/2 on one socket: a gRPC exporter opens
	// HTTP/2 with prior knowledge, an HTTP exporter speaks HTTP/1.1.
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           handler,
		Protocols:         &protocols,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	r.servers = append(r.servers, srv)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), nil
}

// handler routes gRPC to the collector services and plain HTTP to the
// OTLP/HTTP paths; anything else is not this intake's business.
func (r *Receiver) handler() http.Handler {
	gs := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(gs, traceIntake{r: r})
	collogspb.RegisterLogsServiceServer(gs, logsIntake{r: r})
	colmetricspb.RegisterMetricsServiceServer(gs, metricsIntake{r: r})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.ProtoMajor == 2 && strings.HasPrefix(req.Header.Get("Content-Type"), "application/grpc") {
			gs.ServeHTTP(w, req)
			return
		}
		r.serveHTTP(w, req)
	})
}

// forward stamps the run onto the resources and sends the request on to
// the door under the executor's credential.
func (r *Receiver) outbound(ctx context.Context) context.Context {
	if r.cfg.Token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+r.cfg.Token)
	}
	return ctx
}

// stamp writes the correlation attributes over whatever the tool said:
// the executor knows whose telemetry this is, the tool does not.
func (r *Receiver) stamp(res *resourcepb.Resource) *resourcepb.Resource {
	if res == nil {
		res = &resourcepb.Resource{}
	}
	stamped := map[string]string{
		AttrNamespace: r.cfg.Namespace, AttrRun: r.cfg.RunId, AttrRole: RoleWorkload,
	}
	if r.cfg.AgentId != "" {
		stamped[AttrAgent] = r.cfg.AgentId
	}
	kept := make([]*commonpb.KeyValue, 0, len(res.GetAttributes())+len(stamped))
	for _, kv := range res.GetAttributes() {
		if _, ours := stamped[kv.GetKey()]; ours || kv.GetKey() == AttrEntity {
			continue
		}
		kept = append(kept, kv)
	}
	for _, key := range []string{AttrNamespace, AttrRun, AttrAgent, AttrRole} {
		if v, ok := stamped[key]; ok {
			kept = append(kept, &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}})
		}
	}
	res.Attributes = kept
	return res
}

type traceIntake struct {
	coltracepb.UnimplementedTraceServiceServer
	r *Receiver
}

func (t traceIntake) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range req.GetResourceSpans() {
		rs.Resource = t.r.stamp(rs.GetResource())
	}
	return coltracepb.NewTraceServiceClient(t.r.conn).Export(t.r.outbound(ctx), req)
}

type logsIntake struct {
	collogspb.UnimplementedLogsServiceServer
	r *Receiver
}

func (l logsIntake) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	for _, rl := range req.GetResourceLogs() {
		rl.Resource = l.r.stamp(rl.GetResource())
	}
	return collogspb.NewLogsServiceClient(l.r.conn).Export(l.r.outbound(ctx), req)
}

type metricsIntake struct {
	colmetricspb.UnimplementedMetricsServiceServer
	r *Receiver
}

func (m metricsIntake) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	for _, rm := range req.GetResourceMetrics() {
		rm.Resource = m.r.stamp(rm.GetResource())
	}
	return colmetricspb.NewMetricsServiceClient(m.r.conn).Export(m.r.outbound(ctx), req)
}

// serveHTTP is OTLP/HTTP: protobuf or JSON bodies on the three paths.
func (r *Receiver) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "OTLP/HTTP takes POST", http.StatusMethodNotAllowed)
		return
	}
	var msg, reply proto.Message
	switch req.URL.Path {
	case "/v1/traces":
		msg, reply = &coltracepb.ExportTraceServiceRequest{}, &coltracepb.ExportTraceServiceResponse{}
	case "/v1/logs":
		msg, reply = &collogspb.ExportLogsServiceRequest{}, &collogspb.ExportLogsServiceResponse{}
	case "/v1/metrics":
		msg, reply = &colmetricspb.ExportMetricsServiceRequest{}, &colmetricspb.ExportMetricsServiceResponse{}
	default:
		http.NotFound(w, req)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 8<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	isJSON := strings.HasPrefix(req.Header.Get("Content-Type"), "application/json")
	if isJSON {
		err = protojson.Unmarshal(body, msg)
	} else {
		err = proto.Unmarshal(body, msg)
	}
	if err != nil {
		http.Error(w, "OTLP body: "+err.Error(), http.StatusBadRequest)
		return
	}
	switch m := msg.(type) {
	case *coltracepb.ExportTraceServiceRequest:
		_, err = traceIntake{r: r}.Export(req.Context(), m)
	case *collogspb.ExportLogsServiceRequest:
		_, err = logsIntake{r: r}.Export(req.Context(), m)
	case *colmetricspb.ExportMetricsServiceRequest:
		_, err = metricsIntake{r: r}.Export(req.Context(), m)
	}
	if err != nil {
		http.Error(w, "door: "+err.Error(), http.StatusBadGateway)
		return
	}
	var out []byte
	if isJSON {
		w.Header().Set("Content-Type", "application/json")
		out, _ = protojson.Marshal(reply)
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		out, _ = proto.Marshal(reply)
	}
	_, _ = w.Write(out)
}

// bridgeIP is the machine's address on docker's default bridge, "" when
// there is none: a container in a bridge network reaches the executor
// through it, a container on the host network through the loopback.
func bridgeIP() string {
	iface, err := net.InterfaceByName(bridgeInterface)
	if err != nil {
		return ""
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
			return ipn.IP.String()
		}
	}
	return ""
}

// endpointJSON is a small helper for tests and diagnostics.
func (r *Receiver) String() string {
	b, _ := json.Marshal(map[string]string{"endpoint": r.Endpoint, "bridge": r.Bridge})
	return string(b)
}
