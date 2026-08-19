package obs

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Config wires the exporters. Endpoint is the server's door — the
// standard OTLP gRPC surface lives behind it; the token is the same
// credential the worker already holds.
type Config struct {
	Endpoint  string
	Token     string
	Insecure  bool
	Namespace string
	RunId     string
	AgentId   string
	Role      string
}

// FromEnv builds the Config from the worker wiring environment.
func FromEnv() Config {
	insecure, _ := strconv.ParseBool(os.Getenv(wire.EnvInsecure))
	return Config{
		Endpoint:  os.Getenv(wire.EnvAddress),
		Token:     os.Getenv(wire.EnvToken),
		Insecure:  insecure,
		Namespace: os.Getenv(wire.EnvNamespace),
		RunId:     os.Getenv(wire.EnvRunId),
		AgentId:   os.Getenv(wire.EnvAgentId),
		Role:      os.Getenv(wire.EnvRole),
	}
}

// Setup installs the global providers: traces, metrics, logs over OTLP
// gRPC at the door, resource attributes carrying the graphene
// correlation convention. Empty Endpoint leaves the no-op globals in
// place. The returned shutdown flushes everything.
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	attrs := []attribute.KeyValue{
		semconv.ServiceName("graphene-pipeline"),
		attribute.String(AttrNamespace, cfg.Namespace),
		attribute.String(AttrRun, cfg.RunId),
		attribute.String(AttrRole, cfg.Role),
	}
	if cfg.AgentId != "" {
		attrs = append(attrs, attribute.String(AttrAgent, cfg.AgentId))
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
	if err != nil {
		return nil, err
	}
	headers := map[string]string{}
	if cfg.Token != "" {
		headers["authorization"] = "Bearer " + cfg.Token
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithHeaders(headers)}
	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.Endpoint), otlpmetricgrpc.WithHeaders(headers)}
	logOpts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(cfg.Endpoint), otlploggrpc.WithHeaders(headers)}
	if cfg.Insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
		logOpts = append(logOpts, otlploggrpc.WithInsecure())
	}

	traceExp, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}
	metricExp, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, err
	}
	logExp, err := otlploggrpc.New(ctx, logOpts...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(res))
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	global.SetLoggerProvider(lp)
	// Distributed context: trace context + baggage cross every hop.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}
