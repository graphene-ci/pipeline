package obs

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestMetricChunksPreserveAllDataTypes(t *testing.T) {
	cases := map[string]string{
		"gauge":                `{"dataPoints":[{"asDouble":1.5,"timeUnixNano":"10","flags":1}]}`,
		"sum":                  `{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"asInt":"7","startTimeUnixNano":"2","timeUnixNano":"10"}]}`,
		"histogram":            `{"aggregationTemporality":2,"dataPoints":[{"startTimeUnixNano":"2","timeUnixNano":"10","count":"2","sum":3,"bucketCounts":["1","1"],"explicitBounds":[1.5],"min":1,"max":2}]}`,
		"exponentialHistogram": `{"aggregationTemporality":1,"dataPoints":[{"timeUnixNano":"10","count":"2","sum":3,"scale":2,"positive":{"offset":1,"bucketCounts":["1","1"]},"zeroCount":"0"}]}`,
		"summary":              `{"dataPoints":[{"timeUnixNano":"10","count":"2","sum":3,"quantileValues":[{"quantile":0.5,"value":1.5}]}]}`,
	}
	for kind, data := range cases {
		t.Run(kind, func(t *testing.T) {
			instrument := &metrics.Metric{}
			require.NoError(t, protojson.Unmarshal([]byte(`{"name":"example","description":"preserve","unit":"ms","`+kind+`":`+data+`}`), instrument))
			message := instrument.ProtoReflect()
			field := message.Descriptor().Oneofs().ByName("data")
			dataField := message.WhichOneof(field)
			aggregation := message.Get(dataField).Message()
			pointsField := aggregation.Descriptor().Fields().ByName("data_points")
			points := aggregation.Mutable(pointsField).List()
			point := points.Get(0).Message()
			attrs := point.Mutable(point.Descriptor().Fields().ByName("attributes")).List()
			attrs.Append(protoreflect.ValueOfMessage((&common.KeyValue{Key: "large-label", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: strings.Repeat("x", 2048)}}}).ProtoReflect()))
			points.Append(protoreflect.ValueOfMessage(proto.Clone(point.Interface()).ProtoReflect()))
			request := &collector.ExportMetricsServiceRequest{ResourceMetrics: []*metrics.ResourceMetrics{{SchemaUrl: "resource-schema", ScopeMetrics: []*metrics.ScopeMetrics{{SchemaUrl: "scope-schema", Metrics: []*metrics.Metric{instrument}}}}}}
			original := proto.Clone(request)
			chunks, err := metricChunks(request, 3000)
			require.NoError(t, err)
			require.Len(t, chunks, 2)
			require.True(t, proto.Equal(original, request), "input mutated")
			for _, chunk := range chunks {
				require.LessOrEqual(t, proto.Size(chunk), 3000)
				expected := proto.Clone(request).(*collector.ExportMetricsServiceRequest)
				m := expected.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].ProtoReflect().Get(dataField).Message()
				m.Mutable(pointsField).List().Truncate(1)
				require.True(t, proto.Equal(expected, chunk), "point or envelope changed")
			}
			_, err = metricChunks(request, 1000)
			require.ErrorContains(t, err, "point with metadata exceeds")
		})
	}
}

func TestMetricChunksPreservePartialRejections(t *testing.T) {
	point := &metrics.NumberDataPoint{Attributes: []*common.KeyValue{{Key: "label", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: strings.Repeat("x", metricRequestBytes/2)}}}}}
	request := &collector.ExportMetricsServiceRequest{ResourceMetrics: []*metrics.ResourceMetrics{{ScopeMetrics: []*metrics.ScopeMetrics{{Metrics: []*metrics.Metric{{Name: "large", Data: &metrics.Metric_Gauge{Gauge: &metrics.Gauge{DataPoints: []*metrics.NumberDataPoint{point, point, point}}}}}}}}}}
	response := &collector.ExportMetricsServiceResponse{}
	calls := 0
	err := exportMetricChunks(context.Background(), "export", request, response, nil,
		func(_ context.Context, _ string, _ any, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			calls++
			reply.(*collector.ExportMetricsServiceResponse).PartialSuccess = &collector.ExportMetricsPartialSuccess{RejectedDataPoints: 1, ErrorMessage: "rejected"}
			return nil
		})
	require.NoError(t, err)
	require.Greater(t, calls, 1)
	require.Equal(t, int64(calls), response.GetPartialSuccess().GetRejectedDataPoints())
	require.Equal(t, calls, strings.Count(response.GetPartialSuccess().GetErrorMessage(), "rejected"))
}
