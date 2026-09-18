package obs

import (
	"context"
	"fmt"

	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const metricRequestBytes = 2 * 1024 * 1024

// exportMetricChunks changes only transport framing. The standard exporter
// still owns metric conversion, temporality, retry policy and authentication.
func exportMetricChunks(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	request, ok := req.(*collector.ExportMetricsServiceRequest)
	if !ok || proto.Size(request) <= metricRequestBytes {
		return invoke(ctx, method, req, reply, conn, opts...)
	}
	response, ok := reply.(*collector.ExportMetricsServiceResponse)
	if !ok {
		return fmt.Errorf("unexpected OTLP metrics response type %T", reply)
	}
	chunks, err := metricChunks(request, metricRequestBytes)
	if err != nil {
		return err
	}
	proto.Reset(response)
	for _, chunk := range chunks {
		part := &collector.ExportMetricsServiceResponse{}
		if err := invoke(ctx, method, chunk, part, conn, opts...); err != nil {
			return err
		}
		if partial := part.GetPartialSuccess(); partial != nil {
			if response.PartialSuccess == nil {
				response.PartialSuccess = &collector.ExportMetricsPartialSuccess{}
			}
			response.PartialSuccess.RejectedDataPoints += partial.GetRejectedDataPoints()
			if message := partial.GetErrorMessage(); message != "" {
				if response.PartialSuccess.ErrorMessage != "" {
					response.PartialSuccess.ErrorMessage += "; "
				}
				response.PartialSuccess.ErrorMessage += message
			}
		}
	}
	return nil
}

func metricChunks(request *collector.ExportMetricsServiceRequest, limit int) ([]*collector.ExportMetricsServiceRequest, error) {
	if proto.Size(request) <= limit {
		return []*collector.ExportMetricsServiceRequest{request}, nil
	}
	left, right, ok := bisectMetricMessage(request.ProtoReflect())
	if !ok {
		return nil, fmt.Errorf("OTLP metric point with metadata exceeds %d bytes", limit)
	}
	first, err := metricChunks(left.Interface().(*collector.ExportMetricsServiceRequest), limit)
	if err != nil {
		return nil, err
	}
	second, err := metricChunks(right.Interface().(*collector.ExportMetricsServiceRequest), limit)
	if err != nil {
		return nil, err
	}
	return append(first, second...), nil
}

// metricSplitFields are the OTLP collection boundaries a metric request may
// be cut at. Attributes, exemplars, histogram buckets and each complete data
// point stay together.
var metricSplitFields = []protoreflect.Name{"resource_metrics", "scope_metrics", "metrics", "gauge", "sum", "histogram", "exponential_histogram", "summary", "data_points"}

func bisectMetricMessage(message protoreflect.Message) (protoreflect.Message, protoreflect.Message, bool) {
	return bisectMessage(message, metricSplitFields)
}
