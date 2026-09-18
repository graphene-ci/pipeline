package obs

import (
	"context"
	"fmt"

	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// logRequestBytes keeps every serialized log request well below the door's
// 4 MiB gRPC limit: a batch of long lines is far bigger than that, and a
// rejected request loses the whole batch.
const logRequestBytes = 2 * 1024 * 1024

// logSplitFields are the boundaries a log request may be cut at; a record
// is never split — emit bounds its body, so one record always fits.
var logSplitFields = []protoreflect.Name{"resource_logs", "scope_logs", "log_records"}

// exportLogChunks changes only transport framing. The standard exporter
// still owns conversion, retry policy and authentication; chunks go out in
// order, so the records keep theirs.
func exportLogChunks(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	request, ok := req.(*collector.ExportLogsServiceRequest)
	if !ok || proto.Size(request) <= logRequestBytes {
		return invoke(ctx, method, req, reply, conn, opts...)
	}
	response, ok := reply.(*collector.ExportLogsServiceResponse)
	if !ok {
		return fmt.Errorf("unexpected OTLP logs response type %T", reply)
	}
	chunks, err := logChunks(request, logRequestBytes)
	if err != nil {
		return err
	}
	proto.Reset(response)
	for _, chunk := range chunks {
		part := &collector.ExportLogsServiceResponse{}
		if err := invoke(ctx, method, chunk, part, conn, opts...); err != nil {
			return err
		}
		if partial := part.GetPartialSuccess(); partial != nil {
			if response.PartialSuccess == nil {
				response.PartialSuccess = &collector.ExportLogsPartialSuccess{}
			}
			response.PartialSuccess.RejectedLogRecords += partial.GetRejectedLogRecords()
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

func logChunks(request *collector.ExportLogsServiceRequest, limit int) ([]*collector.ExportLogsServiceRequest, error) {
	if proto.Size(request) <= limit {
		return []*collector.ExportLogsServiceRequest{request}, nil
	}
	left, right, ok := bisectMessage(request.ProtoReflect(), logSplitFields)
	if !ok {
		return nil, fmt.Errorf("OTLP log record with metadata exceeds %d bytes", limit)
	}
	first, err := logChunks(left.Interface().(*collector.ExportLogsServiceRequest), limit)
	if err != nil {
		return nil, err
	}
	second, err := logChunks(right.Interface().(*collector.ExportLogsServiceRequest), limit)
	if err != nil {
		return nil, err
	}
	return append(first, second...), nil
}
