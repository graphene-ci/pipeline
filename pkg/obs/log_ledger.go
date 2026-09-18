package obs

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// Log records travel through a bounded queue that never blocks the code that
// logs: when output outruns the export, the OLDEST records are overwritten.
// That is the right policy for telemetry — a slow door must not stall an
// activity — but the SDK does it silently. The ledger makes it visible:
// what went in, what came out, and at shutdown exactly how much was lost.
const (
	// logQueueSize and logBatchSize: the sustained ceiling is
	// batch / export latency (4096 per ~20 ms ≈ 200k lines/s, against
	// ~25k/s for the SDK's default 512); a queue beyond two batches buys
	// nothing. With maxLogBody the queue holds at most ~128 MiB.
	logQueueSize = 8192
	logBatchSize = 4096
)

// logLedger counts records on both sides of the queue.
type logLedger struct {
	emitted, exported, failed atomic.Int64
}

// lost is exact once the queue has been flushed: everything that went in
// and neither arrived nor failed was overwritten in the queue.
func (l *logLedger) lost() (dropped, failed int64) {
	failed = l.failed.Load()
	return max(l.emitted.Load()-l.exported.Load()-failed, 0), failed
}

// ledgerProcessor counts what enters the queue.
type ledgerProcessor struct {
	sdklog.Processor
	ledger *logLedger
}

func (p ledgerProcessor) OnEmit(ctx context.Context, record *sdklog.Record) error {
	p.ledger.emitted.Add(1)
	return p.Processor.OnEmit(ctx, record)
}

// ledgerExporter counts what leaves it.
type ledgerExporter struct {
	sdklog.Exporter
	ledger *logLedger
}

func (e ledgerExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := e.Exporter.Export(ctx, records)
	if err != nil {
		e.ledger.failed.Add(int64(len(records)))
		return err
	}
	e.ledger.exported.Add(int64(len(records)))
	return nil
}

// observe publishes the running totals as metrics of this process: a gap
// between emitted and exported that keeps growing is loss in progress.
func (l *logLedger) observe(meter metric.Meter) error {
	emitted, err := meter.Int64ObservableCounter("graphene.obs.log.emitted",
		metric.WithDescription("log records handed to the export queue"))
	if err != nil {
		return err
	}
	exported, err := meter.Int64ObservableCounter("graphene.obs.log.exported",
		metric.WithDescription("log records delivered to the door"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(emitted, l.emitted.Load())
		o.ObserveInt64(exported, l.exported.Load())
		return nil
	}, emitted, exported)
	return err
}

// report runs after the final flush, when the loss is exact: it is said
// where a person reading the run will see it — as the last log record of
// this process, on stderr, and as a metric.
func (l *logLedger) report(ctx context.Context, provider *sdklog.LoggerProvider, meter metric.Meter) {
	dropped, failed := l.lost()
	if dropped == 0 && failed == 0 {
		return
	}
	message := fmt.Sprintf("obs: %d log records were lost — %d overwritten in the export queue (output outran the export), %d failed to export; the full output of a job is in its log file",
		dropped+failed, dropped, failed)
	fmt.Fprintln(os.Stderr, message)
	if counter, err := meter.Int64Counter("graphene.obs.log.lost",
		metric.WithDescription("log records lost by this process: queue overflow and failed exports")); err == nil {
		counter.Add(ctx, dropped+failed)
	}
	var record otellog.Record
	record.SetSeverity(otellog.SeverityWarn)
	record.SetBody(attribute.StringValue(message))
	provider.Logger("graphene.obs").Emit(ctx, record)
	_ = provider.ForceFlush(ctx)
}
