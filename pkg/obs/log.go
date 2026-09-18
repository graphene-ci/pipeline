package obs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

// severity maps our four verbs to OTel severities.
func emit(ctx context.Context, sev otellog.Severity, msg string, attrs []KV) {
	logger := global.GetLoggerProvider().Logger("graphene.obs")
	var rec otellog.Record
	rec.SetSeverity(sev)
	rec.SetBody(attribute.StringValue(capBody(msg)))
	rec.AddAttributes(ctxAttrs(ctx)...)
	rec.AddAttributes(attrs...)
	logger.Emit(ctx, rec)
}

// maxLogBody bounds one record's body. It is what makes the export queue
// and an export request bounded in BYTES, not just in records; a caller
// with a longer line (a job's output) cuts it into records itself.
const maxLogBody = 16 << 10

func capBody(msg string) string {
	if len(msg) <= maxLogBody {
		return msg
	}
	kept := strings.ToValidUTF8(msg[:maxLogBody], "")
	return fmt.Sprintf("%s …[+%d bytes]", kept, len(msg)-len(kept))
}

// KV is an attribute (alias for readability at call sites).
type KV = attributeKV

// Debug logs at debug severity.
func Debug(ctx context.Context, msg string, attrs ...KV) {
	emit(ctx, otellog.SeverityDebug, msg, attrs)
}

// Info logs at info severity.
func Info(ctx context.Context, msg string, attrs ...KV) {
	emit(ctx, otellog.SeverityInfo, msg, attrs)
}

// Warn logs at warn severity.
func Warn(ctx context.Context, msg string, attrs ...KV) {
	emit(ctx, otellog.SeverityWarn, msg, attrs)
}

// Error logs at error severity.
func Error(ctx context.Context, msg string, attrs ...KV) {
	emit(ctx, otellog.SeverityError, msg, attrs)
}

// RunTail runs a prepared command, streaming every output line as a
// log record on this context (the "inside" of the activity, live), and
// returns the last tailBytes of combined output for error reporting.
func RunTail(ctx context.Context, cmd *exec.Cmd, tailBytes int) (string, error) {
	tail := &tailBuffer{max: tailBytes}
	outLines := newLineWriter(ctx, "stdout")
	errLines := newLineWriter(ctx, "stderr")
	cmd.Stdout = io.MultiWriter(outLines, tail)
	cmd.Stderr = io.MultiWriter(errLines, tail)
	err := cmd.Run()
	outLines.flush()
	errLines.flush()
	return tail.String(), err
}

// newLineWriter emits complete lines as log records.
func newLineWriter(ctx context.Context, stream string) *lineWriter {
	return &lineWriter{ctx: ctx, stream: stream}
}

type lineWriter struct {
	ctx    context.Context //nolint:containedctx // the writer lives exactly as long as one command run
	stream string
	buf    bytes.Buffer
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			// partial line back into the buffer
			w.buf.WriteString(line)
			break
		}
		if line = strings.TrimRight(line, "\r\n"); line != "" {
			Info(w.ctx, line, Str("stream", w.stream))
		}
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if rest := strings.TrimRight(w.buf.String(), "\r\n"); rest != "" {
		Info(w.ctx, rest, Str("stream", w.stream))
	}
	w.buf.Reset()
}

// tailBuffer keeps the last max bytes.
type tailBuffer struct {
	mu  sync.Mutex // stdout and stderr are copied by separate os/exec goroutines
	max int
	buf bytes.Buffer
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf.Write(p)
	if over := t.buf.Len() - t.max; over > 0 && t.max > 0 {
		t.buf.Next(over)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}
