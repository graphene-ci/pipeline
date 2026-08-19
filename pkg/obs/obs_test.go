package obs

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// recorder captures emitted records via the SDK's simple processor.
type recorder struct {
	sync.Mutex
	bodies []string
}

func (r *recorder) OnEmit(_ context.Context, rec *sdklog.Record) error {
	r.Lock()
	defer r.Unlock()
	r.bodies = append(r.bodies, rec.Body().AsString())
	return nil
}
func (r *recorder) Shutdown(context.Context) error                         { return nil }
func (r *recorder) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (r *recorder) ForceFlush(context.Context) error                       { return nil }

func withRecorder(t *testing.T) *recorder {
	t.Helper()
	rec := &recorder{}
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(sdklog.NewLoggerProvider(sdklog.WithProcessor(rec)))
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
	return rec
}

func TestRunTailStreamsAndTails(t *testing.T) {
	rec := withRecorder(t)
	cmd := exec.Command("sh", "-c", "echo one; echo two >&2; printf 'no-newline'")
	out, err := RunTail(context.Background(), cmd, 64)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, want := range []string{"one", "two", "no-newline"} {
		if !strings.Contains(out, want) {
			t.Fatalf("tail misses %q: %q", want, out)
		}
		found := false
		for _, b := range rec.bodies {
			if b == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("line %q was not streamed as a record: %v", want, rec.bodies)
		}
	}
}

func TestTailBufferBounds(t *testing.T) {
	tb := &tailBuffer{max: 4}
	_, _ = tb.Write([]byte("abcdefgh"))
	if tb.String() != "efgh" {
		t.Fatalf("tail: %q", tb.String())
	}
}

func TestWithEntity(t *testing.T) {
	ctx := WithEntity(context.Background(), "vpc.network/net")
	if Entity(ctx) != "vpc.network/net" {
		t.Fatal("entity lost")
	}
	attrs := ctxAttrs(ctx)
	if len(attrs) != 1 || string(attrs[0].Key) != AttrEntity {
		t.Fatalf("ctx attrs: %v", attrs)
	}
}
