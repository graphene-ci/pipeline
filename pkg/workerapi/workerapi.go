// Package workerapi is the worker-plane client: the gRPC surface a
// RUNNING WORKER calls on the server's door — secrets at the point of
// use, capability publication, blob bytes. One lazy connection per
// process, wired from the same environment the worker itself runs on.
package workerapi

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

var (
	connOnce sync.Once
	conn     *grpc.ClientConn
	connErr  error
)

// bearer sends the worker's token with every call.
type bearer struct {
	token    string
	insecure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearer) RequireTransportSecurity() bool { return !b.insecure }

func dial() (*grpc.ClientConn, error) {
	connOnce.Do(func() {
		address := os.Getenv(wire.EnvAddress)
		if address == "" {
			connErr = fmt.Errorf("%s is not set", wire.EnvAddress)
			return
		}
		insecureOn, _ := strconv.ParseBool(os.Getenv(wire.EnvInsecure))
		creds := credentials.NewTLS(nil)
		if insecureOn {
			creds = insecure.NewCredentials()
		}
		conn, connErr = grpc.NewClient(address,
			grpc.WithTransportCredentials(creds),
			grpc.WithPerRPCCredentials(bearer{token: os.Getenv(wire.EnvToken), insecure: insecureOn}),
		)
	})
	return conn, connErr
}

// GetSecret resolves a secret name to its value — call from activity
// code only; resolving is a side effect and the value must not travel
// back into workflow history.
func GetSecret(ctx context.Context, name string) (string, error) {
	c, err := dial()
	if err != nil {
		return "", err
	}
	resp, err := workerplanev1.NewSecretsAPIClient(c).GetSecret(ctx, &workerplanev1.GetSecretRequest{Name: name})
	if err != nil {
		return "", fmt.Errorf("secret %q: %w", name, err)
	}
	return resp.GetValue(), nil
}

// PublishCapability writes what a machine now CAN onto its record.
func PublishCapability(ctx context.Context, agentId string, capability *workerplanev1.Capability) error {
	c, err := dial()
	if err != nil {
		return err
	}
	_, err = workerplanev1.NewCapabilitiesAPIClient(c).PublishCapability(ctx, &workerplanev1.PublishCapabilityRequest{
		AgentId:    agentId,
		Capability: capability,
	})
	return err
}

// PutBlob streams bytes into the store and returns their address —
// content-addressed, the digest is computed by the server.
func PutBlob(ctx context.Context, r io.Reader) (digest, location string, size int64, err error) {
	c, err := dial()
	if err != nil {
		return "", "", 0, err
	}
	stream, err := workerplanev1.NewBlobsAPIClient(c).PutBlob(ctx)
	if err != nil {
		return "", "", 0, err
	}
	buf := make([]byte, 1<<20)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&workerplanev1.PutBlobRequest{Chunk: buf[:n]}); err != nil {
				return "", "", 0, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", "", 0, readErr
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", "", 0, err
	}
	return resp.GetDigest(), resp.GetLocation(), resp.GetSize(), nil
}

// GetBlob streams a blob's bytes from the store.
func GetBlob(ctx context.Context, location string) (io.ReadCloser, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	stream, err := workerplanev1.NewBlobsAPIClient(c).GetBlob(ctx, &workerplanev1.GetBlobRequest{Location: location})
	if err != nil {
		return nil, err
	}
	return &streamReader{stream: stream}, nil
}

type streamReader struct {
	stream workerplanev1.BlobsAPI_GetBlobClient
	rest   []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.rest) == 0 {
		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err // io.EOF at the end, verbatim
		}
		r.rest = msg.GetChunk()
	}
	n := copy(p, r.rest)
	r.rest = r.rest[n:]
	return n, nil
}

func (r *streamReader) Close() error { return nil }

// EmitEvent puts a domain event into an entity's history — a
// milestone, not a log line (each event costs the entity history
// budget; streams belong in telemetry).
func EmitEvent(ctx context.Context, ref, name string, payload []byte) error {
	c, err := dial()
	if err != nil {
		return err
	}
	_, err = workerplanev1.NewEventsAPIClient(c).Emit(ctx, &workerplanev1.EmitRequest{
		Ref:     ref,
		Name:    name,
		Payload: payload,
	})
	return err
}

// PublishManifest records what this pipeline binary IS; the server
// deduplicates by content.
func PublishManifest(ctx context.Context, manifest []byte) error {
	c, err := dial()
	if err != nil {
		return err
	}
	_, err = workerplanev1.NewManifestAPIClient(c).PublishManifest(ctx, &workerplanev1.PublishManifestRequest{
		Manifest: manifest,
	})
	return err
}
