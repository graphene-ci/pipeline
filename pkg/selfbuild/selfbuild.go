// Package selfbuild turns the running pipeline binary into its own
// worker image, ko-style: cross-compile the binary's own main package
// from the surrounding module, lay it over a static base image, and
// push the result to the installation's registry door — no docker
// daemon anywhere.
package selfbuild

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// DefaultBase is the built-in base image; an installation overrides it
// via the context's baseImage (air-gap mirrors its own).
const DefaultBase = "gcr.io/distroless/static:nonroot"

// The worker image targets the installation's runtime platform.
const (
	targetOS   = "linux"
	targetArch = "amd64"
)

// Options wire one push.
type Options struct {
	// Registry is the installation's door, host:port — the /v2 proxy.
	Registry string
	// Namespace and PipelineId form the repository path.
	Namespace  string
	PipelineId string
	// Token authenticates the /v2 door.
	Token string
	// Insecure switches the registry connection to plaintext.
	Insecure bool
	// BaseImage overrides DefaultBase when set.
	BaseImage string
	// Log receives progress lines; nil silences them.
	Log func(format string, args ...any)
}

func (o Options) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// Push builds the binary's own image and pushes it unless the registry
// already holds this exact content. Returns the full image reference
// and whether anything was actually pushed.
func Push(ctx context.Context, opts Options) (string, bool, error) {
	binPath, digest, err := compile(ctx, opts)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(binPath)) }()

	// The tag is the binary content digest: same code — same tag —
	// nothing to push.
	repo := fmt.Sprintf("%s/%s/%s", opts.Registry, opts.Namespace, opts.PipelineId)
	ref := repo + ":" + digest[:16]

	nameOpts := []name.Option{}
	if opts.Insecure {
		nameOpts = append(nameOpts, name.Insecure)
	}
	tag, err := name.NewTag(ref, nameOpts...)
	if err != nil {
		return "", false, fmt.Errorf("image ref %q: %w", ref, err)
	}
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuth(&authn.Bearer{Token: opts.Token}),
	}

	if _, err := remote.Head(tag, remoteOpts...); err == nil {
		opts.logf("image %s already present — nothing to push", ref)
		return ref, false, nil
	}

	img, err := assemble(ctx, binPath, opts)
	if err != nil {
		return "", false, err
	}
	opts.logf("pushing %s", ref)
	if err := remote.Write(tag, img, remoteOpts...); err != nil {
		return "", false, fmt.Errorf("push %s: %w", ref, err)
	}
	return ref, true, nil
}

// compile rebuilds the binary's own main package for the target
// platform, reproducibly, and returns the output path and its digest.
func compile(ctx context.Context, opts Options) (string, string, error) {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Path == "" {
		return "", "", fmt.Errorf("no build info: the binary must be built by the go toolchain from its module")
	}
	dir, err := os.MkdirTemp("", "graphene-selfbuild-")
	if err != nil {
		return "", "", err
	}
	out := filepath.Join(dir, "app")
	opts.logf("building %s for %s/%s", bi.Path, targetOS, targetArch)
	// -trimpath and a stripped build id keep the output stable for the
	// same source — the content tag depends on it.
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags", "-s -w -buildid=", "-o", out, bi.Path) //nolint:gosec // building the binary's own main package
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+targetOS, "GOARCH="+targetArch)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", fmt.Errorf("go build %s: %w (run from inside the pipeline's module)", bi.Path, err)
	}
	f, err := os.Open(out) //nolint:gosec // our own temp build output
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", "", err
	}
	defer func() { _ = f.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", err
	}
	return out, hex.EncodeToString(sum.Sum(nil)), nil
}

// assemble lays the binary over the base image as /app and sets the
// entrypoint.
func assemble(ctx context.Context, binPath string, opts Options) (v1.Image, error) {
	baseRef := opts.BaseImage
	if baseRef == "" {
		baseRef = DefaultBase
	}
	base, err := baseImage(ctx, baseRef, opts)
	if err != nil {
		return nil, fmt.Errorf("base image %s: %w", baseRef, err)
	}
	layer, err := binaryLayer(binPath)
	if err != nil {
		return nil, err
	}
	img, err := mutate.AppendLayers(base, layer)
	if err != nil {
		return nil, err
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cfg = cfg.DeepCopy()
	cfg.Config.Entrypoint = []string{"/app"}
	cfg.Config.Cmd = nil
	cfg.OS, cfg.Architecture = targetOS, targetArch
	return mutate.ConfigFile(img, cfg)
}

// baseImage fetches the base for the target platform. The built-in
// default lives on the public internet; overriding it points at a
// mirror inside the installation.
func baseImage(ctx context.Context, ref string, opts Options) (v1.Image, error) {
	nameOpts := []name.Option{}
	if opts.Insecure {
		nameOpts = append(nameOpts, name.Insecure)
	}
	r, err := name.ParseReference(ref, nameOpts...)
	if err != nil {
		return nil, err
	}
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(v1.Platform{OS: targetOS, Architecture: targetArch}),
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
	}
	desc, err := remote.Get(r, remoteOpts...)
	if err != nil {
		return nil, err
	}
	return desc.Image()
}

// binaryLayer wraps the binary as a single-file tar layer at /app with
// zeroed metadata — reproducible for identical bytes.
func binaryLayer(binPath string) (v1.Layer, error) {
	raw, err := os.ReadFile(binPath) //nolint:gosec // our own temp build output
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "app", Mode: 0o555, Size: int64(len(raw))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(raw); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	content := buf.Bytes()
	return tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}, tarball.WithMediaType(types.OCILayer))
}
