package pipeline

// The binary's own command surface: the pipeline binary MANAGES its own
// pipeline. `push` builds the binary into a worker image and records it
// with its manifest; `run` pushes-if-changed and starts a managed run.
// Everything about the system's records at large belongs to graphenectl;
// this surface is only about THIS pipeline.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"
	"github.com/graphene-ci/pipeline/pkg/id"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
	"github.com/graphene-ci/pipeline/pkg/selfbuild"
)

// runCLI handles the binary's subcommands; ok=false means no subcommand
// was given and the caller should serve its worker role as usual.
func runCLI[P any](pipelineId id.PipelineId, manifestJSON []byte) (handled bool, err error) {
	if len(os.Args) < 2 {
		return false, nil
	}
	switch os.Args[1] {
	case "push":
		return true, cmdPush(pipelineId, manifestJSON, os.Args[2:])
	case "run":
		return true, cmdRun[P](pipelineId, manifestJSON, os.Args[2:])
	case "dev":
		// TODO(dev): the inplace dev loop — embedded dev contour.
		return true, fmt.Errorf("dev is not implemented yet")
	default:
		// Unknown words are not roles — fail loudly instead of silently
		// serving.
		return true, fmt.Errorf("unknown command %q (want push, run or dev)", os.Args[1])
	}
}

// cmdPush builds and pushes the worker image, then records it together
// with the manifest on the pipeline entity.
func cmdPush(pipelineId id.PipelineId, manifestJSON []byte, args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	ctxName := fs.String("context", "", "connection context name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	_, _, err := push(ctx, pipelineId, manifestJSON, *ctxName)
	return err
}

// push is the shared build-push-record flow. Returns the image ref and
// the resolved context.
func push(ctx context.Context, pipelineId id.PipelineId, manifestJSON []byte, ctxName string) (string, cliconfig.Context, error) {
	cc, name, err := cliconfig.Resolve(ctxName)
	if err != nil {
		return "", cliconfig.Context{}, err
	}
	logf := func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }
	logf("context %s → %s", name, cc.Server)

	image, pushed, err := selfbuild.Push(ctx, selfbuild.Options{
		Registry:   cc.Server,
		Namespace:  cc.Namespace,
		PipelineId: string(pipelineId),
		Token:      cc.Token,
		Insecure:   cc.Insecure,
		BaseImage:  cc.BaseImage,
		Log:        logf,
	})
	if err != nil {
		return "", cc, err
	}

	conn, err := dialDoor(cc)
	if err != nil {
		return "", cc, err
	}
	defer func() { _ = conn.Close() }()
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = workerplanev1.NewManifestAPIClient(conn).PublishManifest(callCtx, &workerplanev1.PublishManifestRequest{
		Manifest: manifestJSON,
		Image:    image,
	})
	if err != nil {
		return "", cc, fmt.Errorf("record pipeline: %w", err)
	}
	if pushed {
		logf("pipeline %s → %s", pipelineId, image)
	}
	return image, cc, nil
}

// cmdRun pushes-if-changed and starts a managed run with typed params
// taken from flags generated off the pipeline's Params type.
func cmdRun[P any](pipelineId id.PipelineId, manifestJSON []byte, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	ctxName := fs.String("context", "", "connection context name")
	watch := fs.Bool("watch", false, "follow the run and exit with its outcome")
	dev := fs.Bool("dev", false, "TODO: local dev run (not implemented)")
	paramsJSON := fs.String("params", "", "params as raw JSON (overrides param flags)")
	var labels labelFlags
	fs.Var(&labels, "label", "run label k=v (repeatable)")
	setters := paramFlags[P](fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dev {
		// TODO(dev): the inplace dev run.
		return fmt.Errorf("--dev is not implemented yet")
	}

	params, err := buildParams[P](*paramsJSON, setters)
	if err != nil {
		return err
	}

	ctx := context.Background()
	image, cc, err := push(ctx, pipelineId, manifestJSON, *ctxName)
	if err != nil {
		return err
	}

	conn, err := dialDoor(cc)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	runs := workerplanev1.NewRunsAPIClient(conn)

	runId := fmt.Sprintf("%s-%s", pipelineId, time.Now().UTC().Format("20060102-150405"))
	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = runs.StartRun(startCtx, &workerplanev1.StartRunRequest{
		RunId:    runId,
		Pipeline: string(pipelineId),
		Params:   params,
		Image:    image,
		Labels:   labels.m,
	})
	if err != nil {
		return fmt.Errorf("start run: %w", err)
	}
	fmt.Fprintf(os.Stderr, "run %s started\n", runId)
	if !*watch {
		fmt.Println(runId)
		return nil
	}
	return watchRun(ctx, runs, runId)
}

// watchRun follows the run to a terminal status, prints the typed
// result on success, and mirrors the outcome in the exit code.
func watchRun(ctx context.Context, runs workerplanev1.RunsAPIClient, runId string) error {
	last := ""
	for {
		resp, err := runs.GetRun(ctx, &workerplanev1.GetRunRequest{RunId: runId})
		if err != nil {
			return fmt.Errorf("watch: %w", err)
		}
		status := resp.GetStatus()
		if status != last {
			fmt.Fprintf(os.Stderr, "run %s: %s\n", runId, status)
			last = status
		}
		switch status {
		case "Completed":
			res, err := runs.RunResult(ctx, &workerplanev1.RunResultRequest{RunId: runId})
			if err != nil {
				return fmt.Errorf("result: %w", err)
			}
			fmt.Println(string(res.GetResult()))
			return nil
		case "Failed", "Canceled", "Terminated", "TimedOut":
			return fmt.Errorf("run %s: %s", runId, strings.ToLower(status))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// dialDoor connects to the installation's single door with the
// context's token. Transient failures retry with backoff — a watch
// must survive a blinking network.
func dialDoor(cc cliconfig.Context) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(nil)
	if cc.Insecure {
		creds = insecure.NewCredentials()
	}
	retryOpts := []retry.CallOption{
		retry.WithMax(5),
		retry.WithBackoff(retry.BackoffExponentialWithJitter(200*time.Millisecond, 0.2)),
		retry.WithCodes(codes.Unavailable, codes.ResourceExhausted, codes.Aborted),
	}
	return grpc.NewClient(cc.Server,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(cliBearer{token: cc.Token, insecure: cc.Insecure}),
		grpc.WithUnaryInterceptor(retry.UnaryClientInterceptor(retryOpts...)),
	)
}

type cliBearer struct {
	token    string
	insecure bool
}

func (b cliBearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b cliBearer) RequireTransportSecurity() bool { return !b.insecure }

// labelFlags collects repeatable -label k=v pairs.
type labelFlags struct{ m map[string]string }

func (l *labelFlags) String() string { return "" }

func (l *labelFlags) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want k=v", s)
	}
	if l.m == nil {
		l.m = map[string]string{}
	}
	l.m[k] = v
	return nil
}

// paramSetter writes one flag's parsed value into the params map when
// the flag was actually set; it closes over its FlagSet.
type paramSetter func(into map[string]any)

// paramFlags derives flags from the exported fields of the Params
// struct: the JSON name becomes the flag name, primitive kinds get
// native flags, everything else takes raw JSON. Non-struct Params types
// fall back to --params only.
func paramFlags[P any](fs *flag.FlagSet) []paramSetter {
	t := reflect.TypeOf((*P)(nil)).Elem()
	if t.Kind() != reflect.Struct {
		return nil
	}
	var setters []paramSetter
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		fieldName := jsonName(f)
		if fieldName == "-" {
			continue
		}
		ifSet := func(fieldName string, value func() any) paramSetter {
			return func(into map[string]any) {
				fs.Visit(func(f *flag.Flag) {
					if f.Name == fieldName {
						into[fieldName] = value()
					}
				})
			}
		}
		switch f.Type.Kind() { //nolint:exhaustive // the default arm takes every compound kind as raw JSON
		case reflect.String:
			p := fs.String(fieldName, "", "param "+fieldName)
			setters = append(setters, ifSet(fieldName, func() any { return *p }))
		case reflect.Bool:
			p := fs.Bool(fieldName, false, "param "+fieldName)
			setters = append(setters, ifSet(fieldName, func() any { return *p }))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if f.Type == reflect.TypeOf(time.Duration(0)) {
				p := fs.Duration(fieldName, 0, "param "+fieldName)
				setters = append(setters, ifSet(fieldName, func() any { return *p }))
				break
			}
			p := fs.Int64(fieldName, 0, "param "+fieldName)
			setters = append(setters, ifSet(fieldName, func() any { return *p }))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			p := fs.Uint64(fieldName, 0, "param "+fieldName)
			setters = append(setters, ifSet(fieldName, func() any { return *p }))
		case reflect.Float32, reflect.Float64:
			p := fs.Float64(fieldName, 0, "param "+fieldName)
			setters = append(setters, ifSet(fieldName, func() any { return *p }))
		default:
			// Compound fields take raw JSON on the flag.
			p := fs.String(fieldName, "", "param "+fieldName+" (JSON)")
			setters = append(setters, ifSet(fieldName, func() any { return json.RawMessage(*p) }))
		}
	}
	return setters
}

func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name
	}
	fieldName, _, _ := strings.Cut(tag, ",")
	if fieldName == "" {
		return f.Name
	}
	return fieldName
}

// buildParams merges flag-set params into JSON; --params wins whole.
func buildParams[P any](rawJSON string, setters []paramSetter) ([]byte, error) {
	if rawJSON != "" {
		var probe P
		if err := json.Unmarshal([]byte(rawJSON), &probe); err != nil {
			return nil, fmt.Errorf("--params: %w", err)
		}
		return []byte(rawJSON), nil
	}
	m := map[string]any{}
	for _, s := range setters {
		s(m)
	}
	return json.Marshal(m)
}
