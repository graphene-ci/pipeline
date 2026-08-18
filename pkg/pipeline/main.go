package pipeline

import (
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Main is the entry point of a pipeline binary: one main == one pipeline.
// It reads the role and wiring from the environment (set by the server or
// the agent when launching this binary as a container, or by the CLI for
// an inplace run), registers the workflow and the machine functions, and
// serves.
//
// Roles of the same binary:
//   - "run" (default) — the run worker: executes the pipeline workflow
//     and machine-independent activities on the run's queue;
//   - "machine" — the per-(machine × run) container hosted by the agent:
//     executes machine functions on the machine's run queue.
//
// workflowFn is the pipeline: an ordinary Temporal workflow function
// taking the typed params struct — the source of the UI form and submit
// validation. machineFns are the named functions callable through
// OnMachine/Action.
func Main(pipelineId id.PipelineId, workflowFn any, opts ...MainOption) {
	if err := serve(pipelineId, workflowFn, opts...); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
}

// MainOption configures Main.
type MainOption func(*mainCfg)

type mainCfg struct {
	machineFns []any
	activities []any
}

// WithMachineFunctions registers the named functions callable on machines
// through OnMachine/Action. They are served by the "machine" role.
func WithMachineFunctions(fns ...any) MainOption {
	return func(c *mainCfg) { c.machineFns = append(c.machineFns, fns...) }
}

// WithActivities registers machine-independent activities served by the
// "run" role alongside the workflow.
func WithActivities(fns ...any) MainOption {
	return func(c *mainCfg) { c.activities = append(c.activities, fns...) }
}

func serve(pipelineId id.PipelineId, workflowFn any, opts ...MainOption) error {
	if err := pipelineId.Validate(); err != nil {
		return err
	}
	cfg := &mainCfg{}
	for _, o := range opts {
		o(cfg)
	}

	role := os.Getenv(wire.EnvRole)
	if role == "" {
		role = "run"
	}
	runId, err := id.ParseRunId(os.Getenv(wire.EnvRunId))
	if err != nil {
		return fmt.Errorf("%s: %w", wire.EnvRunId, err)
	}

	copts := client.Options{HostPort: os.Getenv(wire.EnvAddress)}
	// The address is the server's gRPC proxy — the single door; the
	// run-scoped token authenticates every Temporal call through it.
	if token := os.Getenv(wire.EnvToken); token != "" {
		copts.Credentials = client.NewAPIKeyStaticCredentials(token)
	}
	c, err := client.Dial(copts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer c.Close()

	switch role {
	case "run":
		w := worker.New(c, wire.RunQueue(runId), worker.Options{})
		w.RegisterWorkflow(workflowFn)
		for _, fn := range cfg.activities {
			w.RegisterActivity(fn)
		}
		return w.Run(worker.InterruptCh())
	case "machine":
		machineId, err := id.ParseMachineId(os.Getenv(wire.EnvMachineId))
		if err != nil {
			return fmt.Errorf("%s: %w", wire.EnvMachineId, err)
		}
		w := worker.New(c, wire.MachineRunQueue(machineId, runId), worker.Options{})
		for _, fn := range cfg.machineFns {
			w.RegisterActivity(fn)
		}
		return w.Run(worker.InterruptCh())
	default:
		return fmt.Errorf("unknown role %q (%s)", role, wire.EnvRole)
	}
}
