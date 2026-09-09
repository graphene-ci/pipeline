// Package pipelinetest simulates Graphene's pipeline-facing contracts on the
// Temporal Go testsuite. It does not start servers or execute discovered activity
// bodies. Use Temporal mocks or explicitly register safe activity replacements.
package pipelinetest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestingT is the testing surface used by the harness and its assertions.
type TestingT interface {
	Helper()
	Fatalf(string, ...any)
	Errorf(string, ...any)
}

// Registration is immutable library metadata produced by pipeline.Prepare.
type Registration interface{ Registration(string) any }

// Handler simulates one service activity in the workflow scheduler. Arguments
// are frozen JSON payloads; use Decode to read their typed values. It can use
// workflow timers and Await. Service transport retries and heartbeats are outside
// this model; ordinary user activities still use Temporal's activity machinery.
type Handler func(workflow.Context, []any) (any, error)

// World is isolated state for one root run. Configure before ExecuteWorkflow;
// during execution mutate fixtures only from RegisterDelayedCallback callbacks.
type World struct {
	Env          *testsuite.TestWorkflowEnvironment
	t            TestingT
	mu           sync.Mutex
	handlers     map[string]Handler
	preparers    []func(Registration)
	bodies       map[string]any
	mockedAgents map[string]bool
	resources    map[ref.OwnerRef]*Resource
	agents       map[id.AgentId]pipeline.AgentState
	files        map[id.AgentId]map[string][]byte
	blobs        map[string][]byte
	failures     map[ref.OwnerRef]error
	calls        []Call
	events       []Event
	outcomes     map[ref.OwnerRef]string
	prepared     bool
	advance      time.Duration
	converter    converter.DataConverter
}

// Call records dispatch routing and serialized arguments, without activity bodies.
type Call struct {
	Name      string
	TaskQueue string
	Args      json.RawMessage
	Time      time.Time
}

// Install attaches the simulator and production cleanup interceptor. Supply any
// other worker options here; the harness preserves their interceptors.
func Install(t TestingT, env *testsuite.TestWorkflowEnvironment, options ...worker.Options) *World {
	t.Helper()
	w := &World{Env: env, t: t, handlers: map[string]Handler{}, bodies: map[string]any{}, mockedAgents: map[string]bool{},
		resources: map[ref.OwnerRef]*Resource{}, agents: map[id.AgentId]pipeline.AgentState{}, files: map[id.AgentId]map[string][]byte{},
		blobs: map[string][]byte{}, failures: map[ref.OwnerRef]error{}, outcomes: map[ref.OwnerRef]string{}}
	w.converter = converter.GetDefaultDataConverter()
	var opts worker.Options
	if len(options) > 1 {
		t.Fatalf("pipelinetest: expected at most one worker.Options")
	}
	if len(options) == 1 {
		opts = options[0]
	}
	opts.Interceptors = append(opts.Interceptors, pipeline.RunInterceptor(), &simulation{world: w})
	env.SetWorkerOptions(opts)
	env.SetTestTimeout(10 * time.Second)
	w.installCore()
	return w
}

// Workflow prepares the real pipeline wrapper. Call once per World, before mocks.
// The stable run id is test-<pipeline>; overrides may be set on Env afterwards.
func Workflow[P, R any](w *World, name id.PipelineId, fn func(pipeline.Context, P) (R, error)) func(workflow.Context, P) (R, error) {
	w.t.Helper()
	if w.prepared {
		w.t.Fatalf("pipelinetest: use a new World for each root run")
	}
	d, err := pipeline.Prepare(name, fn)
	if err != nil {
		w.t.Fatalf("prepare pipeline: %v", err)
		return nil
	}
	w.prepared = true
	w.Env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "run/test-" + string(name), TaskQueue: "run/test-" + string(name)})
	for _, prepare := range w.preparers {
		prepare(d)
	}
	for key, body := range d.Activities() {
		w.bodies[key] = body
		w.Env.RegisterActivityWithOptions(blocked(body, key), activity.RegisterOptions{Name: key})
	}
	w.Env.RegisterDynamicActivity(func(ctx context.Context, _ converter.EncodedValues) (any, error) {
		name := activity.GetInfo(ctx).ActivityType.Name
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("activity %q has no installed simulator or discovered test replacement", name), "UnsupportedActivity", nil)
	}, activity.DynamicRegisterOptions{})
	w.Env.RegisterWorkflowWithOptions(d.Workflow, workflow.RegisterOptions{Name: string(name)})
	return d.Workflow
}

// OnPrepare installs a library metadata consumer, before Workflow is prepared.
func (w *World) OnPrepare(fn func(Registration)) {
	if w.prepared {
		w.t.Fatalf("install library adapters before preparing the pipeline")
	}
	w.preparers = append(w.preparers, fn)
}

// Handle installs a service-contract simulation. Library adapters own wire names.
func (w *World) Handle(name string, fn Handler) { w.handlers[name] = fn }

// SetDataConverter installs the same payload converter on the environment and
// simulated activity results. Configure it before executing the workflow.
func (w *World) SetDataConverter(dc converter.DataConverter) {
	w.converter = dc
	w.Env.SetDataConverter(dc)
}

// OnAgentActivity is Temporal's OnActivity scoped to one agent. Arguments include
// the activity context (usually mock.Anything), just like env.OnActivity.
func (w *World) OnAgentActivity(agent id.AgentId, name string, args ...any) *testsuite.MockCallWrapper {
	w.t.Helper()
	_, ok := w.bodies[name]
	if !ok {
		w.t.Fatalf("activity %q was not discovered; call Workflow before OnAgentActivity", name)
		return nil
	}
	key := string(agent) + "/" + name
	if len(args) == 0 {
		w.t.Fatalf("OnAgentActivity arguments must include the activity context matcher")
		return nil
	}
	contextArgument := args[0]
	matched := append([]any{}, args...)
	matched[0] = mock.MatchedBy(func(ctx context.Context) bool {
		info := activity.GetInfo(ctx)
		run := id.RunId(strings.TrimPrefix(info.WorkflowExecution.ID, "run/"))
		if info.TaskQueue != wire.AgentRunQueue(agent, run) {
			return false
		}
		_, differences := mock.Arguments{contextArgument}.Diff([]any{ctx})
		return differences == 0
	})
	w.mockedAgents[key] = true
	return w.Env.OnActivity(name, matched...)
}

func blocked(body any, name string) any {
	typ := reflect.TypeOf(body)
	return reflect.MakeFunc(typ, func(_ []reflect.Value) []reflect.Value {
		out := make([]reflect.Value, typ.NumOut())
		for i := range out {
			out[i] = reflect.Zero(typ.Out(i))
		}
		out[len(out)-1] = reflect.ValueOf(temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("activity %q has no test replacement", name), "UnmockedActivity", nil))
		return out
	}).Interface()
}

// Decode crosses the same JSON boundary as activity payloads; adapters can read
// private library request types without unsafe assertions or reflection.
func Decode(args []any, index int, out any) error {
	if index >= len(args) {
		return fmt.Errorf("missing argument %d", index)
	}
	raw, err := json.Marshal(args[index])
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// Agent returns the routing identity of the current activity simulation.
func Agent(ctx workflow.Context) id.AgentId {
	queue := workflow.GetActivityOptions(ctx).TaskQueue
	if rest, ok := strings.CutPrefix(queue, "agent/"); ok {
		name, _, _ := strings.Cut(rest, "/run/")
		return id.AgentId(name)
	}
	return ""
}

// RunOwner returns the calling run's owner reference.
func RunOwner(ctx workflow.Context) ref.OwnerRef {
	return ref.OwnerRef(workflow.GetInfo(ctx).WorkflowExecution.ID)
}

// Calls returns a detached snapshot of dispatches.
func (w *World) Calls() []Call { w.mu.Lock(); defer w.mu.Unlock(); return clone(w.calls) }

type simulation struct {
	interceptor.WorkerInterceptorBase
	world *World
}
type inbound struct {
	interceptor.WorkflowInboundInterceptorBase
	world *World
}
type outbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	world *World
}

func (s *simulation) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &inbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, world: s.world}
}
func (i *inbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&outbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}, world: i.world})
}
func (o *outbound) ExecuteActivity(ctx workflow.Context, name string, args ...any) workflow.Future {
	w := o.world
	raw, err := json.Marshal(args)
	if err != nil {
		f, set := workflow.NewFuture(ctx)
		set.SetError(err)
		return f
	}
	w.mu.Lock()
	w.calls = append(w.calls, Call{Name: name, TaskQueue: workflow.GetActivityOptions(ctx).TaskQueue, Args: raw, Time: workflow.Now(ctx)})
	w.mu.Unlock()
	if w.mockedAgents[string(Agent(ctx))+"/"+name] {
		return o.Next.ExecuteActivity(ctx, name, args...)
	}
	fn, ok := w.handlers[name]
	if !ok {
		return o.Next.ExecuteActivity(ctx, name, args...)
	}
	fut, set := workflow.NewFuture(ctx)
	if strings.HasPrefix(name, "server.") && workflow.GetActivityOptions(ctx).TaskQueue != wire.ServerQueue {
		set.SetError(fmt.Errorf("service activity %s dispatched to %q instead of %q", name, workflow.GetActivityOptions(ctx).TaskQueue, wire.ServerQueue))
		return fut
	}
	var encoded []json.RawMessage
	if err := json.Unmarshal(raw, &encoded); err != nil {
		set.SetError(err)
		return fut
	}
	frozen := make([]any, len(encoded))
	for i, payload := range encoded {
		frozen[i] = payload
	}
	workflow.Go(ctx, func(gctx workflow.Context) {
		value, runErr := fn(gctx, frozen)
		if runErr != nil {
			set.SetError(runErr)
			return
		}
		payload, encodeErr := w.converter.ToPayloads(value)
		set.Set(payload, encodeErr)
	})
	return fut
}

func clone[T any](value T) T {
	var out T
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}
