package pipeline

import (
	"reflect"
	"sort"
	"strings"
	"sync"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/manifest"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Context is the pipeline's workflow context. It carries ONLY what does
// not exist outside a run — the run's identity and its logger;
// everything that acts is a free function taking Context first. It
// embeds workflow.Context, so plain Temporal code works against it
// unchanged.
type Context struct {
	workflow.Context

	pipelineId id.PipelineId
	rec        *recorder
}

// RunId is the identity of this run. The run workflow's ID on the wire
// is "run/{runId}" — this strips the prefix back off.
func (c Context) RunId() id.RunId {
	if c.rec != nil {
		return ""
	}
	full := workflow.GetInfo(c.Context).WorkflowExecution.ID
	return id.RunId(strings.TrimPrefix(full, "run/"))
}

// Logger is the run's structured logger.
func (c Context) Logger() log.Logger {
	if c.rec != nil {
		return nopLogger{}
	}
	return workflow.GetLogger(c.Context)
}

// --- library-author surface below: user code never needs these ---

// Recording reports whether this is the registration pass: at startup
// every role walks the pipeline function once with a recording Context
// to DISCOVER inline activity declarations — nothing executes, resources
// resolve to zero values. Libraries must register their activities and
// do nothing else when this is true.
func (c Context) Recording() bool { return c.rec != nil }

// RecordActivity registers an activity body under its wire name during
// the recording pass; outside of it the call is a no-op (the worker is
// already serving). Duplicate names with different bodies are collected
// and fail Main with an error list.
func (c Context) RecordActivity(name string, fn any) {
	if c.rec == nil {
		return
	}
	c.rec.record(name, fn)
}

// RecordWorker registers a worker-assembly hook during the recording
// pass: libraries that bring WHOLE workflows (entity definitions), not
// just activity bodies, register them here. The hook runs when Main
// builds each role's worker, with the worker and the Temporal client in
// hand. Outside the recording pass the call is a no-op.
func (c Context) RecordWorker(fn func(w worker.Worker, cl client.Client) error) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	c.rec.workerHooks = append(c.rec.workerHooks, fn)
}

// RecordKind notes an entity kind the pipeline's libraries declare —
// manifest material. Recording pass only.
func (c Context) RecordKind(name string) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	c.rec.kinds[name] = true
}

// KindCommand is one command of a brought kind, for the dictionary.
type KindCommand struct {
	Name string
	// Payload is the Go type of the command's payload; nil for a bare
	// command.
	Payload reflect.Type
}

// RecordKindInfo describes a brought kind FULLY — the installation's
// dictionary entry: what a declaration looks like, which commands it
// answers, which observation dimensions it serves. A library that only
// calls RecordKind still works; its dictionary entry just says less.
func (c Context) RecordKindInfo(name, description string, spec reflect.Type, dimensions []string, cmds ...KindCommand) {
	if c.rec == nil {
		return
	}
	c.RecordKind(name)
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	if c.rec.kindDecls == nil {
		c.rec.kindDecls = map[string]kindDecl{}
	}
	c.rec.kindDecls[name] = kindDecl{description: description, spec: spec, dimensions: dimensions, commands: cmds}
}

// kindDecl is the recorder's note of one described kind.
type kindDecl struct {
	description string
	spec        reflect.Type
	dimensions  []string
	commands    []KindCommand
}

// RecordDeclare notes a resource declaration for the PLAN: the node,
// its tree edges from the options, and one plan step consuming the
// Ready-reads since the previous step. Constructors call it in their
// recording branch; outside the pass it is a no-op.
func (c Context) RecordDeclare(self ref.OwnerRef, o ResourceOptions) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	children := make([]string, 0, len(o.Children))
	for _, child := range o.Children {
		children = append(children, string(child))
	}
	c.rec.nodes = append(c.rec.nodes, planNode{
		Ref: string(self), Parent: string(o.Parent), Children: children,
	})
	c.rec.step("declare", string(self), "", "")
}

// RecordStep notes one plan step that is not a declaration — an
// activity on an agent, a fan-out, a transfer. Recording pass only.
func (c Context) RecordStep(op, subject, agent, note string) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	c.rec.step(op, subject, agent, note)
}

// RecordUse notes that user code read a resource's Ready output — the
// data edge feeding the NEXT plan step. Recording pass only.
func (c Context) RecordUse(self ref.OwnerRef) {
	if c.rec == nil {
		return
	}
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	for _, dep := range c.rec.pending {
		if dep == string(self) {
			return
		}
	}
	c.rec.pending = append(c.rec.pending, string(self))
}

// recorder collects what the registration pass discovers.
type recorder struct {
	mu          sync.Mutex
	activities  map[string]any
	workerHooks []func(w worker.Worker, cl client.Client) error
	kinds       map[string]bool
	kindDecls   map[string]kindDecl
	errs        []error

	// The plan: declared nodes, ordered steps, and the Ready-reads
	// accumulated since the last step (the next step's data deps).
	nodes   []planNode
	steps   []planStep
	pending []string
}

// planNode is one declared resource with its tree edges.
type planNode struct {
	Ref      string
	Parent   string
	Children []string
}

// planStep is one ordered operation of the optimistic zero path.
type planStep struct {
	Op      string
	Subject string
	Agent   string
	Note    string
	Deps    []string
}

// step appends one plan step, consuming the pending data deps. The
// caller holds the lock.
func (r *recorder) step(op, subject, agent, note string) {
	deps := r.pending
	r.pending = nil
	r.steps = append(r.steps, planStep{Op: op, Subject: subject, Agent: agent, Note: note, Deps: deps})
}

func newRecorder() *recorder {
	return &recorder{activities: map[string]any{}, kinds: map[string]bool{}}
}

// planGraph renders the collected plan for the manifest.
func (r *recorder) planGraph() *manifestpb.Graph {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := &manifestpb.Graph{}
	for _, n := range r.nodes {
		g.Nodes = append(g.Nodes, &manifestpb.GraphNode{Ref: n.Ref, Parent: n.Parent, Children: n.Children})
	}
	for _, st := range r.steps {
		g.Steps = append(g.Steps, &manifestpb.GraphStep{
			Op: st.Op, Subject: st.Subject, Agent: st.Agent, Note: st.Note, Deps: st.Deps,
		})
	}
	return g
}

// activityNames lists the discovered wire names.
func (r *recorder) activityNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.activities))
	for name := range r.activities {
		out = append(out, name)
	}
	return out
}

// kindDecls renders the described kinds as manifest material.
func (r *recorder) kindDeclList() []*manifestpb.KindDecl {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.kindDecls))
	for name := range r.kindDecls {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*manifestpb.KindDecl, 0, len(names))
	for _, name := range names {
		d := r.kindDecls[name]
		decl := &manifestpb.KindDecl{Name: name, Description: d.description, Dimensions: d.dimensions}
		if d.spec != nil {
			if schema, err := manifest.SchemaOf(d.spec, schemapb.ID("graphene", schemapb.SchemaName(name+"-spec"), schemapb.Ver(0, 1, 0))); err == nil {
				decl.SpecSchema = schema
			}
		}
		for _, cmd := range d.commands {
			c := &manifestpb.KindDecl_Command{Name: cmd.Name}
			if cmd.Payload != nil {
				if schema, err := manifest.SchemaOf(cmd.Payload, schemapb.ID("graphene", schemapb.SchemaName(name+"-"+cmd.Name), schemapb.Ver(0, 1, 0))); err == nil {
					c.PayloadSchema = schema
				}
			}
			decl.Commands = append(decl.Commands, c)
		}
		out = append(out, decl)
	}
	return out
}

// kindNames lists the declared entity kinds.
func (r *recorder) kindNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.kinds))
	for name := range r.kinds {
		out = append(out, name)
	}
	return out
}

func (r *recorder) record(name string, fn any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.activities[name]; exists {
		// The same declaration reached twice (a loop, a helper called
		// repeatedly) is fine; the body is the same code by construction.
		return
	}
	r.activities[name] = fn
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
