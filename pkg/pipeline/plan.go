package pipeline

// The plan subcommand: render what the recording pass saw — the
// optimistic zero path — without a server, a token, or a start. The
// same graph travels in the manifest, so the installation's copy (what
// the UI shows) and this local render come from one source.

import (
	"flag"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// cmdPlan renders the pipeline's plan from the manifest.
func cmdPlan(manifestJSON []byte, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	output := fs.String("o", "text", "output: text | json | mermaid")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(manifestJSON, &m); err != nil {
		return err
	}
	switch *output {
	case "json":
		raw, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m.GetGraph())
		if err != nil {
			return err
		}
		fmt.Println(string(raw))
		return nil
	case "mermaid":
		fmt.Print(planMermaid(&m))
		return nil
	case "text":
		fmt.Print(planText(&m))
		return nil
	default:
		return fmt.Errorf("unknown output %q (want text, json or mermaid)", *output)
	}
}

// planText renders the plan for a terminal: the ownership tree, then
// the ordered steps with their data dependencies.
func planText(m *manifestpb.Manifest) string {
	var b strings.Builder
	g := m.GetGraph()
	fmt.Fprintf(&b, "pipeline %s — plan of the OPTIMISTIC ZERO PATH\n", m.GetPipelineId())
	fmt.Fprintf(&b, "(branches on runtime values are not visible; declarations are)\n\n")

	if len(m.GetTriggers()) > 0 || m.GetConcurrency() != "" {
		fmt.Fprintf(&b, "starts:\n")
		fmt.Fprintf(&b, "  manual (run)\n")
		for _, t := range m.GetTriggers() {
			switch t.GetKind() {
			case "cron":
				fmt.Fprintf(&b, "  cron %q (%s)\n", t.GetSpec(), t.GetName())
			case "webhook":
				fmt.Fprintf(&b, "  webhook /hooks/.../%s\n", t.GetName())
			}
		}
		policy := m.GetConcurrency()
		if policy == "" {
			policy = "queue"
		}
		fmt.Fprintf(&b, "  concurrency: %s\n\n", policy)
	}

	fmt.Fprintf(&b, "resources (ownership tree; unparented belong to the run):\n")
	children := map[string][]string{}
	roots := []string{}
	for _, n := range g.GetNodes() {
		if n.GetParent() == "" {
			roots = append(roots, n.GetRef())
			continue
		}
		children[n.GetParent()] = append(children[n.GetParent()], n.GetRef())
	}
	var walk func(ref, indent string)
	walk = func(ref, indent string) {
		fmt.Fprintf(&b, "%s%s\n", indent, ref)
		for _, child := range children[ref] {
			walk(child, indent+"  ")
		}
	}
	for _, root := range roots {
		walk(root, "  ")
	}
	fmt.Fprintf(&b, "\nsteps:\n")
	for i, st := range g.GetSteps() {
		line := fmt.Sprintf("  %2d. %-12s %s", i+1, st.GetOp(), st.GetSubject())
		if st.GetAgent() != "" {
			line += "  @ " + st.GetAgent()
		}
		if st.GetNote() != "" {
			line += "  [" + st.GetNote() + "]"
		}
		if len(st.GetDeps()) > 0 {
			line += "  (needs: " + strings.Join(st.GetDeps(), ", ") + ")"
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

// planMermaid renders the plan as a flowchart: resources as nodes,
// ownership and data edges.
func planMermaid(m *manifestpb.Manifest) string {
	var b strings.Builder
	g := m.GetGraph()
	b.WriteString("flowchart TD\n")
	ids := map[string]string{}
	nodeId := func(ref string) string {
		if id, ok := ids[ref]; ok {
			return id
		}
		id := fmt.Sprintf("n%d", len(ids))
		ids[ref] = id
		fmt.Fprintf(&b, "  %s[\"%s\"]\n", id, ref)
		return id
	}
	for _, n := range g.GetNodes() {
		id := nodeId(n.GetRef())
		if n.GetParent() != "" {
			fmt.Fprintf(&b, "  %s -->|owns| %s\n", nodeId(n.GetParent()), id)
		}
	}
	for i, st := range g.GetSteps() {
		if st.GetOp() == "declare" {
			continue
		}
		sid := fmt.Sprintf("s%d", i)
		label := st.GetOp() + ": " + st.GetSubject()
		if st.GetAgent() != "" {
			label += " @ " + st.GetAgent()
		}
		fmt.Fprintf(&b, "  %s([\"%s\"])\n", sid, label)
		for _, dep := range st.GetDeps() {
			fmt.Fprintf(&b, "  %s -.->|ready| %s\n", nodeId(dep), sid)
		}
	}
	return b.String()
}
