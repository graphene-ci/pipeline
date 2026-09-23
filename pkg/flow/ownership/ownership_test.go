package ownership

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An edge survives the trip through visibility: every field comes back,
// and the one character the encoding reserves is cut from a label rather
// than breaking the row.
func TestFlowsMirrorRoundTrip(t *testing.T) {
	t.Parallel()
	flows := []Flow{
		{To: "agent/db-1", Protocol: TCP, Port: 5432, Label: "postgres"},
		{To: "graphene-server", Protocol: OTLP, Label: "obs", Virtual: true},
		{To: "10.0.0.5", Protocol: Protocol("mqtt")},
		{To: "agent/x", Protocol: HTTP, Label: "odd|label"},
	}
	keywords := MirrorFlows(flows)
	require.Equal(t, []string{
		"agent/db-1|tcp|5432|postgres|",
		"graphene-server|otlp||obs|1",
		"10.0.0.5|mqtt|||",
		"agent/x|http||odd|",
	}, keywords)
	flows[3].Label = "odd"
	require.Equal(t, flows, FlowsFromMirror(keywords))
	require.Empty(t, FlowsFromMirror([]string{"", "garbage", "a|b|notaport||"}), "foreign keywords are skipped")
}
