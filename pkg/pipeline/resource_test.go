package pipeline

import "testing"

func TestDecommissionValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		policy Decommission
		ok     bool
	}{
		{policy: DecommissionNone, ok: true},
		{policy: DecommissionBestEffort, ok: true},
		{policy: DecommissionGuaranteed, ok: true},
		{policy: "always"},
	}
	for _, c := range cases {
		t.Run(string(c.policy), func(t *testing.T) {
			t.Parallel()
			if (c.policy.Validate() == nil) != c.ok {
				t.Fatalf("Validate(%q) ok=%v", c.policy, !c.ok)
			}
			if (AgentSpec{Decommission: c.policy}.Validate() == nil) != c.ok {
				t.Fatalf("AgentSpec with %q ok=%v", c.policy, !c.ok)
			}
		})
	}
}

// Options build what a declaration asks: the decommission policy reaches
// the options like every other choice.
func TestResourceOptions(t *testing.T) {
	t.Parallel()
	var o ResourceOptions
	for _, opt := range []ResourceOption{
		WithLabels(map[string]string{"stand": "lab"}),
		WithDecommission(DecommissionGuaranteed),
		Need("docker"),
	} {
		opt(&o)
	}
	if o.Labels["stand"] != "lab" || o.Decommission != DecommissionGuaranteed || len(o.Needs) != 1 || o.Needs[0].Name != "docker" {
		t.Fatalf("options %+v", o)
	}
}
