package pipeline

import (
	"testing"

	"github.com/graphene-ci/pipeline/ref"
)

func TestMachineSpecValidate(t *testing.T) {
	cloud := &CloudSource{Provider: "yc"}
	ssh := &SSHSource{Host: "10.0.0.1", User: "root", KeyRef: ref.SecretRef{Name: "ssh-key"}}
	cases := []struct {
		name string
		spec MachineSpec
		ok   bool
	}{
		{"cloud", MachineSpec{Cloud: cloud}, true},
		{"ssh", MachineSpec{SSH: ssh}, true},
		{"none", MachineSpec{}, false},
		{"both", MachineSpec{Cloud: cloud, SSH: ssh}, false},
		{"ssh without host", MachineSpec{SSH: &SSHSource{User: "root"}}, false},
		{"cloud without provider", MachineSpec{Cloud: &CloudSource{}}, false},
		{"bad owner", MachineSpec{Cloud: cloud, Owner: "run"}, false},
		{"good owner", MachineSpec{Cloud: cloud, Owner: ref.RunOwner("r-1")}, true},
	}
	for _, c := range cases {
		if err := c.spec.Validate(); (err == nil) != c.ok {
			t.Errorf("%s: err=%v want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestOwned(t *testing.T) {
	if !(MachineSpec{Cloud: &CloudSource{Provider: "yc"}}).Owned() {
		t.Error("cloud machine must be owned")
	}
	if (MachineSpec{SSH: &SSHSource{Host: "h", User: "u"}}).Owned() {
		t.Error("recognized machine must not be owned")
	}
}
