package pipeline

import (
	"testing"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

func TestAgentSpecValidate(t *testing.T) {
	ssh := &SSHInstall{
		Address: "10.0.0.1",
		User:    "root",
		KeyRef:  ref.SecretRef{Name: "ssh-key"},
		HostKey: "ssh-ed25519 AAAA...",
	}
	cases := []struct {
		name string
		spec AgentSpec
		ok   bool
	}{
		{"waiting link (no ssh)", AgentSpec{}, true},
		{"ssh install", AgentSpec{SSH: ssh}, true},
		{"ssh without address", AgentSpec{SSH: &SSHInstall{User: "root", KeyRef: ref.SecretRef{Name: "k"}, HostKey: "hk"}}, false},
		{"ssh without key", AgentSpec{SSH: &SSHInstall{Address: "h", User: "root", HostKey: "hk"}}, false},
		{"ssh without host key (no TOFU)", AgentSpec{SSH: &SSHInstall{Address: "h", User: "root", KeyRef: ref.SecretRef{Name: "k"}}}, false},
		{"bad owner", AgentSpec{Owner: "run"}, false},
		{"good owner", AgentSpec{Owner: ref.RunOwner("r-1")}, true},
	}
	for _, c := range cases {
		if err := c.spec.Validate(); (err == nil) != c.ok {
			t.Errorf("%s: err=%v want ok=%v", c.name, err, c.ok)
		}
	}
}
