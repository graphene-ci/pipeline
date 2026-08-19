package machine

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestIdentityOnTheMachine(t *testing.T) {
	t.Setenv(EnvRoot, "")
	if Root() != "" {
		t.Fatal("empty env must mean on-the-machine")
	}
	if got := Path("/etc/nginx"); got != "/etc/nginx" {
		t.Fatalf("identity path: %q", got)
	}
	cmd := Command(context.Background(), "sh", "-c", "true")
	if cmd.SysProcAttr != nil {
		t.Fatal("no chroot on the machine")
	}
}

func TestMountedMachineRoot(t *testing.T) {
	t.Setenv(EnvRoot, "/host")
	if got := Path("/etc/nginx"); got != "/host/etc/nginx" {
		t.Fatalf("mapped path: %q", got)
	}
	cmd := Shell(context.Background(), "true")
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Chroot != "/host" {
		t.Fatalf("shell must chroot into the machine root: %+v", cmd.SysProcAttr)
	}
	if cmd.Path != "/bin/sh" {
		t.Fatalf("path must be the machine's absolute /bin/sh: %q", cmd.Path)
	}
	if cmd.Err != nil {
		t.Fatalf("parent-side lookup error must be cleared: %v", cmd.Err)
	}
	if !hasEnv(cmd.Env, "PATH") {
		t.Fatal("chrooted child needs a PATH")
	}
}

// Command with a relative name on the machine keeps normal lookup
// semantics (including the lookup error surfacing at Run).
func TestRelativeNameOnTheMachine(t *testing.T) {
	t.Setenv(EnvRoot, "")
	cmd := Command(context.Background(), "definitely-not-a-binary-xyz")
	if cmd.Err == nil {
		t.Skip("binary unexpectedly present in PATH")
	}
	if !strings.Contains(cmd.Err.Error(), exec.ErrNotFound.Error()) {
		t.Fatalf("unexpected lookup error: %v", cmd.Err)
	}
}

func TestWorkspace(t *testing.T) {
	t.Setenv(EnvWorkspace, "/var/lib/agent/work/run-1")
	if Workspace() != "/var/lib/agent/work/run-1" {
		t.Fatal("workspace must come from the env verbatim")
	}
}
