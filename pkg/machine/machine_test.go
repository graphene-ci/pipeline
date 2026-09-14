package machine

import (
	"context"
	"os/exec"
	"slices"
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
	if cmd.Path != "/usr/bin/nsenter" {
		t.Fatalf("path must be the machine's nsenter: %q", cmd.Path)
	}
	want := []string{"/usr/bin/nsenter", "--mount=/proc/1/ns/mnt", "--root=/proc/1/root", "--", "/bin/sh", "-c", "true"}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("host namespace command: %q", cmd.Args)
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

func TestHostCommandEnvironment(t *testing.T) {
	t.Setenv(EnvRoot, "/host")
	t.Setenv("DOCKER_HOST", "unix:///host/run/docker.sock")
	cmd := Shell(context.Background(), "true")
	if !slices.Contains(cmd.Env, "DOCKER_HOST=unix:///run/docker.sock") ||
		!slices.Contains(cmd.Env, EnvRoot+"=") {
		t.Fatal("host command retained executor paths")
	}
	t.Setenv("DOCKER_HOST", "tcp://docker.internal:2376")
	cmd = Shell(context.Background(), "true")
	if !slices.Contains(cmd.Env, "DOCKER_HOST=tcp://docker.internal:2376") {
		t.Fatal("explicit external Docker endpoint changed")
	}
}
