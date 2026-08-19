// Package machine is the canonical way for activity bodies to address
// THE MACHINE they run on. The per-(agent × run) container is
// packaging, not isolation: in the runc runtime the agent mounts the
// host filesystem at a fixed root and points this package at it via
// the environment; in the exec runtime the process already lives on
// the machine and everything here is the identity function.
//
// Files: machine.Path("/etc/nginx"). Scripts: machine.Shell(ctx, "...")
// — chrooted into the machine, so /bin/sh is the MACHINE's shell (the
// worker image needs none). Docker: the agent sets DOCKER_HOST to the
// machine's socket, so docker clients built FromEnv just work.
package machine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// EnvRoot names the environment variable the agent sets in the runc
// runtime: the mount point of the machine's filesystem inside the
// container. Empty (exec runtime) means the process is on the machine.
const EnvRoot = "GRAPHENE_MACHINE_ROOT"

// Root returns the machine filesystem's mount point, or "" when the
// process runs directly on the machine.
func Root() string {
	return os.Getenv(EnvRoot)
}

// Path maps a machine path to where this process reaches it.
func Path(machinePath string) string {
	root := Root()
	if root == "" {
		return machinePath
	}
	return filepath.Join(root, machinePath)
}

// Command builds a command that executes ON THE MACHINE. When the
// machine filesystem is mounted (runc), the child chroots into it
// before exec, so name resolves in the MACHINE's filesystem and must
// be absolute ("/bin/sh", "/usr/bin/git"); PATH lookup cannot cross a
// chroot. On the machine itself (exec runtime) this is plain
// exec.CommandContext.
func Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	root := Root()
	if root == "" {
		return exec.CommandContext(ctx, name, args...) //nolint:gosec // running caller commands on the machine is this package's purpose
	}
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // see above
	// The parent must not try PATH lookup in the container fs — the
	// kernel resolves name inside the chroot at execve.
	cmd.Err = nil
	cmd.Path = name
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
	// A chrooted child starts at the machine's /, with a sane PATH.
	cmd.Dir = "/"
	if !hasEnv(cmd.Env, "PATH") {
		cmd.Env = append(os.Environ(),
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return cmd
}

// Shell runs a shell script on the machine with the machine's /bin/sh.
func Shell(ctx context.Context, script string) *exec.Cmd {
	return Command(ctx, "/bin/sh", "-c", script)
}

func hasEnv(env []string, key string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, key+"=") {
			return true
		}
	}
	return false
}
