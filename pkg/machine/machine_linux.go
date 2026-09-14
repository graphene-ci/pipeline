//go:build linux

package machine

import (
	"os/exec"
	"syscall"
)

// setChroot resolves Cmd.Dir inside the host filesystem before namespace entry.
// Keeping an old mount's cwd descriptor can make it unreachable in the new root;
// nsenter must reopen the same absolute path after entering the host namespace.
// There is no PID namespace in the machine executor: PID 1 is host init.
func setChroot(cmd *exec.Cmd, root string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{
		cmd.Path, "-c", hostLauncher, "graphene-host",
	}, cmd.Args...)
}
