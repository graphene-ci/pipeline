//go:build linux

package machine

import (
	"os/exec"
	"syscall"
)

// setChroot starts the host's nsenter from its filesystem, then joins the
// host mount namespace. Mounts must be visible to the host Docker daemon.
// There is no PID namespace in the machine executor: PID 1 is host init.
func setChroot(cmd *exec.Cmd, root string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
	cmd.Path = "/usr/bin/nsenter"
	cmd.Args = append([]string{
		cmd.Path, "--mount=/proc/1/ns/mnt", "--root=/proc/1/root", "--wdns=/", "--",
	}, cmd.Args...)
}
