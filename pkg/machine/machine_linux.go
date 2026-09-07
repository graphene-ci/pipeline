//go:build linux

package machine

import (
	"os/exec"
	"syscall"
)

// setChroot makes the child exec inside the machine's mounted filesystem —
// the kernel resolves the (absolute) command name at execve within root.
func setChroot(cmd *exec.Cmd, root string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
}
