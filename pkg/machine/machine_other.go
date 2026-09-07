//go:build !linux

package machine

import "os/exec"

// setChroot is a no-op off Linux: the machine role only ever runs on Linux,
// but the CLI and other tools cross-compile this package for macOS/Windows
// and must still build.
func setChroot(_ *exec.Cmd, _ string) {}
