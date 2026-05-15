//go:build !windows

package doctor

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// preparePackCmdForTimeout puts the script in a new process group so
// killPackCmdTree can SIGKILL the whole tree when the context fires.
// Without this, only the immediate shell child receives the signal —
// long-running grandchildren (eg `sleep`) keep the stdout pipe open
// and CombinedOutput waits until they exit on their own.
func preparePackCmdForTimeout(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killPackCmdTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil {
		if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil &&
			!errors.Is(killErr, os.ErrProcessDone) &&
			!errors.Is(killErr, syscall.ESRCH) {
			return killErr
		}
		return nil
	}
	if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
		return killErr
	}
	return nil
}
