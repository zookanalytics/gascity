//go:build darwin

package proctable

import (
	"fmt"

	"github.com/gastownhall/gascity/internal/pidutil"
	"golang.org/x/sys/unix"
)

// ProcessIdentity returns the high-resolution kernel start-time token used by
// SnapshotProcesses for pid.
func ProcessIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid PID %d", pid)
	}
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if pidutil.KinfoErrorMeansGone(pid, err, func(pid int) error { return unix.Kill(pid, 0) }) {
			return "", fmt.Errorf("%w: PID %d", ErrProcessGone, pid)
		}
		return "", fmt.Errorf("reading process identity for PID %d: %w", pid, err)
	}
	if process == nil {
		return "", fmt.Errorf("process identity for PID %d returned no kernel record", pid)
	}
	if int(process.Proc.P_pid) != pid {
		return "", fmt.Errorf("process identity query for PID %d returned PID %d", pid, process.Proc.P_pid)
	}
	identity, err := darwinStartIdentity(process.Proc.P_starttime)
	if err != nil {
		return "", fmt.Errorf("process identity for PID %d: %w", pid, err)
	}
	return identity, nil
}
