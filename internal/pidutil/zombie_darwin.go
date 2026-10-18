//go:build darwin

package pidutil

import (
	"errors"

	"golang.org/x/sys/unix"
)

// darwinSZOMB is the zombie value of kinfo_proc's p_stat (SZOMB in
// <sys/proc.h>).
const darwinSZOMB = 5

// platformZombie reads the process state from the kernel's kinfo_proc (sysctl
// kern.proc.pid.<pid>), the table ps itself reads, so no process is forked.
//
// A PID the kernel has no entry for was reaped after Alive's kill(0) probe:
// the sysctl succeeds with no data, which SysctlKinfoProc reports as EIO. It
// is answered like a zombie, so Alive reports it dead; ps would find nothing
// for it and report it alive. Any other sysctl error falls back to the ps
// probe.
func platformZombie(pid int) bool {
	kinfo, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if errors.Is(err, unix.EIO) {
		return true
	}
	if err != nil {
		return psReportsZombie(pid)
	}
	return kinfo.Proc.P_stat == darwinSZOMB
}
