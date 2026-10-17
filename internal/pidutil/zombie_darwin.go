//go:build darwin

package pidutil

import "golang.org/x/sys/unix"

// darwinSZOMB is the zombie value of kinfo_proc's p_stat (SZOMB in
// <sys/proc.h>).
const darwinSZOMB = 5

// platformZombie reads the process state from the kernel's kinfo_proc (sysctl
// kern.proc.pid.<pid>), the table ps itself reads, so no process is forked. A
// sysctl that answers nothing (an error, or no entry for the PID) falls back to
// the ps probe, which reads the same table and answers those cases as it
// always has.
func platformZombie(pid int) bool {
	kinfo, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return psReportsZombie(pid)
	}
	return kinfo.Proc.P_stat == darwinSZOMB
}
