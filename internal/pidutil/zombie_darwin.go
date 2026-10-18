//go:build darwin

package pidutil

import (
	"errors"

	"golang.org/x/sys/unix"
)

// darwinSZOMB is the zombie value of kinfo_proc's p_stat (SZOMB in
// <sys/proc.h>).
const darwinSZOMB = 5

// sysctlKinfoProc reads one kinfo_proc record. It is a seam so tests can model
// a short or mismatched record, which a live sysctl does not produce on demand.
var sysctlKinfoProc = unix.SysctlKinfoProc

// platformDead reports whether pid is a zombie or has no process at all,
// reading the kernel's kinfo_proc (sysctl kern.proc.pid.<pid>), the table ps
// itself reads, so no process is forked.
//
// A PID reaped after Alive's kill(0) probe has no kernel entry, and the sysctl
// reports that as an error KinfoErrorMeansGone classifies. A record for a
// different PID, or an error that a fresh kill(0) does not confirm as gone,
// falls back to the ps probe.
func platformDead(pid int) bool {
	kinfo, err := sysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if KinfoErrorMeansGone(pid, err, func(pid int) error { return probeSignal(pid, 0) }) {
			return true
		}
		return psReportsZombie(pid)
	}
	if kinfo == nil || int(kinfo.Proc.P_pid) != pid {
		return psReportsZombie(pid)
	}
	return kinfo.Proc.P_stat == darwinSZOMB
}

// KinfoErrorMeansGone reports whether a kern.proc.pid sysctl error for pid
// means the process no longer exists. ESRCH says so directly. EIO and ENOENT
// are ambiguous: x/sys reports any record whose size is not
// SizeofKinfoProc as EIO, so an empty result for a reaped PID and a short
// record for a live one look the same. Those count as gone only when probe
// (a kill(pid, 0)) answers ESRCH; a live or permission-denied probe means the
// process is still there.
func KinfoErrorMeansGone(pid int, err error, probe func(int) error) bool {
	if errors.Is(err, unix.ESRCH) {
		return true
	}
	if (!errors.Is(err, unix.EIO) && !errors.Is(err, unix.ENOENT)) || probe == nil {
		return false
	}
	return errors.Is(probe(pid), unix.ESRCH)
}
