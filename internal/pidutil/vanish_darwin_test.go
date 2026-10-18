//go:build darwin

package pidutil

import (
	"os/exec"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// A PID reaped between Alive's kill(0) probe and its kinfo_proc read has no
// kernel entry. ps finds nothing for it either and would report it alive, so
// the probe answers it as gone without forking ps.
func TestPlatformDeadReportsAReapedPIDGoneWithoutPS(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run child: %v", err)
	}
	pid := cmd.Process.Pid

	marker := recordingPS(t)

	if !platformDead(pid) {
		t.Fatalf("platformDead(%d) = false for a reaped PID; Alive would report it alive", pid)
	}
	if psRan(marker) {
		t.Fatal("platformDead forked ps for a PID the kernel has no entry for")
	}
}

// fakeKinfo replaces the kinfo_proc read and the kill(0) probe for one test.
func fakeKinfo(t *testing.T, kinfo *unix.KinfoProc, kinfoErr, probeErr error) {
	t.Helper()
	origSysctl, origProbe := sysctlKinfoProc, probeSignal
	sysctlKinfoProc = func(string, ...int) (*unix.KinfoProc, error) { return kinfo, kinfoErr }
	probeSignal = func(int, syscall.Signal) error { return probeErr }
	t.Cleanup(func() { sysctlKinfoProc, probeSignal = origSysctl, origProbe })
}

// An error from the sysctl counts as gone only when kill(0) confirms it.
// Otherwise the process exists, and a short or mismatched record must not read
// as dead: the probe defers to ps, which here finds nothing and so answers
// alive.
func TestPlatformDeadNeedsAGoneProbeOrAMatchingRecord(t *testing.T) {
	const pid = 4242
	for _, test := range []struct {
		name     string
		kinfo    *unix.KinfoProc
		kinfoErr error
		probeErr error
		wantDead bool
		wantPS   bool
	}{
		{name: "EIO corroborated by ESRCH", kinfoErr: unix.EIO, probeErr: unix.ESRCH, wantDead: true},
		{name: "EIO with live PID", kinfoErr: unix.EIO, wantPS: true},
		{name: "EIO with denied probe", kinfoErr: unix.EIO, probeErr: unix.EPERM, wantPS: true},
		{name: "unrelated error", kinfoErr: unix.EINVAL, probeErr: unix.ESRCH, wantPS: true},
		{name: "record for another PID", kinfo: &unix.KinfoProc{Proc: unix.ExternProc{P_pid: pid + 1, P_stat: darwinSZOMB}}, wantPS: true},
		{name: "zombie record", kinfo: &unix.KinfoProc{Proc: unix.ExternProc{P_pid: pid, P_stat: darwinSZOMB}}, wantDead: true},
		{name: "live record", kinfo: &unix.KinfoProc{Proc: unix.ExternProc{P_pid: pid, P_stat: 2}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fakeKinfo(t, test.kinfo, test.kinfoErr, test.probeErr)
			marker := recordingPS(t)
			if got := platformDead(pid); got != test.wantDead {
				t.Fatalf("platformDead = %t, want %t", got, test.wantDead)
			}
			if got := psRan(marker); got != test.wantPS {
				t.Fatalf("ps ran = %t, want %t", got, test.wantPS)
			}
		})
	}
}

func TestKinfoMissingErrorsRequireGoneProbe(t *testing.T) {
	for _, test := range []struct {
		name     string
		kinfoErr error
		probeErr error
		wantGone bool
	}{
		{name: "EIO corroborated by ESRCH", kinfoErr: unix.EIO, probeErr: unix.ESRCH, wantGone: true},
		{name: "EIO with live PID", kinfoErr: unix.EIO},
		{name: "EIO with denied probe", kinfoErr: unix.EIO, probeErr: unix.EPERM},
		{name: "ENOENT corroborated by ESRCH", kinfoErr: unix.ENOENT, probeErr: unix.ESRCH, wantGone: true},
		{name: "ENOENT with live PID", kinfoErr: unix.ENOENT},
		{name: "ENOENT with denied probe", kinfoErr: unix.ENOENT, probeErr: unix.EPERM},
		{name: "unrelated error stays indeterminate", kinfoErr: unix.EACCES, probeErr: unix.ESRCH},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := KinfoErrorMeansGone(999, test.kinfoErr, func(int) error { return test.probeErr }); got != test.wantGone {
				t.Fatalf("KinfoErrorMeansGone(%v, probe=%v) = %t, want %t", test.kinfoErr, test.probeErr, got, test.wantGone)
			}
		})
	}

	if !KinfoErrorMeansGone(999, unix.ESRCH, func(int) error {
		t.Fatal("direct ESRCH should not require a liveness probe")
		return nil
	}) {
		t.Fatal("direct ESRCH was not classified as a vanished process")
	}
}
