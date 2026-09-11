//go:build !windows

package execgrace

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// reinterruptInterval is how often the re-interrupt ladder re-sends the
// interrupt to a foreground child that survived the first one and re-checks
// whether the blockers are gone.
const reinterruptInterval = 25 * time.Millisecond

// getpgid and killProcessGroup are indirection seams over the corresponding
// syscalls so tests can force the fallback branches below deterministically —
// a real process group cannot be made to fail Getpgid or a group kill on
// demand.
var (
	getpgid          = syscall.Getpgid
	killProcessGroup = syscall.Kill
)

// setProcessGroup puts the command in its own process group so a cooperative
// cancellation can be delivered to the whole group — reaching any foreground
// child (for example a long-running git checkout under a setup shell) that
// would otherwise keep the shell from running its rollback trap before the
// forced kill.
func setProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// interruptProcessGroup sends os.Interrupt to the command's process group so a
// foreground child receives it alongside the shell leader, reporting which
// path delivered the signal. It preserves the os.ErrProcessDone signal the
// caller special-cases: an already-exited target reports ErrProcessDone
// rather than a spurious failure. If the group id cannot be resolved it falls
// back to signaling the leader directly. The returned CancelOutcome is only
// meaningful when the error is nil.
func interruptProcessGroup(cmd *exec.Cmd) (CancelOutcome, error) {
	if cmd.Process == nil {
		return CancelNotDelivered, os.ErrProcessDone
	}
	pgid, err := getpgid(cmd.Process.Pid)
	if err != nil {
		if sigErr := cmd.Process.Signal(os.Interrupt); sigErr != nil {
			return CancelNotDelivered, sigErr
		}
		return CancelLeaderSignaledOnly, nil
	}
	if killErr := killProcessGroup(-pgid, syscall.SIGINT); killErr != nil {
		if errors.Is(killErr, syscall.ESRCH) {
			return CancelNotDelivered, os.ErrProcessDone
		}
		return CancelNotDelivered, killErr
	}
	return CancelGroupSignaled, nil
}

// foregroundGroupMembersOf snapshots the command's foreground children — the
// process group members other than the leader. Taken before the interrupt,
// while the shell is still blocked in them and its rollback trap has not
// started, the snapshot cannot include a process the trap later spawns, so the
// re-interrupt ladder can safely re-signal exactly these pids.
func foregroundGroupMembersOf(cmd *exec.Cmd) []int {
	if cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return nil
	}
	return foregroundGroupMembers(pgid, cmd.Process.Pid)
}

// reinterruptForegroundChildren re-sends the interrupt to a foreground child
// that survived the first group interrupt, until it exits or the grace budget
// runs out. It runs in its own goroutine for the life of the cancellation.
//
// A single group interrupt can miss a foreground child whose signal disposition
// had not yet settled to the default when it was delivered (it was mid-exec), so
// the child keeps running, the shell stays blocked in wait, and its rollback
// trap is deferred until WaitDelay force-kills the shell. Re-sending the
// interrupt once the disposition settles reaches the child normally and frees
// the shell to run its trap.
//
// blockers is the pre-interrupt snapshot of foreground children, and the ladder
// acts only on it: it re-signals exactly those pids, never the group, so a
// process the trap later spawns (a teardown command) is a different pid and is
// never signaled. It stops the instant the blockers are gone, since the shell
// can then run its trap. The grace budget is cmd.WaitDelay — the same window
// before os/exec escalates to SIGKILL — so the ladder never outlives the
// cancellation.
//
// An empty snapshot is inert. With no foreground child captured before the
// interrupt there is nothing that could have missed it, and a pid that appears
// afterward cannot be re-signaled safely: it is indistinguishable from a
// teardown process the shell's trap spawned once it ran, which interrupting
// would cut short.
func reinterruptForegroundChildren(cmd *exec.Cmd, blockers []int) {
	if cmd.Process == nil || cmd.WaitDelay <= 0 || len(blockers) == 0 {
		return
	}
	deadline := time.Now().Add(cmd.WaitDelay)
	for time.Now().Before(deadline) {
		alive := alivePids(blockers)
		if len(alive) == 0 {
			// The blockers are gone: the shell's wait has returned and it can
			// run its trap. Stop before the trap can spawn a child.
			return
		}
		for _, pid := range alive {
			_ = syscall.Kill(pid, syscall.SIGINT)
		}
		time.Sleep(reinterruptInterval)
	}
}

// alivePids returns the subset of pids that still exist. A zombie counts as
// alive; it is reaped by its parent a moment later and drops out on the next
// check. Signal 0 performs the liveness check without delivering a signal.
func alivePids(pids []int) []int {
	var alive []int
	for _, pid := range pids {
		if syscall.Kill(pid, 0) == nil {
			alive = append(alive, pid)
		}
	}
	return alive
}
