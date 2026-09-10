//go:build !windows

package execgrace

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

const (
	// reinterruptInterval is how often the re-interrupt ladder re-sends the
	// interrupt to a foreground child that survived the first one and re-checks
	// whether the blockers are gone.
	reinterruptInterval = 25 * time.Millisecond
	// blockerAppearWindow bounds how long the ladder waits for a foreground
	// child to appear before concluding the first interrupt already sufficed
	// (the shell ran its trap without a blocking child). It covers the case
	// where cancellation raced the child's own fork/exec.
	blockerAppearWindow = 250 * time.Millisecond
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
// foreground child receives it alongside the shell leader. It preserves the
// os.ErrProcessDone signal the caller special-cases: an already-exited target
// reports ErrProcessDone rather than a spurious failure. If the group id cannot
// be resolved it falls back to signaling the leader directly.
func interruptProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return cmd.Process.Signal(os.Interrupt)
	}
	if killErr := syscall.Kill(-pgid, syscall.SIGINT); killErr != nil {
		if errors.Is(killErr, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return killErr
	}
	return nil
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
// A single group interrupt can miss a foreground child that was mid-fork/exec
// when it was delivered — its signal disposition not yet settled to the
// default — so the child keeps running, the shell stays blocked in wait, and
// its rollback trap is deferred until WaitDelay force-kills the shell. Once the
// child has settled a moment later, an interrupt reaches it normally.
//
// blockers is the pre-interrupt snapshot of foreground children. The ladder
// re-signals exactly those pids, never the group, so a process the trap later
// spawns (a teardown command) is a different pid and is never signaled. It
// stops the instant they are all gone, since the shell can then run its trap.
// The grace budget is cmd.WaitDelay — the same window before os/exec escalates
// to SIGKILL — so the ladder never outlives the cancellation.
func reinterruptForegroundChildren(cmd *exec.Cmd, blockers []int) {
	if cmd.Process == nil || cmd.WaitDelay <= 0 {
		return
	}
	leader := cmd.Process.Pid
	pgid, err := syscall.Getpgid(leader)
	if err != nil {
		return
	}
	deadline := time.Now().Add(cmd.WaitDelay)

	// An empty snapshot means the child was mid-fork when the interrupt was
	// delivered — the very race this rescues. Watch briefly for it to appear;
	// the shell cannot reach its trap until a foreground child returns, so a
	// child seen now is still one blocking the trap, not one the trap spawned.
	if len(blockers) == 0 {
		for appearDeadline := time.Now().Add(blockerAppearWindow); len(blockers) == 0 && time.Now().Before(appearDeadline); {
			time.Sleep(reinterruptInterval)
			blockers = foregroundGroupMembers(pgid, leader)
		}
		if len(blockers) == 0 {
			return
		}
	}

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
