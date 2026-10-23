//go:build !windows

package main

import "syscall"

// backgroundSysProcAttr returns SysProcAttr for a background child that
// outlives the gc that forks it. The child starts a new session, which gives
// it a new process group and no controlling terminal, and the processes it
// starts inherit that session. A descendant that opens /dev/tty to prompt,
// such as ssh asking for a passphrase, then fails at once instead of being
// stopped by SIGTTIN or SIGTTOU as a background job of the terminal gc was
// run from. Setpgid stays unset: Go's child setup calls setsid before
// setpgid, and setpgid on a session leader fails with EPERM.
func backgroundSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
