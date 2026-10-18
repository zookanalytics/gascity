//go:build !darwin

package pidutil

// platformDead reports whether ps shows pid as a zombie. ps finds nothing for
// a vanished PID, so this answers false for one; Alive covers that case where
// /proc is mounted, and this probe is reached on linux only otherwise.
func platformDead(pid int) bool {
	return psReportsZombie(pid)
}
