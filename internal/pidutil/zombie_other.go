//go:build !darwin

package pidutil

func platformZombie(pid int) bool {
	return psReportsZombie(pid)
}
