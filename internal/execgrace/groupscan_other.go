//go:build !windows && !linux

package execgrace

// foregroundGroupMembers has no portable implementation off Linux, where the
// process group cannot be enumerated without /proc. Returning no members makes
// the re-interrupt ladder inert on these platforms, leaving the single-signal
// cancellation behavior unchanged. The re-interrupt is a Linux-targeted
// robustness fix (the fleet and CI run Linux); porting it elsewhere means
// adding a platform-specific group scan here.
func foregroundGroupMembers(_, _ int) []int { return nil }
