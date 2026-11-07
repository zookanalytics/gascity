//go:build !windows

package main

import "testing"

// TestBackgroundSysProcAttrStartsNewSession pins how a forked supervisor is
// detached. Setsid starts it in a new session with no controlling terminal,
// so a child that prompts on /dev/tty fails instead of being stopped as a
// background job of the terminal gc was run from. Setpgid must stay unset:
// Go's child setup calls setsid before setpgid, and setpgid on a session
// leader fails with EPERM, which would fail the fork.
func TestBackgroundSysProcAttrStartsNewSession(t *testing.T) {
	attr := backgroundSysProcAttr()
	if attr == nil || !attr.Setsid || attr.Setpgid {
		t.Fatalf("backgroundSysProcAttr() = %#v, want Setsid only", attr)
	}
}
