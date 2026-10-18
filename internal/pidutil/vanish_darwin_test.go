//go:build darwin

package pidutil

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A PID reaped between Alive's kill(0) probe and its kinfo_proc read has no
// kernel entry. ps finds nothing for it either and would report it alive, so
// the probe answers it as gone without forking ps.
func TestPlatformZombieReportsAReapedPIDGoneWithoutPS(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run child: %v", err)
	}
	pid := cmd.Process.Pid

	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "ps-ran")
	if err := os.WriteFile(filepath.Join(binDir, "ps"), []byte(fmt.Sprintf("#!/bin/sh\n: > %q\n", marker)), 0o755); err != nil {
		t.Fatalf("WriteFile(ps): %v", err)
	}
	t.Setenv("PATH", binDir)

	if !platformZombie(pid) {
		t.Fatalf("platformZombie(%d) = false for a reaped PID; Alive would report it alive", pid)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("platformZombie forked ps for a PID the kernel has no entry for")
	}
}
