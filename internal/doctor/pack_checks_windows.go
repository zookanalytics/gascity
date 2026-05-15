//go:build windows

package doctor

import "os/exec"

// preparePackCmdForTimeout is a no-op on Windows: there is no portable
// process-group equivalent, so we rely on Cmd.Cancel + Process.Kill.
func preparePackCmdForTimeout(_ *exec.Cmd) {}

func killPackCmdTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
