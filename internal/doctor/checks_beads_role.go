package doctor

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// BeadsRoleCheck verifies that beads.role is set in global git config.
// bd exits non-zero with "beads.role not configured" (gastownhall/beads#2950)
// when this key is absent, causing the config-set calls in gc-beads-bd's
// op_init to fail silently (they use || true). The silent failures leave
// issue_prefix and types.custom unset in the Dolt database, making every
// subsequent bd-create call fail with "database not initialized".
type BeadsRoleCheck struct{}

// Name returns the check identifier.
func (c *BeadsRoleCheck) Name() string { return "beads-role" }

// Run checks that beads.role is set in global git config.
func (c *BeadsRoleCheck) Run(_ *CheckContext) *CheckResult {
	r := &CheckResult{Name: c.Name()}
	out, err := exec.Command("git", "config", "--global", "beads.role").Output()
	if err != nil || strings.TrimSpace(string(out)) == "" {
		r.Status = StatusError
		r.Message = "beads.role not set in global git config"
		r.FixHint = "run: git config --global beads.role maintainer"
		return r
	}
	r.Status = StatusOK
	r.Message = fmt.Sprintf("beads.role = %q", strings.TrimSpace(string(out)))
	return r
}

// CanFix returns true — the missing role can be set automatically.
func (c *BeadsRoleCheck) CanFix() bool { return true }

// Fix sets beads.role to "maintainer" in global git config if it is not
// already set. A non-empty existing value is left unchanged.
func (c *BeadsRoleCheck) Fix(_ *CheckContext) error {
	out, err := exec.Command("git", "config", "--global", "beads.role").CombinedOutput()
	if err == nil && strings.TrimSpace(string(out)) != "" {
		return nil
	}
	// A set-but-empty GIT_CONFIG_GLOBAL resolves git's global config file to
	// the empty path. The read above survives it (git treats the empty path as
	// /dev/null) and so always falls through to the write below, which fails
	// with "error: could not write config file : <errno>". The empty filename
	// between the colon and the errno is the only clue to the cause, and the
	// errno text is not stable across systems — decoding that cost a full
	// investigation once already (gc-f7wx8). Name the variable instead. An
	// unset variable is the normal state and must not trip this.
	if v, ok := os.LookupEnv("GIT_CONFIG_GLOBAL"); ok && v == "" {
		return fmt.Errorf("setting beads.role: GIT_CONFIG_GLOBAL is set but empty, so git has no global config file to write to; unset it, or point it at a real writable file")
	}
	writeOut, writeErr := exec.Command("git", "config", "--global", "beads.role", "maintainer").CombinedOutput()
	if writeErr != nil {
		writeMsg := strings.TrimSpace(string(writeOut))
		if err != nil {
			readMsg := strings.TrimSpace(string(out))
			if readMsg == "" {
				readMsg = err.Error()
			}
			if writeMsg != "" {
				return fmt.Errorf("setting beads.role after reading current value failed (%s): %s: %w", readMsg, writeMsg, writeErr)
			}
			return fmt.Errorf("setting beads.role after reading current value failed (%s): %w", readMsg, writeErr)
		}
		if writeMsg != "" {
			return fmt.Errorf("setting beads.role: %s: %w", writeMsg, writeErr)
		}
		return fmt.Errorf("setting beads.role: %w", writeErr)
	}
	return nil
}
