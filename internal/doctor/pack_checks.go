package doctor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/citylayout"
)

// defaultPackScriptTimeout bounds an individual pack doctor Run or Fix
// script when the caller does not configure one. A hung pack script is
// the documented failure mode (eg `bd --version` wedged on a contended
// lock); without a per-script ceiling, `gc doctor` blocks indefinitely
// on the first wedged check. 30s is long enough for cold-start tools
// that touch disk/network and short enough that an operator notices.
const defaultPackScriptTimeout = 30 * time.Second

// PackScriptCheck implements Check by running a script shipped with
// a pack. The script follows the pack doctor protocol:
//
//   - Exit 0 = OK, Exit 1 = Warning, Exit 2 = Error
//   - First line of stdout = message (shown after check name)
//   - Remaining stdout lines = details (shown in verbose mode)
//
// The script receives environment variables:
//
//	GC_CITY_PATH    — absolute path to the city root
//	GC_PACK_DIR — absolute path to the pack directory
//
// When FixScript is non-empty, the check also supports `gc doctor --fix`:
// the fix script is dispatched with the same environment contract as
// Script. Exit 0 = remediation succeeded (Fix returns nil); non-zero
// exit surfaces as a fix error (Fix returns an error carrying the
// exit code and captured output). Packs opt into auto-remediation by
// declaring `fix = "..."` in their pack.toml [[doctor]] entry (or in a
// convention-discovered doctor/<name>/doctor.toml manifest).
type PackScriptCheck struct {
	// CheckName is the fully-qualified name, e.g. "maintenance:check-binaries".
	CheckName string
	// Script is the absolute path to the check script.
	Script string
	// FixScript is the absolute path to the remediation script, or
	// empty when the check is diagnostic-only. When set, CanFix returns
	// true and Fix dispatches to this script.
	FixScript string
	// PackDir is the absolute pack directory path.
	PackDir string
	// PackName is the logical pack name used for runtime env injection.
	PackName string
	// Timeout bounds how long Run or Fix may take before the subprocess
	// is killed and the operation reported as an error. Zero falls
	// back to defaultPackScriptTimeout.
	Timeout time.Duration
}

// Name returns the check's fully-qualified name.
func (c *PackScriptCheck) Name() string { return c.CheckName }

// CanFix reports whether the pack declared a fix script for this check.
// When true, `gc doctor --fix` will dispatch to FixScript after the
// check returns a non-OK status.
func (c *PackScriptCheck) CanFix() bool { return c.FixScript != "" }

// timeout returns the effective wall-clock bound for a single Run or
// Fix invocation. Zero or negative Timeout falls back to the package
// default so a forgotten value never silently disables the safety net.
func (c *PackScriptCheck) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultPackScriptTimeout
}

// Fix runs the pack's fix script with the same environment contract as
// Run. Returns nil on exit 0 (remediation succeeded); returns an error
// carrying the exit code and any captured output on non-zero exit or
// if the script cannot be executed. When FixScript is empty this is a
// no-op and returns nil — callers should gate on CanFix first.
func (c *PackScriptCheck) Fix(ctx *CheckContext) error {
	if c.FixScript == "" {
		return nil
	}

	timeout := c.timeout()
	cmdCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, c.FixScript) //nolint:gosec // path from pack config
	cmd.Dir = c.PackDir
	cmd.Env = append(cmd.Environ(), citylayout.PackRuntimeEnv(ctx.CityPath, c.PackName)...)
	cmd.Env = append(cmd.Env,
		"GC_PACK_DIR="+c.PackDir,
	)
	preparePackCmdForTimeout(cmd)
	cmd.Cancel = func() error { return killPackCmdTree(cmd) }
	// WaitDelay forces CombinedOutput to return promptly once the
	// context fires — without it, an orphaned grandchild that inherits
	// the stdout pipe can keep cmd.Wait() blocked for the lifetime
	// of that child, defeating the timeout entirely.
	cmd.WaitDelay = 250 * time.Millisecond

	start := time.Now()
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start)

	// Surface the timeout case first so it's never masked by exec.ExitError
	// (the kernel kills the process when the context fires; some shells
	// translate that to a non-zero exit code rather than a context error).
	if cmdCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("fix script %s timed out after %s (limit %s)",
			c.CheckName, elapsed.Round(time.Millisecond), timeout)
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			output := strings.TrimSpace(string(out))
			if output == "" {
				return fmt.Errorf("fix script exited with status %d", exitErr.ExitCode())
			}
			return fmt.Errorf("fix script exited with status %d: %s", exitErr.ExitCode(), output)
		}
		return fmt.Errorf("fix script error: %w", err)
	}
	return nil
}

// Run executes the pack script and interprets its output.
func (c *PackScriptCheck) Run(ctx *CheckContext) *CheckResult {
	timeout := c.timeout()
	runCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, c.Script) //nolint:gosec // script path from pack config
	cmd.Dir = c.PackDir
	cmd.Env = append(cmd.Environ(), citylayout.PackRuntimeEnv(ctx.CityPath, c.PackName)...)
	cmd.Env = append(cmd.Env,
		"GC_PACK_DIR="+c.PackDir,
	)
	preparePackCmdForTimeout(cmd)
	cmd.Cancel = func() error { return killPackCmdTree(cmd) }
	cmd.WaitDelay = 250 * time.Millisecond

	start := time.Now()
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start)

	if runCtx.Err() == context.DeadlineExceeded {
		return &CheckResult{
			Name:   c.CheckName,
			Status: StatusError,
			Message: fmt.Sprintf("%s timed out after %s (limit %s)",
				c.CheckName, elapsed.Round(time.Millisecond), timeout),
		}
	}

	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			// Script not found or not executable.
			return &CheckResult{
				Name:    c.CheckName,
				Status:  StatusError,
				Message: "script error: " + err.Error(),
			}
		}
	}

	message, details := parseScriptOutput(string(out))
	if message == "" {
		message = "check completed"
	}

	var status CheckStatus
	switch exitCode {
	case 0:
		status = StatusOK
	case 1:
		status = StatusWarning
	default:
		status = StatusError
	}

	return &CheckResult{
		Name:    c.CheckName,
		Status:  status,
		Message: message,
		Details: details,
	}
}

// parseScriptOutput splits script output into a message (first line)
// and details (remaining non-empty lines).
func parseScriptOutput(output string) (string, []string) {
	output = strings.TrimSpace(output)
	if output == "" {
		return "", nil
	}

	lines := strings.Split(output, "\n")
	message := strings.TrimSpace(lines[0])

	var details []string
	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			details = append(details, trimmed)
		}
	}
	return message, details
}
