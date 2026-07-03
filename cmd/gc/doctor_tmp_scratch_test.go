package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/doctor"
)

func TestTmpScratchSpaceCheckWarnsBelowFloor(t *testing.T) {
	c := &tmpScratchSpaceCheck{
		tempDir:       func() string { return "/scratch" },
		freeBytes:     func(string) (int64, error) { return 1 << 30, nil }, // 1 GiB free
		warnFreeBytes: 2 << 30,                                             // 2 GiB floor
	}
	res := c.Run(nil)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want Warning; msg=%q", res.Status, res.Message)
	}
	// Advisory so the check reports the exhaustion risk without ever gating
	// city dispatch — /tmp scratch is a build-host concern, not a runtime gate.
	if res.Severity != doctor.SeverityAdvisory {
		t.Fatalf("severity = %v, want Advisory", res.Severity)
	}
	if !strings.Contains(res.Message, "/scratch") {
		t.Fatalf("message should name the probed dir: %q", res.Message)
	}
	if len(res.Details) == 0 || res.FixHint == "" {
		t.Fatalf("warning should carry remediation details + fix hint; details=%v hint=%q", res.Details, res.FixHint)
	}
}

func TestTmpScratchSpaceCheckOKAboveFloor(t *testing.T) {
	c := &tmpScratchSpaceCheck{
		tempDir:       func() string { return "/scratch" },
		freeBytes:     func(string) (int64, error) { return 8 << 30, nil }, // 8 GiB free
		warnFreeBytes: 2 << 30,
	}
	res := c.Run(nil)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want OK; msg=%q", res.Status, res.Message)
	}
}

func TestTmpScratchSpaceCheckFailsOpenOnProbeError(t *testing.T) {
	c := &tmpScratchSpaceCheck{
		tempDir:       func() string { return "/scratch" },
		freeBytes:     func(string) (int64, error) { return -1, errors.New("statfs boom") },
		warnFreeBytes: 2 << 30,
	}
	res := c.Run(nil)
	if res.Status != doctor.StatusOK {
		t.Fatalf("probe error must fail open to OK, got %v (%q)", res.Status, res.Message)
	}
}

func TestTmpScratchSpaceCheckDisabledWhenFloorZero(t *testing.T) {
	c := &tmpScratchSpaceCheck{
		tempDir: func() string { return "/scratch" },
		freeBytes: func(string) (int64, error) {
			t.Fatal("freeBytes must not be probed when the check is disabled")
			return 0, nil
		},
		warnFreeBytes: 0,
	}
	res := c.Run(nil)
	if res.Status != doctor.StatusOK {
		t.Fatalf("disabled check must report OK, got %v", res.Status)
	}
}

func TestTmpScratchWarnFreeBytesEnvOverride(t *testing.T) {
	t.Setenv("GC_TMP_WARN_FREE_BYTES", "1048576") // 1 MiB
	if got := tmpScratchWarnFreeBytes(); got != 1<<20 {
		t.Fatalf("override = %d, want %d", got, 1<<20)
	}
	t.Setenv("GC_TMP_WARN_FREE_BYTES", "not-a-number")
	if got := tmpScratchWarnFreeBytes(); got != defaultTmpScratchWarnFreeBytes {
		t.Fatalf("invalid override = %d, want default %d", got, defaultTmpScratchWarnFreeBytes)
	}
	t.Setenv("GC_TMP_WARN_FREE_BYTES", "-1")
	if got := tmpScratchWarnFreeBytes(); got != defaultTmpScratchWarnFreeBytes {
		t.Fatalf("negative override = %d, want default %d", got, defaultTmpScratchWarnFreeBytes)
	}
	t.Setenv("GC_TMP_WARN_FREE_BYTES", "0")
	if got := tmpScratchWarnFreeBytes(); got != 0 {
		t.Fatalf("explicit 0 = %d, want 0 (disabled)", got)
	}
}

func TestNewTmpScratchSpaceCheckDefaults(t *testing.T) {
	c := newTmpScratchSpaceCheck()
	if c.tempDir == nil || c.freeBytes == nil {
		t.Fatal("constructor left a nil dependency")
	}
	if c.Name() != "tmp-scratch-space" {
		t.Fatalf("name = %q, want tmp-scratch-space", c.Name())
	}
	if c.CanFix() {
		t.Fatal("CanFix should be false (no safe automatic remediation)")
	}
	if err := c.Fix(nil); err != nil {
		t.Fatalf("Fix should be a no-op, got %v", err)
	}
	if c.WarmupEligible() {
		t.Fatal("WarmupEligible should be false")
	}
}
