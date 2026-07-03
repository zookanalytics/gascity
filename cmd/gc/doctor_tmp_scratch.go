package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gastownhall/gascity/internal/doctor"
)

// tmpScratchSpaceCheck reports free space on the filesystem backing the
// build/test scratch temp dir ($TMPDIR, default /tmp).
//
// On this class of Gas City host /tmp is a size-capped tmpfs, and
// `make check` (`go test -p=4 ./...`) links several CGO/ICU test binaries in
// parallel; the linker scratch for each lands under $TMPDIR. The peak can
// exhaust a 16 GiB tmpfs in a burst and surface as spurious "No space left on
// device" test failures that are green in isolation — costing an operator real
// time to distinguish from genuine failures (gc-yiqil).
//
// The existing free-space guards (managed-Dolt startup preflight,
// store-maintenance DOLT_GC guard) watch only the Dolt data dir, so this
// exhaustion class is otherwise invisible to routine health checks. This check
// closes that monitoring gap.
//
// Pure observability (SeverityAdvisory): it stats the temp filesystem only,
// never mutates anything, and never gates dispatch. On platforms without
// statfs (the Windows stub) or on any probe error it reports OK and skips —
// a missing free-space reading must never turn into a health failure.
type tmpScratchSpaceCheck struct {
	// tempDir returns the scratch dir to probe. Injectable for tests;
	// production uses os.TempDir (honors $TMPDIR, defaults to /tmp).
	tempDir func() string
	// freeBytes returns the bytes available to an unprivileged process in the
	// filesystem containing path. Injectable for tests; production reuses the
	// managed-Dolt preflight's build-tagged statfs reader (containerFreeBytes).
	freeBytes func(path string) (int64, error)
	// warnFreeBytes is the free-byte floor at or below which the check warns.
	// Zero disables the check.
	warnFreeBytes int64
}

const (
	// defaultTmpScratchWarnFreeBytes is the soft floor (2 GiB). The parallel
	// CGO/ICU link phase of `make check` can consume multiple GiB of scratch in
	// a burst, so a temp filesystem below this is at real risk of a link-phase
	// ENOSPC. Matches the managed-Dolt preflight warn floor for consistency.
	defaultTmpScratchWarnFreeBytes = int64(2) << 30 // 2 GiB

	tmpScratchGiB = float64(1 << 30)
)

func newTmpScratchSpaceCheck() *tmpScratchSpaceCheck {
	return &tmpScratchSpaceCheck{
		tempDir:       os.TempDir,
		freeBytes:     containerFreeBytes,
		warnFreeBytes: tmpScratchWarnFreeBytes(),
	}
}

func (c *tmpScratchSpaceCheck) Name() string                     { return "tmp-scratch-space" }
func (c *tmpScratchSpaceCheck) CanFix() bool                     { return false }
func (c *tmpScratchSpaceCheck) Fix(_ *doctor.CheckContext) error { return nil }
func (c *tmpScratchSpaceCheck) WarmupEligible() bool             { return false }

// tmpScratchWarnFreeBytes reads the warn floor from GC_TMP_WARN_FREE_BYTES,
// defaulting to 2 GiB. A value of 0 disables the check; a negative or
// unparseable value falls back to the default.
func tmpScratchWarnFreeBytes() int64 {
	if v := strings.TrimSpace(os.Getenv("GC_TMP_WARN_FREE_BYTES")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return defaultTmpScratchWarnFreeBytes
}

func (c *tmpScratchSpaceCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	res := &doctor.CheckResult{Name: c.Name(), Severity: doctor.SeverityAdvisory}

	if c.warnFreeBytes == 0 {
		res.Status = doctor.StatusOK
		res.Message = "tmp-scratch-space: check disabled (GC_TMP_WARN_FREE_BYTES=0)"
		return res
	}

	dir := c.tempDir()
	free, err := c.freeBytes(dir)
	if err != nil {
		// Fail open: an unsupported platform (Windows stub returns
		// errDiskPreflightUnsupported) or a transient probe error must never
		// become a health failure.
		res.Status = doctor.StatusOK
		res.Message = fmt.Sprintf("tmp-scratch-space: free-space probe unavailable for %s — skipped", dir)
		return res
	}

	if free < c.warnFreeBytes {
		res.Status = doctor.StatusWarning
		res.Message = fmt.Sprintf(
			"low scratch space: %.1f GiB free on %s (warn < %.1f GiB) — parallel `make check` CGO/ICU linking can ENOSPC here",
			float64(free)/tmpScratchGiB, dir, float64(c.warnFreeBytes)/tmpScratchGiB)
		res.Details = []string{
			"On a size-capped /tmp tmpfs the `go test -p=4 ./...` link phase can exhaust the",
			"filesystem in a burst and surface as spurious 'No space left on device' test failures",
			"that pass in isolation. Routine free-space guards (managed-Dolt preflight,",
			"store-maintenance) watch only the Dolt data dir, so this class is otherwise invisible.",
			"Reclaim leaked build scratch (safe; only stale dirs):",
			fmt.Sprintf("  find %s -maxdepth 1 \\( -name 'go-build*' -o -name 'go-link*' \\) -mmin +30 -type d -exec rm -rf {} +", dir),
			"Durable fix: point the build/test TMPDIR at a non-tmpfs path (gc-v2z1p), or enlarge /tmp.",
			"Tune or disable this floor via GC_TMP_WARN_FREE_BYTES (bytes; 0 disables).",
		}
		res.FixHint = "clear stale go-build/go-link dirs from $TMPDIR, or set TMPDIR to a non-tmpfs path"
		return res
	}

	res.Status = doctor.StatusOK
	res.Message = fmt.Sprintf("tmp-scratch-space: %.1f GiB free on %s", float64(free)/tmpScratchGiB, dir)
	return res
}
