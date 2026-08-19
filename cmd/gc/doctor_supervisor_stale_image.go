package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gastownhall/gascity/internal/doctor"
)

// supervisorStaleImageCheckTimeout bounds the /health query used to
// corroborate a stale image with a build-identity comparison. The check is
// advisory, so a slow or wedged supervisor degrades to the uncorroborated
// verdict rather than stalling the doctor run.
const supervisorStaleImageCheckTimeout = 3 * time.Second

// supervisorStaleImageCheck reports when the supervisor holding the city is
// running a binary whose inode has been unlinked — an image no rebuild can
// reach and no reload can refresh.
//
// This is the check that was missing in gc-f1081. A holder on a deleted inode
// wedged the city for five days: every landed fix was inert in the running
// city, `gc supervisor run` crash-looped 83439 times against it, and nothing
// surfaced the condition. `gc doctor` had no binary-identity coverage at all —
// drift detection lived only on the `gc start` path — so the state was found
// by hand, a day late, during unrelated patrol.
//
// A deleted image is not on its own a defect: every `go install` unlinks the
// inode the running supervisor still holds, so the marker is the normal state
// between a rebuild and the next restart. What makes it a defect is a deleted
// image the supervisor is *still serving* when the built code has moved on.
// The check therefore reports the bare marker as a warning ("restart pending")
// and escalates to an error only when a build-identity comparison confirms the
// running city is not serving the built code.
//
// Both verdicts are advisory. The warning form is routine in any tree that
// rebuilds gc, so gating dispatch on it would halt normal development; the
// value here is visibility, which the doctor report and its JSON provide
// without blocking.
type supervisorStaleImageCheck struct {
	pid int
	// localBuildID is the build identity of the gc binary running this
	// check. Empty or "unknown" means the comparison is unavailable.
	localBuildID string
	// classify resolves the holder's image state. Injectable for tests.
	classify func(pid int) (supervisorHolderState, string)
	// supervisorBuildID returns the build identity the running supervisor
	// reports over /health. Injectable for tests.
	supervisorBuildID func(ctx context.Context) (string, error)
}

// newSupervisorStaleImageCheck builds the check for the supervisor at pid.
// A pid of 0 means no supervisor is running and the check reports OK.
func newSupervisorStaleImageCheck(pid int) *supervisorStaleImageCheck {
	return &supervisorStaleImageCheck{
		pid:               pid,
		localBuildID:      commit,
		classify:          classifySupervisorHolder,
		supervisorBuildID: liveSupervisorBuildID,
	}
}

// liveSupervisorBuildID queries the running supervisor's /health for the build
// identity it was compiled from.
func liveSupervisorBuildID(ctx context.Context) (string, error) {
	baseURL, err := supervisorAPIBaseURLHook()
	if err != nil {
		return "", err
	}
	status, err := newHTTPSupervisorClient(baseURL).Status(ctx)
	if err != nil {
		return "", err
	}
	return status.BuildID, nil
}

// Name returns the check identifier.
func (c *supervisorStaleImageCheck) Name() string { return "supervisor-stale-image" }

// CanFix reports that this check does not support automatic remediation.
// Replacing the holder restarts the city, which is the operator's call.
func (c *supervisorStaleImageCheck) CanFix() bool { return false }

// Fix is a no-op; CanFix returns false.
func (c *supervisorStaleImageCheck) Fix(_ *doctor.CheckContext) error { return nil }

// WarmupEligible returns false: this is a steady-state health question, and
// `gc start` already runs its own drift check on the same supervisor.
func (c *supervisorStaleImageCheck) WarmupEligible() bool { return false }

// Run classifies the supervisor's running image and reports whether the city
// is being served by a binary that no longer exists on disk.
func (c *supervisorStaleImageCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	r := &doctor.CheckResult{Name: c.Name(), Severity: doctor.SeverityAdvisory}
	if c.pid == 0 {
		r.Status = doctor.StatusOK
		r.Message = "supervisor not running — image check skipped"
		return r
	}

	state, exePath := c.classify(c.pid)
	switch state {
	case supervisorHolderLive:
		r.Status = doctor.StatusOK
		r.Message = fmt.Sprintf("supervisor (PID %d) is running an image present on disk", c.pid)
		r.Details = []string{"exe: " + exePath}
		return r
	case supervisorHolderUnknown:
		// No /proc (darwin), a uid mismatch, or a non-definitive stat error.
		// Absence of evidence is not a finding; say so rather than guess.
		r.Status = doctor.StatusOK
		r.Message = fmt.Sprintf("supervisor (PID %d) image could not be verified", c.pid)
		r.Details = []string{fmt.Sprintf("could not read /proc/%d/exe (different uid, or no procfs on this platform)", c.pid)}
		return r
	}

	// Stale image confirmed. Corroborate with build identity to separate a
	// routine post-rebuild restart-pending state from a wedged supervisor.
	ctx, cancel := context.WithTimeout(context.Background(), supervisorStaleImageCheckTimeout)
	defer cancel()
	svBuildID, buildErr := c.supervisorBuildID(ctx)
	drifted, comparable := supervisorBuildDrifted(c.localBuildID, svBuildID, buildErr)

	details := []string{
		fmt.Sprintf("unlinked image, last on disk at: %s", exePath),
		fmt.Sprintf("verify with: ls -la /proc/%d/exe", c.pid),
	}
	if comparable {
		details = append(details, fmt.Sprintf("supervisor build %s, local build %s", svBuildID, c.localBuildID))
	}

	if drifted {
		r.Status = doctor.StatusError
		r.Message = fmt.Sprintf("supervisor (PID %d) is serving a DELETED image that no longer matches the built code — landed changes are not live in this city", c.pid)
		r.FixHint = "restart the supervisor to pick up the built binary: 'gc stop --force && gc start' (a holder on a deleted inode cannot be refreshed in place)"
		r.Details = details
		return r
	}

	r.Status = doctor.StatusWarning
	r.Message = fmt.Sprintf("supervisor (PID %d) is running a deleted image (restart pending)", c.pid)
	r.FixHint = "expected shortly after a rebuild; 'gc start' restarts the supervisor onto the new binary"
	r.Details = details
	return r
}

// supervisorBuildDrifted reports whether the supervisor's reported build
// identity differs from the local one, and whether the two were comparable at
// all. An unavailable or unknown identity on either side yields
// (false, false): the caller must not escalate on a comparison it could not
// make.
func supervisorBuildDrifted(localBuildID, supervisorBuildID string, queryErr error) (drifted, comparable bool) {
	if queryErr != nil {
		return false, false
	}
	if !knownBuildID(localBuildID) || !knownBuildID(supervisorBuildID) {
		return false, false
	}
	return localBuildID != supervisorBuildID, true
}

// knownBuildID reports whether a build identity is usable for comparison.
// The build-metadata defaults leave "unknown" in place when neither ldflags
// nor embedded VCS info supplied a revision, so it is not a real identity.
func knownBuildID(id string) bool {
	return id != "" && id != "unknown"
}
