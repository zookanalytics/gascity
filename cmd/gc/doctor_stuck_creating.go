package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
)

// stuckCreatingWarnAfter is how long a session may sit in state=creating
// before gc doctor warns. Healthy provider starts complete well under this
// bound, and the reconciler's stale-creating recovery normally clears wedged
// creates within about a minute — a session still creating after 3 minutes
// has already outlived auto-recovery.
const stuckCreatingWarnAfter = 3 * time.Minute

// stuckCreatingFailAfter (2× the warn threshold) is when the check fails
// instead of warns: the session is unambiguously stuck and needs an operator.
const stuckCreatingFailAfter = 2 * stuckCreatingWarnAfter

// stuckCreatingMessageNameCap bounds how many stuck identities the one-line
// summary message names before collapsing the rest into "+N more". The full
// list always appears in Details (verbose output).
const stuckCreatingMessageNameCap = 5

// stuckCreatingDoctorCheck reports sessions wedged in state=creating. The
// reconciler auto-recovers the common cases (reapStaleSessionBeads); this
// check is the visibility net for the variants that don't auto-recover, so
// an operator or deacon sees the stuck template without hand-rolling
// `gc session list --json` queries.
type stuckCreatingDoctorCheck struct {
	cfg      *config.City
	cityPath string
	newStore func(string) (beads.Store, error)
	// now overrides the clock for tests; nil means time.Now.
	now func() time.Time
}

func (c *stuckCreatingDoctorCheck) Name() string { return "session-stuck-creating" }

func (c *stuckCreatingDoctorCheck) CanFix() bool { return false }

func (c *stuckCreatingDoctorCheck) Fix(_ *doctor.CheckContext) error { return nil }

// stuckCreatingFinding captures one session bead wedged in state=creating.
type stuckCreatingFinding struct {
	id       string
	identity string
	age      time.Duration
	// ageKnown is false when the bead has neither a parseable
	// pending_create_started_at marker nor a CreatedAt.
	ageKnown bool
	started  time.Time
}

func (f stuckCreatingFinding) detail() string {
	if !f.ageKnown {
		return fmt.Sprintf("%s (%s) in creating with no usable start timestamp (no pending_create_started_at, zero created_at); treated as stuck", f.id, f.identity)
	}
	return fmt.Sprintf("%s (%s) in creating for %s (started %s)", f.id, f.identity, f.age.Truncate(time.Second), f.started.Format(time.RFC3339))
}

func (c *stuckCreatingDoctorCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	r := &doctor.CheckResult{Name: c.Name(), Status: doctor.StatusOK, Message: "no sessions stuck in creating state"}
	if c == nil || c.newStore == nil {
		return r
	}
	store, err := c.newStore(c.cityPath)
	if err != nil {
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("stuck-creating diagnostics skipped: %v", err)
		return r
	}
	sessions, err := session.ListAllSessionBeads(store, beads.ListQuery{Sort: beads.SortCreatedAsc})
	if err != nil {
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("stuck-creating diagnostics skipped: %v", err)
		return r
	}
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now()
	}

	var failed, warned []stuckCreatingFinding
	var details []string
	for _, b := range sessions {
		if b.Status == "closed" {
			continue
		}
		if strings.TrimSpace(b.Metadata["state"]) != string(session.StateCreating) {
			continue
		}
		f := stuckCreatingFinding{id: b.ID, identity: stuckCreatingIdentity(b)}
		if started, ok := stuckCreatingStartedAt(b); ok {
			f.started = started
			f.age = now.Sub(started)
			f.ageKnown = true
		}
		if stuckCreatingPreStartAllowlisted(c.cfg, b) {
			// Templates with pre_start commands legitimately create slowly;
			// excluded per spec, but surfaced in verbose output so a
			// genuinely wedged pre_start session is still discoverable.
			details = append(details, "allowlisted (pre_start configured): "+f.detail())
			continue
		}
		switch {
		case !f.ageKnown:
			// Nothing can ever age this bead out; mirror the reconciler's
			// zero-CreatedAt handling and treat it as stuck now.
			failed = append(failed, f)
		case f.age >= stuckCreatingFailAfter:
			failed = append(failed, f)
		case f.age >= stuckCreatingWarnAfter:
			warned = append(warned, f)
		}
	}

	for _, f := range failed {
		details = append(details, f.detail())
	}
	for _, f := range warned {
		details = append(details, f.detail())
	}
	r.Details = details

	switch {
	case len(failed) > 0:
		r.Status = doctor.StatusError
		r.Message = fmt.Sprintf("%d session(s) stuck in creating > %s: %s", len(failed), stuckCreatingFailAfter, stuckCreatingNameList(failed))
		if len(warned) > 0 {
			r.Message += fmt.Sprintf("; %d more > %s: %s", len(warned), stuckCreatingWarnAfter, stuckCreatingNameList(warned))
		}
	case len(warned) > 0:
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("%d session(s) in creating > %s: %s", len(warned), stuckCreatingWarnAfter, stuckCreatingNameList(warned))
	}
	if r.Status != doctor.StatusOK {
		r.FixHint = "inspect stuck sessions with `gc session list --json`; see engdocs/contributors/reconciler-debugging.md for the reconciler diagnosis workflow"
	}
	return r
}

// stuckCreatingStartedAt returns the timestamp anchoring how long the session
// has been in its current create attempt. It prefers the per-attempt
// pending_create_started_at marker over bead CreatedAt — the same preference
// the reconciler's staleness logic (isStaleCreating) uses — so doctor and
// reconciler agree about age. ok=false means neither anchor is usable.
func stuckCreatingStartedAt(b beads.Bead) (time.Time, bool) {
	if t, ok := parseRFC3339Metadata(b.Metadata["pending_create_started_at"]); ok {
		return t, true
	}
	if !b.CreatedAt.IsZero() {
		return b.CreatedAt, true
	}
	return time.Time{}, false
}

// stuckCreatingIdentity names the session for operator-facing output. The
// template metadata is the canonical config identity; alias and session_name
// are progressively weaker fallbacks for beads predating template stamping.
func stuckCreatingIdentity(b beads.Bead) string {
	for _, v := range []string{
		strings.TrimSpace(b.Metadata["template"]),
		strings.TrimSpace(b.Metadata["alias"]),
		strings.TrimSpace(b.Metadata["session_name"]),
	} {
		if v != "" {
			return v
		}
	}
	return "unknown template"
}

// stuckCreatingPreStartAllowlisted reports whether the session's backing
// agent template configures pre_start commands. Such templates legitimately
// spend long stretches in state=creating (heavy warmups run before the
// provider start completes), so the spec excludes them from stuck findings.
// The first identity that resolves to an agent decides; named-session
// identities resolve through their backing template.
func stuckCreatingPreStartAllowlisted(cfg *config.City, b beads.Bead) bool {
	if cfg == nil {
		return false
	}
	for _, identity := range []string{
		strings.TrimSpace(b.Metadata["template"]),
		strings.TrimSpace(b.Metadata["alias"]),
	} {
		if identity == "" {
			continue
		}
		if a := config.FindAgent(cfg, identity); a != nil {
			return len(a.PreStart) > 0
		}
		if ns := config.FindNamedSession(cfg, identity); ns != nil {
			if a := config.FindAgent(cfg, ns.TemplateQualifiedName()); a != nil {
				return len(a.PreStart) > 0
			}
		}
	}
	return false
}

// stuckCreatingNameList joins finding identities for the one-line summary,
// capped at stuckCreatingMessageNameCap names so a mass wedge stays readable.
func stuckCreatingNameList(findings []stuckCreatingFinding) string {
	names := make([]string, 0, len(findings))
	for _, f := range findings {
		names = append(names, f.identity)
	}
	if len(names) > stuckCreatingMessageNameCap {
		return strings.Join(names[:stuckCreatingMessageNameCap], ", ") + fmt.Sprintf(", +%d more", len(names)-stuckCreatingMessageNameCap)
	}
	return strings.Join(names, ", ")
}
