package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/usage"
)

// tokenTelemetrySilenceThreshold is how long an awake session may go without a
// recorded model-usage sample before the check reports it. It doubles as the
// post-wake grace period, so a session that just started is never flagged for
// not having emitted yet.
//
// It must stay comfortably above liveModelSweepMinInterval: the in-interval sweep
// is what keeps a self-driving agent's samples flowing, so a threshold at or
// below its cadence would report sessions that are merely between sweeps.
const tokenTelemetrySilenceThreshold = time.Hour

// tokenTelemetryReadLimit bounds how much of the usage log the check reads from
// the newest end. The log grows without bound on a busy city and this check runs
// inside an interactive `gc doctor`, so it reads a bounded tail rather than the
// full history — the check only ever asks "is there a sample newer than the
// threshold", which the newest records answer.
const tokenTelemetryReadLimit = 8 << 20 // 8 MiB

// agentTokenTelemetryCheck reports awake sessions that have recorded no model
// usage for longer than tokenTelemetrySilenceThreshold.
//
// It exists because the absence of a telemetry series is indistinguishable from
// zero spend. Cost is skipped rather than zero-filled for an unpriced model, and
// a session whose interval never ends was, until the in-interval sweep, never
// swept at all — so a whole tier of agents could answer "0 calls / $0.00" while
// running continuously, and nothing in the metrics said otherwise (gc-kawr5).
// This check is that missing signal: it makes the next blind spot announce
// itself instead of reading as a confident zero.
//
// Detection only, and deliberately a warning rather than an error: an awake but
// genuinely idle session — a converse thread nobody has typed into for an hour —
// is silent for a benign reason, and the two are not separable from bead state
// alone. The finding names the session so an operator can tell them apart.
type agentTokenTelemetryCheck struct {
	cityPath string
	newStore func(string) (beads.Store, error)
	// now is injectable so tests can pin the clock against fixture timestamps.
	now func() time.Time
}

// newAgentTokenTelemetryCheck constructs an agentTokenTelemetryCheck.
func newAgentTokenTelemetryCheck(cityPath string, newStore func(string) (beads.Store, error)) *agentTokenTelemetryCheck {
	return &agentTokenTelemetryCheck{cityPath: cityPath, newStore: newStore, now: time.Now}
}

// Name returns the check's identifier.
func (c *agentTokenTelemetryCheck) Name() string { return "agent-token-telemetry" }

// CanFix reports that this check is detection-only.
func (c *agentTokenTelemetryCheck) CanFix() bool { return false }

// WarmupEligible returns false: the check measures steady-state emission over a
// one-hour window, and at `gc start` every session is inside its grace period,
// so running it there could only ever report nothing.
func (c *agentTokenTelemetryCheck) WarmupEligible() bool { return false }

// Fix is a no-op; this check never auto-repairs findings.
func (c *agentTokenTelemetryCheck) Fix(_ *doctor.CheckContext) error { return nil }

// Run compares the city's awake sessions against the newest model-usage facts
// and reports any session past the silence threshold with nothing recorded.
func (c *agentTokenTelemetryCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	if c.newStore == nil || strings.TrimSpace(c.cityPath) == "" {
		return okCheck(c.Name(), "no city bead store to inspect")
	}
	now := c.now().UTC()

	usagePath := filepath.Join(c.cityPath, ".gc", "usage.jsonl")
	facts, _, err := usage.ReadRecentFacts(usagePath, tokenTelemetryReadLimit)
	if err != nil {
		return warnCheck(c.Name(),
			"could not read the usage log to verify agent token telemetry",
			"fix access to <city>/.gc/usage.jsonl, then rerun gc doctor",
			[]string{fmt.Sprintf("reading %s: %v", usagePath, err)})
	}
	if len(facts) == 0 {
		// Either usage recording is off ([usage] provider = "discard") or the
		// city has never recorded a fact. Neither is evidence of a telemetry
		// gap, and reporting it would make the check permanently red on a city
		// that opted out of usage accounting.
		return okCheck(c.Name(), "no usage facts recorded; agent token telemetry not verifiable")
	}

	store, err := c.newStore(c.cityPath)
	if err != nil {
		return warnCheck(c.Name(),
			"could not open the city bead store to list awake sessions",
			"fix bead store access, then rerun gc doctor",
			[]string{fmt.Sprintf("opening bead store: %v", err)})
	}

	cutoff := now.Add(-tokenTelemetrySilenceThreshold)
	lastSampleBySession, newestSample := tokenSampleIndex(facts)

	awake, silent := c.scanAwakeSessions(store, lastSampleBySession, now, cutoff)
	if len(silent) == 0 {
		return okCheck(c.Name(),
			fmt.Sprintf("all %d awake session(s) past the grace period have recent token samples", awake))
	}
	sort.Strings(silent)

	// Every awake session silent at once is a different diagnosis from one
	// silent session among many: it points at the emission path having stopped
	// rather than at a single agent, which is what a shared cutoff timestamp
	// across many agents looks like.
	fleetWide := len(silent) == awake && awake > 1
	message := fmt.Sprintf("%d of %d awake session(s) have recorded no token samples in the last %s",
		len(silent), awake, tokenTelemetrySilenceThreshold)
	hint := "confirm the session is genuinely idle; if it is working, its invocation telemetry is not being recorded"
	if fleetWide {
		message = fmt.Sprintf("no token samples recorded for ANY of the %d awake session(s) in the last %s (newest sample: %s)",
			awake, tokenTelemetrySilenceThreshold, formatSampleAge(newestSample, now))
		hint = "every awake session going silent together points at the emission path, not at one agent: check the controller's model-usage sweep and the usage sink"
	}
	return warnCheck(c.Name(), message, hint, silent)
}

// scanAwakeSessions returns the number of awake sessions past the grace period
// and a detail line for each that has recorded no sample since cutoff.
//
// Sessions in a terminal state are out of scope: they cannot be expected to emit.
// So are sessions with no awake_started_at (never confirmed a start) and those
// still inside the grace period, which have had no chance to emit yet.
func (c *agentTokenTelemetryCheck) scanAwakeSessions(store beads.Store, lastSample map[string]time.Time, now, cutoff time.Time) (awake int, silent []string) {
	sessions, err := store.List(beads.ListQuery{
		Type:      session.BeadType,
		Label:     session.LabelSession,
		Status:    "open",
		AllowScan: true,
	})
	if err != nil {
		return 0, []string{fmt.Sprintf("listing session beads: %v", err)}
	}
	for _, b := range sessions {
		if b.Metadata == nil || isComputeTerminalState(b.Metadata["state"]) {
			continue
		}
		started, err := time.Parse(time.RFC3339, strings.TrimSpace(b.Metadata["awake_started_at"]))
		if err != nil || started.After(cutoff) {
			continue
		}
		awake++
		if last, ok := lastSample[b.ID]; ok && !last.Before(cutoff) {
			continue
		}
		name := strings.TrimSpace(b.Metadata["session_name"])
		if name == "" {
			name = b.ID
		}
		silent = append(silent, fmt.Sprintf("%s (%s): awake %s, last token sample %s",
			name, b.ID, formatSampleAge(started, now), formatSampleAge(lastSample[b.ID], now)))
	}
	return awake, silent
}

// tokenSampleIndex maps session id to the newest model-usage sample recorded
// for it, and returns the newest sample across all sessions. Only model facts
// count: a compute fact records wall-clock time and is emitted even by a session
// whose token telemetry is entirely dark, so counting it would mask the gap.
func tokenSampleIndex(facts []usage.Fact) (bySession map[string]time.Time, newest time.Time) {
	bySession = make(map[string]time.Time)
	for _, f := range facts {
		if f.Kind != usage.KindModel {
			continue
		}
		id := strings.TrimSpace(f.SessionID)
		at := time.UnixMilli(f.At).UTC()
		if id != "" && at.After(bySession[id]) {
			bySession[id] = at
		}
		if at.After(newest) {
			newest = at
		}
	}
	return bySession, newest
}

// formatSampleAge renders how long ago t was, or "never" for a zero time.
func formatSampleAge(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return fmt.Sprintf("%s ago", now.Sub(t).Round(time.Minute))
}
