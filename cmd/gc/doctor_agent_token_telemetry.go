package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/usage"
	"github.com/gastownhall/gascity/internal/worker"
	workertranscript "github.com/gastownhall/gascity/internal/worker/transcript"
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
// Detection only, and deliberately a warning rather than an error. An awake but
// genuinely idle session — a converse thread nobody has typed into for an hour —
// is silent for a benign reason. Bead state alone cannot tell that apart from a
// working session whose telemetry is not being recorded, so for each silent
// session the check cross-references the live transcript under the session's
// work_dir, resolved independently of the stored session_key. A transcript still
// being written while no fact is recorded is a real emission gap that the finding
// announces; a transcript as quiet as the facts is benign idleness the check
// suppresses. When no unambiguous live transcript resolves — a shared pool
// work_dir, or none on disk — the check falls back to naming the session for an
// operator to judge.
//
// The result is SeverityAdvisory: it is a reading for an operator, never a gate.
// Left blocking, it reported an unactionable finding every hour and desensitized
// its readers to the blocking findings that do need action (gc-w8sxu).
//
// The population is awake sessions that invoke a model. A session with no
// provider resolves no model at all — config.ResolveProvider's start_command
// escape hatch yields a Command and no provider Name — so it can never record a
// token sample however long it runs, and counting it would report a permanent,
// unfixable gap against a process that is behaving correctly. Idleness is not
// the discriminator: an awake, working session that records nothing is still a
// real finding and still fires.
type agentTokenTelemetryCheck struct {
	cityPath string
	newStore func(string) (beads.Store, error)
	// now is injectable so tests can pin the clock against fixture timestamps.
	now func() time.Time
	// resolveLiveTranscript reports how long ago the newest transcript being
	// written under workDir for the provider family was last written, resolved
	// independently of any stored session_key. found is false when no transcript
	// resolves. Injectable so tests can supply fixtures without provider-native
	// transcript files on disk.
	resolveLiveTranscript func(family, workDir string) (modTime time.Time, found bool)
}

// newAgentTokenTelemetryCheck constructs an agentTokenTelemetryCheck.
func newAgentTokenTelemetryCheck(cityPath string, newStore func(string) (beads.Store, error)) *agentTokenTelemetryCheck {
	return &agentTokenTelemetryCheck{
		cityPath:              cityPath,
		newStore:              newStore,
		now:                   time.Now,
		resolveLiveTranscript: liveTranscriptModTime,
	}
}

// liveTranscriptModTime resolves the newest transcript being written under
// workDir for the provider family, independently of any stored session_key, and
// returns its modtime. Bypassing the keyed lookup is the crux: the keyed path
// would read the same stale transcript a session_key-staleness emission bug is
// stuck on, so a session that is actually working would misreport as idle. It is
// the production resolver behind agentTokenTelemetryCheck.resolveLiveTranscript.
func liveTranscriptModTime(family, workDir string) (time.Time, bool) {
	if strings.TrimSpace(workDir) == "" {
		return time.Time{}, false
	}
	path := workertranscript.DiscoverPath(worker.DefaultSearchPaths(), family, workDir, "")
	if path == "" {
		return time.Time{}, false
	}
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, false
	}
	return info.ModTime().UTC(), true
}

// Name returns the check's identifier.
func (c *agentTokenTelemetryCheck) Name() string { return "agent-token-telemetry" }

// advisoryResult marks a result informational, alongside the okCheck/warnCheck
// factories. Every result this check produces goes through here: it is pure
// observability and never gates a consumer.
func advisoryResult(res *doctor.CheckResult) *doctor.CheckResult {
	res.Severity = doctor.SeverityAdvisory
	return res
}

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
		return advisoryResult(okCheck(c.Name(), "no city bead store to inspect"))
	}
	now := c.now().UTC()

	usagePath := filepath.Join(c.cityPath, ".gc", "usage.jsonl")
	facts, _, err := usage.ReadRecentFacts(usagePath, tokenTelemetryReadLimit)
	if err != nil {
		return advisoryResult(warnCheck(c.Name(),
			"could not read the usage log to verify agent token telemetry",
			"fix access to <city>/.gc/usage.jsonl, then rerun gc doctor",
			[]string{fmt.Sprintf("reading %s: %v", usagePath, err)}))
	}
	if len(facts) == 0 {
		// Either usage recording is off ([usage] provider = "discard") or the
		// city has never recorded a fact. Neither is evidence of a telemetry
		// gap, and reporting it would make the check permanently red on a city
		// that opted out of usage accounting.
		return advisoryResult(okCheck(c.Name(), "no usage facts recorded; agent token telemetry not verifiable"))
	}

	store, err := c.newStore(c.cityPath)
	if err != nil {
		return advisoryResult(warnCheck(c.Name(),
			"could not open the city bead store to list awake sessions",
			"fix bead store access, then rerun gc doctor",
			[]string{fmt.Sprintf("opening bead store: %v", err)}))
	}

	cutoff := now.Add(-tokenTelemetrySilenceThreshold)
	lastSampleBySession, newestSample := tokenSampleIndex(facts)

	scan := c.scanAwakeSessions(store, lastSampleBySession, now, cutoff)
	if len(scan.gaps)+len(scan.indeterminate) == 0 {
		if scan.awake == 0 {
			return advisoryResult(okCheck(c.Name(),
				"no awake model-invoking session(s) past the grace period"+unmeasuredSuffix(scan.unmeasured)))
		}
		return advisoryResult(okCheck(c.Name(),
			fmt.Sprintf("all %d awake session(s) past the grace period have recent token samples or are idle%s",
				scan.awake, unmeasuredSuffix(scan.unmeasured))))
	}
	sort.Strings(scan.gaps)
	sort.Strings(scan.indeterminate)
	// Gaps lead: an actionable finding reads before the ones an operator must judge.
	details := append(append([]string{}, scan.gaps...), scan.indeterminate...)

	// Every awake session silent at once is a different diagnosis from one
	// silent session among many: it points at the emission path having stopped
	// rather than at a single agent, which is what a shared cutoff timestamp
	// across many agents looks like.
	fleetWide := len(details) == scan.awake && scan.awake > 1
	message := fmt.Sprintf("%d of %d awake session(s) have recorded no token samples in the last %s",
		len(details), scan.awake, tokenTelemetrySilenceThreshold)
	var hint string
	switch {
	case fleetWide:
		message = fmt.Sprintf("no token samples recorded for ANY of the %d awake session(s) in the last %s (newest sample: %s)",
			scan.awake, tokenTelemetrySilenceThreshold, formatSampleAge(newestSample, now))
		hint = "every awake session going silent together points at the emission path, not at one agent: check the controller's model-usage sweep and the usage sink"
	case len(scan.gaps) > 0:
		hint = "the named session(s) are still writing transcripts while nothing is recorded for them: the model-usage sweep is reading a stale transcript. Investigate the emission path, not the agents."
	default:
		hint = "confirm the session is genuinely idle; if it is working, its invocation telemetry is not being recorded"
	}
	return advisoryResult(warnCheck(c.Name(), message+unmeasuredSuffix(scan.unmeasured), hint, details))
}

// awakeSilenceScan is the outcome of scanning awake sessions for silence. gaps
// and indeterminate are both reported; a session classified idle is dropped
// because its silence is benign.
type awakeSilenceScan struct {
	// awake is the number of awake model-invoking sessions past the grace period.
	awake int
	// unmeasured is the number of awake sessions excluded because they invoke no
	// model and can never record a token sample.
	unmeasured int
	// gaps names each silent session whose live transcript is still being written:
	// a real emission gap an operator should act on.
	gaps []string
	// indeterminate names each silent session with no unambiguous live transcript
	// to judge by — a shared pool work_dir, or none on disk — left for an operator.
	indeterminate []string
}

// scanAwakeSessions classifies every awake model-invoking session past the grace
// period that has recorded no sample since cutoff.
//
// A silent session is not automatically a finding: bead state cannot tell a
// working-but-unrecorded session from a benignly idle one. The scan resolves each
// silent session's live transcript under its work_dir, independently of the
// stored session_key (see liveTranscriptModTime), and classifies by whether that
// transcript is still being written inside the silence window:
//   - A transcript written since the cutoff, while no fact landed in the window,
//     is a real emission gap and goes in gaps.
//   - A transcript as quiet as the facts is genuinely idle, and the session is
//     dropped as benign.
//   - A work_dir shared by another live model session, or one with no transcript
//     on disk, is indeterminate: it is reported with today's advisory wording
//     rather than guessed.
//
// Sessions in a terminal state are out of scope: they cannot be expected to emit.
// So are sessions with no awake_started_at (never confirmed a start) and those
// still inside the grace period, which have had no chance to emit yet.
//
// Sessions that invoke no model are out of scope for a stronger reason: they are
// not agents at all. session.ProviderFamilyFromMetadata is the canonical
// provider ladder (builtin_ancestor → provider_kind → provider), and it resolves
// to "" only when a session records no provider on any rung — the on-disk shape
// of a start_command process such as the control dispatcher's `gc convoy control
// --serve --follow` loop. Reading the ladder rather than the raw provider keeps a
// wrapped provider alias in the measured population; deriving the exclusion from
// the provider (not from a session name or template) keeps role names out of Go.
func (c *agentTokenTelemetryCheck) scanAwakeSessions(store beads.Store, lastSample map[string]time.Time, now, cutoff time.Time) awakeSilenceScan {
	sessions, err := store.List(beads.ListQuery{
		Type:      session.BeadType,
		Label:     session.LabelSession,
		Status:    "open",
		AllowScan: true,
	})
	if err != nil {
		return awakeSilenceScan{indeterminate: []string{fmt.Sprintf("listing session beads: %v", err)}}
	}

	// Count non-terminal model-invoking sessions per work_dir. A work_dir shared
	// by two of them holds two live transcripts that keyless, newest-wins
	// discovery cannot tell apart, so a silent session sharing one is left
	// indeterminate rather than matched to a guessed transcript.
	liveByWorkDir := map[string]int{}
	for _, b := range sessions {
		if b.Metadata == nil || isComputeTerminalState(b.Metadata["state"]) {
			continue
		}
		if session.ProviderFamilyFromMetadata(b.Metadata, "") == "" {
			continue
		}
		if wd := contract.WorkerDirFromMetadata(b.Metadata); wd != "" {
			liveByWorkDir[wd]++
		}
	}

	var scan awakeSilenceScan
	for _, b := range sessions {
		if b.Metadata == nil || isComputeTerminalState(b.Metadata["state"]) {
			continue
		}
		started, err := time.Parse(time.RFC3339, strings.TrimSpace(b.Metadata["awake_started_at"]))
		if err != nil || started.After(cutoff) {
			continue
		}
		family := session.ProviderFamilyFromMetadata(b.Metadata, "")
		if family == "" {
			scan.unmeasured++
			continue
		}
		scan.awake++
		if last, ok := lastSample[b.ID]; ok && !last.Before(cutoff) {
			continue
		}
		name := strings.TrimSpace(b.Metadata["session_name"])
		if name == "" {
			name = b.ID
		}
		detail := fmt.Sprintf("%s (%s): awake %s, last token sample %s",
			name, b.ID, formatSampleAge(started, now), formatSampleAge(lastSample[b.ID], now))

		workDir := contract.WorkerDirFromMetadata(b.Metadata)
		if workDir == "" || liveByWorkDir[workDir] > 1 {
			scan.indeterminate = append(scan.indeterminate, detail)
			continue
		}
		modTime, found := c.resolveLiveTranscript(family, workDir)
		if !found {
			scan.indeterminate = append(scan.indeterminate, detail)
			continue
		}
		if modTime.After(cutoff) {
			// The live transcript is being written inside the silence window while
			// no fact landed in it: the session is working and its telemetry is not
			// being recorded. Announce it so the finding needs no converse sitting.
			scan.gaps = append(scan.gaps, detail+fmt.Sprintf(", live transcript written %s but no usage recorded",
				formatSampleAge(modTime, now)))
			continue
		}
		// The live transcript is as quiet as the facts: genuinely idle. Its silence
		// is benign, so it is dropped rather than re-escalated every hour.
	}
	return scan
}

// unmeasuredSuffix states how many awake sessions were left out of the
// population. The exclusion is reported rather than applied silently: this
// check exists because an absent telemetry series is indistinguishable from
// zero spend (gc-kawr5), and an unreported exclusion would rebuild that same
// blind spot one level up.
func unmeasuredSuffix(unmeasured int) string {
	if unmeasured == 0 {
		return ""
	}
	return fmt.Sprintf(" (%d awake session(s) invoke no model and are not measured)", unmeasured)
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
