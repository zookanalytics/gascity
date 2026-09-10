package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/sessionlog"
	"github.com/gastownhall/gascity/internal/usage"
)

// awakeSessionBead seeds a live model-invoking session bead awake since the
// given time. The provider is set because every session that actually runs a
// model carries one: config.ResolveProvider fails an agent that names no
// provider, so a blank provider never reaches a real LLM session bead.
func awakeSessionBead(t *testing.T, store beads.Store, name string, awakeSince time.Time) beads.Bead {
	t.Helper()
	return awakeSessionBeadWithProvider(t, store, name, awakeSince, "claude")
}

// awakeSessionBeadWithProvider seeds a live session bead with an explicit
// provider. A blank provider is the on-disk shape of a session that invokes no
// model at all: config.ResolveProvider's start_command escape hatch returns a
// ResolvedProvider with a Command and no provider Name, so the session bead is
// written with none.
func awakeSessionBeadWithProvider(t *testing.T, store beads.Store, name string, awakeSince time.Time, provider string) beads.Bead {
	t.Helper()
	meta := map[string]string{
		"state":            "active",
		"session_name":     name,
		"awake_started_at": awakeSince.UTC().Format(time.RFC3339),
	}
	if provider != "" {
		meta["provider"] = provider
	}
	b, err := store.Create(beads.Bead{
		Type:     session.BeadType,
		Status:   "open",
		Title:    name,
		Labels:   []string{session.LabelSession},
		Metadata: meta,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// writeUsageFacts writes model usage facts to a LocalSink JSONL file.
func writeUsageFacts(t *testing.T, path string, facts []usage.Fact) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	sink := usage.NewLocalSink(path)
	for _, f := range facts {
		if err := sink.Record(t.Context(), f); err != nil {
			t.Fatal(err)
		}
	}
}

func runTokenTelemetryCheck(t *testing.T, cityPath string, store beads.Store, now time.Time) *doctor.CheckResult {
	t.Helper()
	c := newAgentTokenTelemetryCheck(cityPath, func(string) (beads.Store, error) { return store, nil }, nil)
	c.now = func() time.Time { return now }
	return c.Run(nil)
}

// runTokenTelemetryCheckWithSearchPaths runs the check on its production resolver
// with the given transcript search roots, so a test can exercise discovery,
// history loading, and the newest-usage-bearing-invocation read end to end against
// a real transcript file on disk rather than an injected timestamp.
func runTokenTelemetryCheckWithSearchPaths(t *testing.T, cityPath string, store beads.Store, now time.Time, searchPaths []string) *doctor.CheckResult {
	t.Helper()
	c := newAgentTokenTelemetryCheck(cityPath, func(string) (beads.Store, error) { return store, nil }, searchPaths)
	c.now = func() time.Time { return now }
	return c.Run(nil)
}

// TestAgentTokenTelemetryFlagsSilentAwakeSession is the tripwire the blind spot
// itself asked for: an agent that is awake and long past the silence threshold
// with no token samples reads as $0.00 spend rather than as unmeasured, so the
// check must name it instead of letting the metric answer confidently and
// wrongly (gc-kawr5).
func TestAgentTokenTelemetryFlagsSilentAwakeSession(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	loud := awakeSessionBead(t, store, "rig--polecat", now.Add(-4*time.Hour))
	silent := awakeSessionBead(t, store, "rig--refinery", now.Add(-4*time.Hour))

	// Only the polecat has recent samples; the refinery has none at all.
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: loud.ID, Worker: "rig--polecat", At: now.Add(-5 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%q", res.Status, res.Message)
	}
	joined := strings.Join(res.Details, "\n")
	if !strings.Contains(joined, silent.ID) && !strings.Contains(joined, "rig--refinery") {
		t.Errorf("silent session not named in details:\n%s", joined)
	}
	if strings.Contains(joined, "rig--polecat") {
		t.Errorf("session with recent samples must not be flagged:\n%s", joined)
	}
}

// TestAgentTokenTelemetryIgnoresRecentlyWokenSession pins the grace period: a
// session that just woke has had no chance to emit a sample yet, and flagging
// it would make the check noisy on every restart.
func TestAgentTokenTelemetryIgnoresRecentlyWokenSession(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	awakeSessionBead(t, store, "rig--just-woke", now.Add(-2*time.Minute))
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: "other", At: now.Add(-time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok (session is inside the grace period); details=%v", res.Status, res.Details)
	}
}

// TestAgentTokenTelemetryIgnoresTerminalSessions pins the scope: only sessions
// that are actually awake can be expected to emit samples.
func TestAgentTokenTelemetryIgnoresTerminalSessions(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	b := awakeSessionBead(t, store, "rig--asleep", now.Add(-8*time.Hour))
	if err := store.SetMetadata(b.ID, "state", "asleep"); err != nil {
		t.Fatal(err)
	}
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: "other", At: now.Add(-time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok (asleep sessions are out of scope); details=%v", res.Status, res.Details)
	}
}

// TestAgentTokenTelemetrySkipsWhenNoUsageLogExists keeps the check quiet on a
// city that records no usage facts at all: there is nothing to read, so a
// finding would be an artifact of configuration rather than evidence of a gap.
func TestAgentTokenTelemetrySkipsWhenNoUsageLogExists(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	awakeSessionBead(t, store, "rig--refinery", now.Add(-8*time.Hour))

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok when no usage log exists; details=%v", res.Status, res.Details)
	}
}

// TestAgentTokenTelemetryReportsWholePipelineDark distinguishes the two shapes
// this check can see. One silent session among many is a per-session gap; every
// awake session silent while the log holds only older records is the emission
// path itself having stopped — the 15-agents-share-one-cutoff-timestamp shape
// that motivated the check.
func TestAgentTokenTelemetryReportsWholePipelineDark(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	awakeSessionBead(t, store, "rig--refinery", now.Add(-8*time.Hour))
	awakeSessionBead(t, store, "rig--witness", now.Add(-8*time.Hour))
	awakeSessionBead(t, store, "rig--deacon", now.Add(-8*time.Hour))

	// The log exists but every record predates the threshold.
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: "old", At: now.Add(-9 * time.Hour).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%q", res.Status, res.Message)
	}
	if !strings.Contains(res.Message, "no token samples") {
		t.Errorf("message should name the fleet-wide silence, got %q", res.Message)
	}
}

// TestAgentTokenTelemetryIgnoresNonModelSessions pins the population the check
// measures. A session that invokes no model — the `gc convoy control --serve
// --follow` control loop is the standing example — is a Go process with no
// provider, so it can never record a token sample no matter how long it runs.
// Counting it as a session that owes one made the check report a permanent,
// unfixable finding against processes that were behaving correctly (gc-w8sxu).
func TestAgentTokenTelemetryIgnoresNonModelSessions(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	healthy := awakeSessionBead(t, store, "rig--polecat", now.Add(-4*time.Hour))
	serve := awakeSessionBeadWithProvider(t, store, "core--control-dispatcher", now.Add(-4*time.Hour), "")

	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: healthy.ID, Worker: "rig--polecat", At: now.Add(-5 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok (the only silent session invokes no model); message=%q details=%v",
			res.Status, res.Message, res.Details)
	}
	joined := strings.Join(res.Details, "\n") + "\n" + res.Message
	if strings.Contains(joined, serve.ID) || strings.Contains(joined, "core--control-dispatcher") {
		t.Errorf("non-model session must not be reported as owing a sample:\n%s", joined)
	}
}

// TestAgentTokenTelemetryKeepsWatchingModelSessionsAmongNonModelOnes is the
// other half of the population rule: excluding the control loops must not
// excuse a genuine agent session. An awake session that runs a model and has
// recorded nothing is still exactly the finding this check exists to make.
func TestAgentTokenTelemetryKeepsWatchingModelSessionsAmongNonModelOnes(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	silent := awakeSessionBead(t, store, "rig--refinery", now.Add(-4*time.Hour))
	awakeSessionBeadWithProvider(t, store, "core--control-dispatcher", now.Add(-4*time.Hour), "")

	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: "other", At: now.Add(-time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning (a model session recorded nothing); message=%q", res.Status, res.Message)
	}
	joined := strings.Join(res.Details, "\n")
	if !strings.Contains(joined, silent.ID) && !strings.Contains(joined, "rig--refinery") {
		t.Errorf("silent model session not named in details:\n%s", joined)
	}
	// The denominator counts model sessions only, so one silent out of one
	// watched session is fleet-wide — the control loop must not dilute it.
	if !strings.Contains(res.Message, "1 of 1") && !strings.Contains(res.Message, "ANY of the 1") {
		t.Errorf("denominator should count only model-invoking sessions, got %q", res.Message)
	}
}

// TestAgentTokenTelemetryAccountsForExcludedSessions keeps the exclusion
// visible. This check exists because a missing telemetry series is
// indistinguishable from zero spend (gc-kawr5); an exclusion that reported
// nothing would reintroduce exactly that blind spot one level up, so the
// number of sessions dropped from the population is stated rather than
// silently applied.
func TestAgentTokenTelemetryAccountsForExcludedSessions(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	healthy := awakeSessionBead(t, store, "rig--polecat", now.Add(-4*time.Hour))
	awakeSessionBeadWithProvider(t, store, "core--control-dispatcher", now.Add(-4*time.Hour), "")
	awakeSessionBeadWithProvider(t, store, "other--control-dispatcher", now.Add(-4*time.Hour), "")

	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: healthy.ID, Worker: "rig--polecat", At: now.Add(-5 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok; message=%q", res.Status, res.Message)
	}
	if !strings.Contains(res.Message, "2") || !strings.Contains(res.Message, "no model") {
		t.Errorf("message should account for the 2 excluded non-model sessions, got %q", res.Message)
	}
}

// TestAgentTokenTelemetryIsAdvisory pins the severity. The check's own finding
// cannot separate a real telemetry gap from an awake-but-idle agent — an
// on_demand agent parked between wakes is silent for a benign reason — so the
// result is for an operator to read, not a gate for automation to trip on.
// Left blocking, it desensitized the deacon to genuinely blocking findings
// (gc-w8sxu).
func TestAgentTokenTelemetryIsAdvisory(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	awakeSessionBead(t, store, "rig--refinery", now.Add(-8*time.Hour))
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: "other", At: now.Add(-time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	res := runTokenTelemetryCheck(t, cityPath, store, now)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%q", res.Status, res.Message)
	}
	if res.Severity != doctor.SeverityAdvisory {
		t.Errorf("severity = %v, want advisory", res.Severity)
	}
}

// runTokenTelemetryCheckWithResolver runs the check with an injected live-transcript
// resolver so a test can pin the newest usage-bearing invocation time under a
// work_dir without writing provider-native transcript files to disk.
func runTokenTelemetryCheckWithResolver(t *testing.T, cityPath string, store beads.Store, now time.Time, resolver func(family, workDir string) (time.Time, bool)) *doctor.CheckResult {
	t.Helper()
	c := newAgentTokenTelemetryCheck(cityPath, func(string) (beads.Store, error) { return store, nil }, nil)
	c.now = func() time.Time { return now }
	if resolver != nil {
		c.resolveLiveTranscript = resolver
	}
	return c.Run(nil)
}

// TestAgentTokenTelemetryFlagsWorkingSessionWithLiveTranscript is the discriminator
// the escalation-noise fix asks for: a silent session whose live transcript records
// a usage-bearing model invocation inside the silence window is working but not
// being recorded, not idle. Resolving the transcript independently of the stored
// session_key finds the live one the stale keyed lookup misses, so the check
// announces the gap instead of handing an operator a converse sitting to confirm
// idleness (gc-1fke8).
func TestAgentTokenTelemetryFlagsWorkingSessionWithLiveTranscript(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	loud := awakeSessionBead(t, store, "rig--polecat", now.Add(-4*time.Hour))
	working := awakeSessionBead(t, store, "rig--refinery", now.Add(-4*time.Hour))
	if err := store.SetMetadata(working.ID, "work_dir", "/w/refinery"); err != nil {
		t.Fatal(err)
	}

	// The polecat has a recent sample; the refinery's newest fact is 90m old, past
	// the one-hour silence threshold, so only the refinery is a silence candidate.
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: loud.ID, Worker: "rig--polecat", At: now.Add(-5 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
		{Kind: usage.KindModel, SessionID: working.ID, Worker: "rig--refinery", At: now.Add(-90 * time.Minute).UnixMilli(), IdempotencyKey: "k2"},
	})

	// The refinery's newest usage-bearing model invocation was 5m ago — inside the
	// silence window — yet nothing was recorded in that window: a real emission gap.
	resolver := func(_, workDir string) (time.Time, bool) {
		if workDir == "/w/refinery" {
			return now.Add(-5 * time.Minute), true
		}
		return time.Time{}, false
	}

	res := runTokenTelemetryCheckWithResolver(t, cityPath, store, now, resolver)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%q", res.Status, res.Message)
	}
	joined := strings.Join(res.Details, "\n")
	if !strings.Contains(joined, working.ID) && !strings.Contains(joined, "rig--refinery") {
		t.Errorf("gap session not named in details:\n%s", joined)
	}
	if !strings.Contains(joined, "transcript shows model usage") {
		t.Errorf("the finding must self-announce the unrecorded usage, got:\n%s", joined)
	}
	if !strings.Contains(res.FixHint, "emission path") {
		t.Errorf("a confirmed gap must point at the emission path, got hint %q", res.FixHint)
	}
	if strings.Contains(res.FixHint, "confirm the session is genuinely idle") {
		t.Errorf("a confirmed gap must not carry the idle-confirmation hint: %q", res.FixHint)
	}
}

// TestAgentTokenTelemetryClassifiesQuietTranscriptAsIdle is the noise-removal half:
// a silent session whose live transcript is as quiet as its last recorded fact is
// genuinely idle, so the check suppresses it instead of re-escalating benign idle
// every hour — the re-escalation tk-jnrm6i documented (gc-1fke8).
func TestAgentTokenTelemetryClassifiesQuietTranscriptAsIdle(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	idle := awakeSessionBead(t, store, "rig--converse", now.Add(-4*time.Hour))
	if err := store.SetMetadata(idle.ID, "work_dir", "/w/converse"); err != nil {
		t.Fatal(err)
	}
	lastFact := now.Add(-90 * time.Minute)
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: idle.ID, Worker: "rig--converse", At: lastFact.UnixMilli(), IdempotencyKey: "k1"},
	})

	// The transcript's newest usage-bearing invocation matches the newest recorded
	// fact: nothing has happened since, so the session is genuinely idle, not a gap.
	resolver := func(_, workDir string) (time.Time, bool) {
		if workDir == "/w/converse" {
			return lastFact, true
		}
		return time.Time{}, false
	}

	res := runTokenTelemetryCheckWithResolver(t, cityPath, store, now, resolver)
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok (idle session, benign silence); message=%q details=%v",
			res.Status, res.Message, res.Details)
	}
}

// TestAgentTokenTelemetrySharedWorkdirFallsBackToAdvisory pins the third case: when
// two live model sessions share a pool work_dir, keyless newest-wins discovery
// cannot attribute a transcript, so a silent session among them keeps today's
// advisory wording rather than being matched to a guessed transcript (gc-1fke8).
func TestAgentTokenTelemetrySharedWorkdirFallsBackToAdvisory(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	silent := awakeSessionBead(t, store, "pool--polecat-1", now.Add(-4*time.Hour))
	busy := awakeSessionBead(t, store, "pool--polecat-2", now.Add(-4*time.Hour))
	for _, id := range []string{silent.ID, busy.ID} {
		if err := store.SetMetadata(id, "work_dir", "/w/pool"); err != nil {
			t.Fatal(err)
		}
	}
	// Only the busy session has a recent sample; the other is silent but shares
	// the directory, so the check cannot resolve its transcript.
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: busy.ID, Worker: "pool--polecat-2", At: now.Add(-5 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	resolver := func(_, workDir string) (time.Time, bool) {
		t.Fatalf("resolver must not be called for a shared work_dir (%s)", workDir)
		return time.Time{}, false
	}

	res := runTokenTelemetryCheckWithResolver(t, cityPath, store, now, resolver)
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning (silent session reported as advisory); message=%q", res.Status, res.Message)
	}
	joined := strings.Join(res.Details, "\n")
	if !strings.Contains(joined, silent.ID) && !strings.Contains(joined, "pool--polecat-1") {
		t.Errorf("silent shared-workdir session not named in details:\n%s", joined)
	}
	if !strings.Contains(res.FixHint, "confirm the session is genuinely idle") {
		t.Errorf("a shared work_dir must fall back to the advisory wording, got hint %q", res.FixHint)
	}
}

// writeClaudeTranscriptForWorkDir writes a claude JSONL transcript discoverable
// under searchRoot for workDir, using the same ProjectSlug layout Claude Code
// writes on disk, so the production resolver reads a real transcript file rather
// than an injected timestamp.
func writeClaudeTranscriptForWorkDir(t *testing.T, searchRoot, workDir string, lines []string) {
	t.Helper()
	slugDir := filepath.Join(searchRoot, sessionlog.ProjectSlug(workDir))
	if err := os.MkdirAll(slugDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(slugDir, "session.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// claudeAssistantUsageLine builds a claude assistant transcript entry that carries
// token usage, timestamped at at.
func claudeAssistantUsageLine(uuid, parent string, at time.Time) string {
	return fmt.Sprintf(`{"uuid":%q,"parentUuid":%q,"type":"assistant","message":{"role":"assistant","content":"done","model":"claude-sonnet","stop_reason":"end_turn","usage":{"input_tokens":1200,"output_tokens":40}},"timestamp":%q,"sessionId":"provider-claude"}`,
		uuid, parent, at.UTC().Format(time.RFC3339))
}

// claudeUserLine builds a claude user transcript entry: a non-usage write that
// bumps the transcript file's modtime without a model being invoked.
func claudeUserLine(uuid, parent string, at time.Time) string {
	return fmt.Sprintf(`{"uuid":%q,"parentUuid":%q,"type":"user","message":{"role":"user","content":"one more thing"},"timestamp":%q,"sessionId":"provider-claude"}`,
		uuid, parent, at.UTC().Format(time.RFC3339))
}

// TestAgentTokenTelemetryReadsUsageBearingInvocationNotFileModtime is the
// regression the pre-open review asked for: the discriminator must be the newest
// usage-bearing model invocation in the live transcript, not the transcript file's
// modtime. A non-usage write — a user message, a tool result, a reasoning record —
// bumps the file without a model being invoked, so a modtime read would report a
// genuinely idle session as a working one the moment its transcript is appended
// to. Here the newest usage-bearing turn is 90m old (as old as the last recorded
// fact) while a user message landed 5m ago; the session is idle and must NOT be
// reported as an emission gap (gc-1fke8).
func TestAgentTokenTelemetryReadsUsageBearingInvocationNotFileModtime(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	searchRoot := t.TempDir()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	workDir := filepath.Join(t.TempDir(), "idle-project")
	idle := awakeSessionBead(t, store, "rig--converse", now.Add(-4*time.Hour))
	if err := store.SetMetadata(idle.ID, "work_dir", workDir); err != nil {
		t.Fatal(err)
	}

	lastFact := now.Add(-90 * time.Minute)
	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: idle.ID, Worker: "rig--converse", At: lastFact.UnixMilli(), IdempotencyKey: "k1"},
	})

	// Newest usage-bearing turn is 90m old (matches the fact); a non-usage user
	// message landed 5m ago and only bumps the file's modtime.
	writeClaudeTranscriptForWorkDir(t, searchRoot, workDir, []string{
		claudeUserLine("u1", "", now.Add(-95*time.Minute)),
		claudeAssistantUsageLine("a1", "u1", lastFact),
		claudeUserLine("u9", "a1", now.Add(-5*time.Minute)),
	})

	res := runTokenTelemetryCheckWithSearchPaths(t, cityPath, store, now, []string{searchRoot})
	if res.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok (idle: newest usage-bearing turn predates the cutoff); message=%q details=%v",
			res.Status, res.Message, res.Details)
	}
	joined := strings.Join(res.Details, "\n")
	if strings.Contains(joined, "transcript shows model usage") {
		t.Errorf("a recent non-usage write must not be reported as an emission gap:\n%s", joined)
	}
}

// TestAgentTokenTelemetryResolvesTranscriptUnderConfiguredObservePath pins the
// second review finding: the check must search the city's configured observe
// paths, not only the built-in defaults. The transcript here lives ONLY under a
// custom search root, and its newest usage-bearing turn is 5m old while the last
// recorded fact is 90m old — a real emission gap. The check can find it, and so
// classify it as a gap, only when the configured search root is threaded in; on
// bare defaults it would miss the transcript entirely and fall back to the
// idle-confirmation advisory (gc-1fke8).
func TestAgentTokenTelemetryResolvesTranscriptUnderConfiguredObservePath(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	searchRoot := t.TempDir()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	workDir := filepath.Join(t.TempDir(), "working-project")
	working := awakeSessionBead(t, store, "rig--refinery", now.Add(-4*time.Hour))
	if err := store.SetMetadata(working.ID, "work_dir", workDir); err != nil {
		t.Fatal(err)
	}

	writeUsageFacts(t, filepath.Join(cityPath, ".gc", "usage.jsonl"), []usage.Fact{
		{Kind: usage.KindModel, SessionID: working.ID, Worker: "rig--refinery", At: now.Add(-90 * time.Minute).UnixMilli(), IdempotencyKey: "k1"},
	})

	// The newest usage-bearing turn is 5m old — inside the silence window — and the
	// transcript lives only under the custom search root.
	writeClaudeTranscriptForWorkDir(t, searchRoot, workDir, []string{
		claudeUserLine("u1", "", now.Add(-10*time.Minute)),
		claudeAssistantUsageLine("a1", "u1", now.Add(-5*time.Minute)),
	})

	res := runTokenTelemetryCheckWithSearchPaths(t, cityPath, store, now, []string{searchRoot})
	if res.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning (gap found under the configured search root); message=%q details=%v",
			res.Status, res.Message, res.Details)
	}
	joined := strings.Join(res.Details, "\n")
	if !strings.Contains(joined, "transcript shows model usage") {
		t.Errorf("gap under the configured search root must self-announce the unrecorded usage, got:\n%s", joined)
	}
	if !strings.Contains(res.FixHint, "emission path") {
		t.Errorf("a confirmed gap must point at the emission path, got hint %q", res.FixHint)
	}
}
