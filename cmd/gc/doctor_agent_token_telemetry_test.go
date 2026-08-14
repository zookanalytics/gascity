package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
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
	c := newAgentTokenTelemetryCheck(cityPath, func(string) (beads.Store, error) { return store, nil })
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
