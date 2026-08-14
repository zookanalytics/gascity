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

// awakeSessionBead seeds a live session bead awake since the given time.
func awakeSessionBead(t *testing.T, store beads.Store, name string, awakeSince time.Time) beads.Bead {
	t.Helper()
	b, err := store.Create(beads.Bead{
		Type:   session.BeadType,
		Status: "open",
		Title:  name,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"state":            "active",
			"session_name":     name,
			"awake_started_at": awakeSince.UTC().Format(time.RFC3339),
		},
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
