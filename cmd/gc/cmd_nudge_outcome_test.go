package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// A caller that queues a nudge and later reasons from silence needs a handle on
// the nudge it queued; without one, "delivered" and "dropped" are the same
// observation.
func TestQueueSessionNudgeJSONReportsNudgeID(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()
	store := beads.NewMemStore()
	fake := runtime.NewFake()
	if err := fake.Start(context.Background(), "sess-worker", runtime.Config{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	target := nudgeTarget{
		cityPath:    dir,
		agent:       config.Agent{Name: "worker"},
		resolved:    &config.ResolvedProvider{Name: "codex"},
		sessionName: "sess-worker",
	}

	var stdout, stderr bytes.Buffer
	code := deliverSessionNudgeWithWorker(target, store, fake, "check your hook", nudgeDeliveryQueue, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("deliverSessionNudgeWithWorker = %d, want 0; stderr: %s", code, stderr.String())
	}

	var got sessionNudgeJSON
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", stdout.String(), err)
	}
	if got.Outcome != "queued" || !got.Queued {
		t.Fatalf("outcome/queued = %q/%v, want queued/true", got.Outcome, got.Queued)
	}
	if got.NudgeID == "" {
		t.Fatal("nudge_id = \"\", want the queued nudge id so the caller can resolve its fate")
	}

	pending, _, _, err := listQueuedNudges(dir, "worker", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if got.NudgeID != pending[0].ID {
		t.Fatalf("nudge_id = %q, want the queued item id %q", got.NudgeID, pending[0].ID)
	}
}

func TestQueueSessionNudgeTextReportsNudgeID(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()
	store := beads.NewMemStore()
	fake := runtime.NewFake()
	if err := fake.Start(context.Background(), "sess-worker", runtime.Config{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	target := nudgeTarget{
		cityPath:    dir,
		agent:       config.Agent{Name: "worker"},
		resolved:    &config.ResolvedProvider{Name: "codex"},
		sessionName: "sess-worker",
	}

	var stdout, stderr bytes.Buffer
	if code := deliverSessionNudgeWithWorker(target, store, fake, "check your hook", nudgeDeliveryQueue, false, &stdout, &stderr); code != 0 {
		t.Fatalf("deliverSessionNudgeWithWorker = %d, want 0; stderr: %s", code, stderr.String())
	}

	pending, _, _, err := listQueuedNudges(dir, "worker", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if !strings.Contains(stdout.String(), "Queued nudge for worker") {
		t.Fatalf("stdout = %q, want the queued confirmation", stdout.String())
	}
	if !strings.Contains(stdout.String(), pending[0].ID) {
		t.Fatalf("stdout = %q, want the queued nudge id %q", stdout.String(), pending[0].ID)
	}
}

func TestResolveQueuedNudgeOutcome_PendingInFlightAndDropped(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()
	now := time.Now().Add(-time.Minute)

	pending := newQueuedNudgeWithOptions("worker", "still queued", "session", now, queuedNudgeOptions{ID: "n-pending"})
	claimed := newQueuedNudgeWithOptions("worker", "being delivered", "session", now, queuedNudgeOptions{ID: "n-claimed"})
	dropped := newQueuedNudgeWithOptions("worker", "fenced out", nudgeSourceWait, now, queuedNudgeOptions{ID: "n-dropped"})
	for _, item := range []queuedNudge{pending, claimed, dropped} {
		if err := enqueueQueuedNudge(dir, item); err != nil {
			t.Fatalf("enqueueQueuedNudge(%s): %v", item.ID, err)
		}
	}

	if _, err := claimDueQueuedNudgesMatching(dir, time.Now(), func(item queuedNudge) bool {
		return item.ID == claimed.ID
	}); err != nil {
		t.Fatalf("claimDueQueuedNudgesMatching: %v", err)
	}
	if err := recordQueuedNudgeFailure(dir, []string{dropped.ID}, errNudgeSessionFenceMismatch, time.Now()); err != nil {
		t.Fatalf("recordQueuedNudgeFailure: %v", err)
	}

	cases := []struct {
		id         string
		wantFound  bool
		wantResult string
		wantState  string
		wantReason string
	}{
		{id: "n-pending", wantFound: true, wantResult: nudgeOutcomePending, wantState: "pending"},
		{id: "n-claimed", wantFound: true, wantResult: nudgeOutcomeInFlight, wantState: "in_flight"},
		{id: "n-dropped", wantFound: true, wantResult: nudgeOutcomeDropped, wantState: "dead", wantReason: errNudgeSessionFenceMismatch.Error()},
		{id: "n-missing", wantFound: false},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			report, found, err := resolveQueuedNudgeOutcome(dir, tc.id)
			if err != nil {
				t.Fatalf("resolveQueuedNudgeOutcome: %v", err)
			}
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}
			if !tc.wantFound {
				return
			}
			if report.Outcome != tc.wantResult {
				t.Fatalf("outcome = %q, want %q", report.Outcome, tc.wantResult)
			}
			if report.State != tc.wantState {
				t.Fatalf("state = %q, want %q", report.State, tc.wantState)
			}
			if tc.wantReason != "" && !strings.Contains(report.Reason, tc.wantReason) {
				t.Fatalf("reason = %q, want it to contain %q", report.Reason, tc.wantReason)
			}
			if report.NudgeID != tc.id {
				t.Fatalf("nudge_id = %q, want %q", report.NudgeID, tc.id)
			}
		})
	}
}

// Once a nudge is delivered it leaves the queue entirely, so the durable shadow
// bead is the only remaining record — and it is the record that separates a
// delivered nudge from a dropped one.
func TestResolveQueuedNudgeOutcome_DeliveredFromShadowBead(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()

	item := newQueuedNudgeWithOptions("worker", "delivered reminder", "session", time.Now().Add(-time.Minute), queuedNudgeOptions{ID: "n-delivered"})
	if err := enqueueQueuedNudge(dir, item); err != nil {
		t.Fatalf("enqueueQueuedNudge: %v", err)
	}
	if _, err := claimDueQueuedNudgesMatching(dir, time.Now(), func(candidate queuedNudge) bool {
		return candidate.ID == item.ID
	}); err != nil {
		t.Fatalf("claimDueQueuedNudgesMatching: %v", err)
	}
	if err := ackQueuedNudges(dir, []string{item.ID}); err != nil {
		t.Fatalf("ackQueuedNudges: %v", err)
	}

	state, err := loadNudgeQueueState(dir)
	if err != nil {
		t.Fatalf("loadNudgeQueueState: %v", err)
	}
	if queuedNudgeExists(&state, item.ID) {
		t.Fatal("delivered nudge is still in the queue, want it gone from state.json")
	}

	report, found, err := resolveQueuedNudgeOutcome(dir, item.ID)
	if err != nil {
		t.Fatalf("resolveQueuedNudgeOutcome: %v", err)
	}
	if !found {
		t.Fatal("found = false, want the delivered nudge resolved from its shadow bead")
	}
	if report.Outcome != nudgeOutcomeDelivered {
		t.Fatalf("outcome = %q, want %q", report.Outcome, nudgeOutcomeDelivered)
	}
	if report.State != "injected" {
		t.Fatalf("state = %q, want injected", report.State)
	}
}

func TestResolveQueuedNudgeOutcome_DroppedFromTerminalShadowBead(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()

	item := newQueuedNudgeWithOptions("worker", "withdrawn reminder", nudgeSourceWait, time.Now().Add(-time.Minute), queuedNudgeOptions{ID: "n-withdrawn"})
	if err := enqueueQueuedNudge(dir, item); err != nil {
		t.Fatalf("enqueueQueuedNudge: %v", err)
	}
	if _, err := claimDueQueuedNudgesMatching(dir, time.Now(), func(candidate queuedNudge) bool {
		return candidate.ID == item.ID
	}); err != nil {
		t.Fatalf("claimDueQueuedNudgesMatching: %v", err)
	}
	if err := ackQueuedNudgesWithOutcome(dir, []string{item.ID}, "failed", "wait-canceled", "delivery-withdrawn"); err != nil {
		t.Fatalf("ackQueuedNudgesWithOutcome: %v", err)
	}

	report, found, err := resolveQueuedNudgeOutcome(dir, item.ID)
	if err != nil {
		t.Fatalf("resolveQueuedNudgeOutcome: %v", err)
	}
	if !found {
		t.Fatal("found = false, want the withdrawn nudge resolved from its shadow bead")
	}
	if report.Outcome != nudgeOutcomeDropped {
		t.Fatalf("outcome = %q, want %q", report.Outcome, nudgeOutcomeDropped)
	}
	if !strings.Contains(report.Reason, "wait-canceled") {
		t.Fatalf("reason = %q, want it to name the withdrawal reason", report.Reason)
	}
}

// The --json flag is only honored for commands that ship a result schema: the
// contract gate rejects an undeclared command with json_unsupported before the
// command ever runs, so a renderer that emits perfect JSON is still unreachable
// without this.
func TestNudgeShowDeclaresJSONSupport(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"nudge", "show", "--json-schema"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(nudge show --json-schema) = %d, stderr=%q", code, stderr.String())
	}
	var manifest struct {
		SchemaVersion string                     `json:"schema_version"`
		Command       []string                   `json:"command"`
		JSONSupported bool                       `json:"json_supported"`
		Schemas       map[string]json.RawMessage `json:"schemas"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		t.Fatalf("manifest is not JSON: %v\n%s", err, stdout.String())
	}
	if !manifest.JSONSupported {
		t.Fatalf("json_supported = false, want the result schema to declare support: %s", stdout.String())
	}
	if got := strings.Join(manifest.Command, " "); got != "nudge show" {
		t.Fatalf("command = %q, want \"nudge show\"", got)
	}
	if !json.Valid(manifest.Schemas["result"]) {
		t.Fatalf("result schema missing or invalid: %s", manifest.Schemas["result"])
	}
}

func TestRenderNudgeOutcome_JSONAndText(t *testing.T) {
	report := nudgeOutcomeReport{
		NudgeID:   "nudge-abc",
		Outcome:   nudgeOutcomeDropped,
		State:     "failed",
		Reason:    "queued nudge session fence mismatch",
		Agent:     "worker",
		SessionID: "gc-1",
		Source:    "session",
		Message:   "check your hook",
	}

	var stdout, stderr bytes.Buffer
	if code := renderNudgeOutcome("/city", report, true, &stdout, &stderr); code != 0 {
		t.Fatalf("renderNudgeOutcome(json) = %d, want 0; stderr: %s", code, stderr.String())
	}
	var decoded nudgeShowJSON
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode %q: %v", stdout.String(), err)
	}
	if decoded.SchemaVersion != "1" || decoded.Command != "nudge show" {
		t.Fatalf("envelope = %+v, want schema_version 1 and command \"nudge show\"", decoded)
	}
	if decoded.Outcome != nudgeOutcomeDropped || decoded.NudgeID != "nudge-abc" || decoded.Reason != report.Reason {
		t.Fatalf("payload = %+v, want the dropped outcome carried through", decoded)
	}

	stdout.Reset()
	stderr.Reset()
	if code := renderNudgeOutcome("/city", report, false, &stdout, &stderr); code != 0 {
		t.Fatalf("renderNudgeOutcome(text) = %d, want 0; stderr: %s", code, stderr.String())
	}
	for _, want := range []string{"nudge-abc", nudgeOutcomeDropped, "queued nudge session fence mismatch"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout = %q, missing %q", stdout.String(), want)
		}
	}
}
