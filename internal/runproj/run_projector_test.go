package runproj

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
)

// runProjectorIDs is the id universe of the randomized streams: run roots,
// dotted children and grandchildren, and free-standing beads, few enough that
// a stream re-roots, deletes and recreates the same ids many times.
var runProjectorIDs = []string{"r1", "r2", "r3", "r1.1", "r1.2", "r2.1", "r2.1.x", "b1", "b2", "b3", "m1", "s1"}

func pick[T any](rng *rand.Rand, xs []T) T { return xs[rng.IntN(len(xs))] }

// randomRunBead draws a bead snapshot that exercises every input the run
// summary and census read: types inside and outside the run filter, gc:
// labels, the root markers (gc.root_bead_id, molecule_id, gc.kind, formula
// metadata, source-run keys), terminal outcomes, cancel requests, statuses
// padded with spaces, and update times that tie at second granularity or fall
// back to the creation time.
func randomRunBead(rng *rand.Rand, id string) beads.Bead {
	base := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	b := beads.Bead{
		ID:          id,
		Title:       pick(rng, []string{"", "build it", "review the change", "step"}),
		Status:      pick(rng, []string{"open", "in_progress", "closed", "closed", "blocked", " closed "}),
		Type:        pick(rng, []string{"task", "task", "molecule", "step", "bug", "message", "session", "convoy", ""}),
		Assignee:    pick(rng, []string{"", "worker-a", "worker-b"}),
		CreatedAt:   base.Add(time.Duration(rng.IntN(8)) * time.Second),
		Description: pick(rng, []string{"", "please review", "do the work"}),
	}
	if rng.IntN(4) > 0 {
		b.UpdatedAt = base.Add(time.Duration(rng.IntN(8)) * time.Second)
	}
	if rng.IntN(3) == 0 {
		b.ParentID = pick(rng, runProjectorIDs)
	}
	switch rng.IntN(8) {
	case 0:
		b.Labels = []string{"gc:control"}
	case 1:
		b.Labels = []string{"team"}
	}
	md := beads.StringMap{}
	maybe := func(key string, values ...string) {
		if rng.IntN(3) == 0 {
			md[key] = pick(rng, values)
		}
	}
	maybe(beadmeta.RootBeadIDMetadataKey, "r1", "r2", "r3", "r2.1")
	maybe(beadmeta.MoleculeIDMetadataKey, "r1", " r2 ", "m1")
	maybe(beadmeta.KindMetadataKey, "run", "other")
	maybe(beadmeta.FormulaContractMetadataKey, "graph.v2")
	maybe(beadmeta.FormulaMetadataKey, "mol-adopt-pr-v2", "mol-polecat-work", "mol-bug-report-flow-v2")
	maybe(beadmeta.OutcomeMetadataKey, beadmeta.OutcomeFail, beadmeta.OutcomeCanceled, beadmeta.OutcomeSkipped, "pass")
	maybe(beadmeta.CancelRequestedMetadataKey, "true")
	maybe(beadmeta.StepRefMetadataKey, "implement", "review", "submit")
	maybe(beadmeta.ScopeKindMetadataKey, "rig", "city")
	maybe(beadmeta.ScopeRefMetadataKey, "gascity")
	if rng.IntN(10) == 0 {
		md["pr_review.run_root_id"] = pick(rng, []string{"r1", "r3"})
	}
	if len(md) > 0 {
		b.Metadata = md
	}
	return b
}

// randomRunBatch draws one tail poll's worth of events: bead lifecycle events
// (some id-only deletes, some undecodable), non-bead events, and envelope
// correlation ids for the fold's backfill.
func randomRunBatch(rng *rand.Rand, seq *uint64) []events.Event {
	n := 1 + rng.IntN(6)
	evts := make([]events.Event, 0, n)
	for range n {
		*seq++
		id := pick(rng, runProjectorIDs)
		e := events.Event{Seq: *seq, Subject: id}
		switch r := rng.IntN(20); {
		case r < 2:
			e.Type = events.SessionWoke
		case r == 2:
			e.Type = events.BeadUpdated
			e.Payload = json.RawMessage(`{"title":"no id"}`)
		case r == 3:
			e.Type = events.BeadDeleted
			e.Payload = json.RawMessage(`{"id":"` + id + `"}`)
		case r == 4:
			e.Type = events.BeadDeleted
			payload, _ := json.Marshal(randomRunBead(rng, id))
			e.Payload = payload
		default:
			e.Type = pick(rng, []string{events.BeadCreated, events.BeadUpdated, events.BeadUpdated, events.BeadClosed})
			payload, _ := json.Marshal(randomRunBead(rng, id))
			e.Payload = payload
			if rng.IntN(4) == 0 {
				e.StepID = pick(rng, []string{"implement", "review"})
			}
		}
		evts = append(evts, e)
	}
	return evts
}

// fullRunBuild is the reference the RunProjector must reproduce: a Projector's
// fold, filtered, through the full builders.
func fullRunBuild(p *Projector) ([]beads.Bead, RunSummary, CanonicalRunStatusCounts) {
	beadList := FilterRunBeads(p.Beads())
	summary, lanes := BuildRunSummaryWithAllLanes(beadList)
	return beadList, summary, CountCanonicalRunStatuses(beadList, lanes)
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func assertRunProjectorMatches(t *testing.T, label string, ref *Projector, rp *RunProjector) {
	t.Helper()
	wantBeads, wantSummary, wantCensus := fullRunBuild(ref)
	if got := rp.Beads(); !reflect.DeepEqual(got, wantBeads) {
		t.Fatalf("%s: beads differ:\ngot  %v\nwant %v", label, idsOf(got), idsOf(wantBeads))
	}
	gotSummary, gotCensus := rp.Summary()
	if !reflect.DeepEqual(gotSummary, wantSummary) {
		t.Fatalf("%s: summary differs:\ngot  %s\nwant %s", label, jsonOf(t, gotSummary), jsonOf(t, wantSummary))
	}
	if gotCensus != wantCensus {
		t.Fatalf("%s: census = %+v, want %+v", label, gotCensus, wantCensus)
	}
	if rp.LastSeq() != ref.LastSeq() || rp.DecodeMisses() != ref.DecodeMisses() {
		t.Fatalf("%s: cursor %d / misses %d, want %d / %d", label, rp.LastSeq(), rp.DecodeMisses(), ref.LastSeq(), ref.DecodeMisses())
	}
}

// The RunProjector's beads, summary and census equal the full builders' over
// the filtered fold after every batch of a randomized stream, and a batch it
// reports unchanged leaves the filtered beads unchanged, so skipping that
// rebuild loses nothing.
func TestRunProjectorMatchesFullBuild(t *testing.T) {
	for seed := uint64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed*7919))
		ref := NewProjector()
		rp := NewRunProjector()
		var seq uint64
		for batch := range 40 {
			evts := randomRunBatch(rng, &seq)
			before := rp.Beads()
			ref.Apply(evts)
			changed := rp.Apply(evts)
			label := "seed " + jsonOfSeed(seed, batch)
			if !changed && !reflect.DeepEqual(rp.Beads(), before) {
				t.Fatalf("%s: Apply reported no change but the run beads changed", label)
			}
			// Summarize only some batches, so dirty runs accumulate across
			// several folds before a rebuild, as between tail polls.
			if rng.IntN(3) > 0 {
				assertRunProjectorMatches(t, label, ref, rp)
			}
		}
		assertRunProjectorMatches(t, "seed "+jsonOfSeed(seed, -1), ref, rp)
	}
}

func jsonOfSeed(seed uint64, batch int) string {
	raw, _ := json.Marshal(map[string]any{"seed": seed, "batch": batch})
	return string(raw)
}

// The shared bead fixture, folded as bead.created events, projects to the same
// summary and census as the full builders over it.
func TestRunProjectorMatchesFullBuildOnFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "beads_fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	var beadList []beads.Bead
	if err := json.Unmarshal(raw, &beadList); err != nil {
		t.Fatal(err)
	}
	evts := make([]events.Event, 0, len(beadList))
	for i, b := range beadList {
		evts = append(evts, runBeadEvent(uint64(i+1), events.BeadCreated, b))
	}
	ref := NewProjector()
	ref.Apply(evts)
	rp := NewRunProjector()
	rp.Apply(evts)
	assertRunProjectorMatches(t, "fixture", ref, rp)
	if summary, _ := rp.Summary(); summary.TotalActive+summary.TotalHistorical+len(summary.BlockedLanes) == 0 {
		t.Fatal("control: the fixture projects no run")
	}
}

// A change to one run's bead rebuilds that run's lane alone; a change to a
// bead outside the run filter rebuilds nothing and reports no change.
func TestRunProjectorRebuildsOnlyTheChangedRun(t *testing.T) {
	rp := NewRunProjector()
	var seq uint64
	event := func(typ string, b beads.Bead) events.Event {
		seq++
		return runBeadEvent(seq, typ, b)
	}
	var evts []events.Event
	for _, root := range []string{"run-a", "run-b", "run-c"} {
		evts = append(evts, event(events.BeadCreated, beads.Bead{ID: root, Type: "molecule", Status: "open"}))
		for _, step := range []string{".1", ".2"} {
			evts = append(evts, event(events.BeadCreated, beads.Bead{
				ID: root + step, Type: "task", Status: "open",
				Metadata: beads.StringMap{beadmeta.RootBeadIDMetadataKey: root},
			}))
		}
	}
	rp.Apply(evts)
	if summary, _ := rp.Summary(); summary.TotalActive != 3 {
		t.Fatalf("control: %d active runs, want 3", summary.TotalActive)
	}

	before := rp.laneRefreshes
	if !rp.Apply([]events.Event{event(events.BeadUpdated, beads.Bead{
		ID: "run-b.1", Type: "task", Status: "in_progress",
		Metadata: beads.StringMap{beadmeta.RootBeadIDMetadataKey: "run-b"},
	})}) {
		t.Fatal("a run bead update reported no change")
	}
	summary, census := rp.Summary()
	if got := rp.laneRefreshes - before; got != 1 {
		t.Fatalf("rebuilt %d lanes for one run's change, want 1", got)
	}
	if summary.TotalActive != 3 || census.Active != 1 || census.Pending != 2 {
		t.Fatalf("summary active %d, census %+v; want 3 active runs, 1 started", summary.TotalActive, census)
	}

	before = rp.laneRefreshes
	if rp.Apply([]events.Event{event(events.BeadUpdated, beads.Bead{ID: "worker-session", Type: "session", Status: "open"})}) {
		t.Fatal("a session bead update reported a run change")
	}
	rp.Summary()
	if got := rp.laneRefreshes - before; got != 0 {
		t.Fatalf("rebuilt %d lanes for a session bead, want 0", got)
	}
}

// Run beads share one copy of each metadata key.
func TestRunProjectorInternsMetadataKeys(t *testing.T) {
	rp := NewRunProjector()
	for i, id := range []string{"run-a", "run-b"} {
		rp.Apply([]events.Event{runBeadEvent(uint64(i+1), events.BeadCreated, beads.Bead{
			ID: id, Type: "molecule", Status: "open",
			Metadata: beads.StringMap{beadmeta.FormulaMetadataKey: "mol-x"},
		})})
	}
	keyOf := func(b beads.Bead) string {
		for k := range b.Metadata {
			return k
		}
		t.Fatal("bead has no metadata")
		return ""
	}
	got := rp.Beads()
	if len(got) != 2 {
		t.Fatalf("got %d beads, want 2", len(got))
	}
	if a, b := keyOf(got[0]), keyOf(got[1]); unsafe.StringData(a) != unsafe.StringData(b) {
		t.Fatal("the two beads hold separate copies of the same metadata key")
	}
}

// ColdLoad folds the event log on disk to the projection Apply builds from the
// same events, and stops with ctx's error.
func TestRunProjectorColdLoad(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 42))
	var seq uint64
	var all []events.Event
	for range 50 {
		all = append(all, randomRunBatch(rng, &seq)...)
	}
	path := filepath.Join(t.TempDir(), "events.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, e := range all {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	ref := NewProjector()
	ref.Apply(all)
	rp := NewRunProjector()
	if err := rp.ColdLoad(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	assertRunProjectorMatches(t, "cold load", ref, rp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewRunProjector().ColdLoad(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cold load: err = %v, want context.Canceled", err)
	}
}
