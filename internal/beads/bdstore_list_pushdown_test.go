package beads_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// The edge read a ParentIDs list sends for tk-p1 and tk-p2, and the dependents
// bd answers it with: the issues on the near end of each parent-child edge.
// tk-c1 is listed twice, the way a bd that does not collapse rows repeats a
// dependent reached through two anchors.
const (
	twoParentEdgesCmd = `bd dep list tk-p1 tk-p2 --direction=up --type=parent-child --json`
	twoParentEdges    = `[
		{"id":"tk-c2","title":"two","status":"open","issue_type":"task","dependency_type":"parent-child"},
		{"id":"tk-c1","title":"one","status":"closed","issue_type":"task","dependency_type":"parent-child"},
		{"id":"tk-c1","title":"one","status":"closed","issue_type":"task","dependency_type":"parent-child"}
	]`
	twoChildrenCmd = `bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1,tk-c2`
	twoChildren    = `[
		{"id":"tk-c2","title":"two","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2"},
		{"id":"tk-c1","title":"one","status":"closed","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"}
	]`
	// wholeRowsCmd reads every row of the store, closed ones included.
	wholeRowsCmd = `bd list --json --all --include-infra --include-gates --limit 0`
)

func (r *recordingRunner) sent() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

func listedIDs(got []beads.Bead) []string {
	ids := make([]string, 0, len(got))
	for _, b := range got {
		ids = append(ids, b.ID)
	}
	return ids
}

func assertSent(t *testing.T, r *recordingRunner, want ...string) {
	t.Helper()
	if got := r.sent(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bd commands =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestBdStoreListParentIDsReadsEdgesThenChildrenByID pins the read shape of a
// ParentIDs list: one edge read naming every parent, then one read of the
// children those edges name, by id, and no read of every row (wholeRowsCmd).
// The read is bounded by the parents, so it needs no AllowScan.
func TestBdStoreListParentIDsReadsEdgesThenChildrenByID(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: twoParentEdges,
		twoChildrenCmd:    twoChildren,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{
		ParentIDs:     []string{"tk-p2", "tk-p1", "tk-p2"},
		IncludeClosed: true,
		Sort:          beads.SortCreatedAsc,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1", "tk-c2"}) {
		t.Fatalf("List(ParentIDs) = %v, want [tk-c1 tk-c2] in created order", ids)
	}
	if got[0].ParentID != "tk-p1" || got[1].ParentID != "tk-p2" {
		t.Fatalf("parents = %q, %q; want tk-p1, tk-p2", got[0].ParentID, got[1].ParentID)
	}
	assertSent(t, runner, twoParentEdgesCmd, twoChildrenCmd)
}

// TestBdStoreListParentIDsAnswersExactly pins that a ParentIDs list returns
// only beads whose ParentID is a listed parent. bd resolves an id it cannot
// find to another bead whose id contains it, so the edge read can name the
// children of a bead nobody asked about; the id read returns them with their
// own parent, and the list drops them.
func TestBdStoreListParentIDsAnswersExactly(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-x9","dependency_type":"parent-child"}
		]`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1,tk-x9`: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"},
			{"id":"tk-x9","title":"stranger","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","parent":"tk-p20"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1"}) {
		t.Fatalf("List(ParentIDs) = %v, want only tk-c1; tk-x9's parent tk-p20 was not listed", ids)
	}
}

// TestBdStoreListParentIDsDropsEdgesOfOtherTypes pins that only parent-child
// edges name children. The edge read asks bd for that type; a dependent bd
// reports through another edge type is not a child.
func TestBdStoreListParentIDsDropsEdgesOfOtherTypes(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-b1","dependency_type":"blocks"}
		]`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1`: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1"}) {
		t.Fatalf("List(ParentIDs) = %v, want [tk-c1]", ids)
	}
}

// TestBdStoreListParentIDsWithNoChildrenReadsNoRows pins that parents with no
// parent-child edges cost the edge read and nothing else, and that the empty
// answer is not reported as a store that cannot see its ledger: it is an
// answer about the parents.
func TestBdStoreListParentIDsWithNoChildrenReadsNoRows(t *testing.T) {
	runner := newRecordingRunner(map[string]string{twoParentEdgesCmd: noRows})
	var notices strings.Builder
	s := beads.NewBdStore(scopeWithUnreadDatabase(t), runner.run, beads.WithBdStoreNoticeSink(&notices))

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(ParentIDs) = %v, want none", listedIDs(got))
	}
	assertSent(t, runner, twoParentEdgesCmd)
	if notices.Len() != 0 {
		t.Fatalf("a childless parent set printed the unread-store notice: %q", notices.String())
	}
}

// TestBdStoreListParentIDsSpellsALoneParentTwice pins the lone-parent read.
// bd answers a single id outside batch mode, where an id it cannot resolve
// fails the whole command; spelled twice, bd skips it and reports no
// dependents. A bd that repeats the rows of a repeated id reads the children
// once.
func TestBdStoreListParentIDsSpellsALoneParentTwice(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		`bd dep list tk-p1 tk-p1 --direction=up --type=parent-child --json`: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-c1","dependency_type":"parent-child"}
		]`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1`: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"", "tk-p1", "tk-p1"}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1"}) {
		t.Fatalf("List(ParentIDs) = %v, want [tk-c1]", ids)
	}
	assertSent(t, runner,
		`bd dep list tk-p1 tk-p1 --direction=up --type=parent-child --json`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1`)
}

// TestBdStoreListParentIDsNamingNoParentReadsNothing pins that ParentIDs whose
// every entry is empty name no parent, so no bead can match and bd is not
// asked.
func TestBdStoreListParentIDsNamingNoParentReadsNothing(t *testing.T) {
	runner := newRecordingRunner(nil)
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"", ""}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("List(ParentIDs of empty ids) = %#v, want an empty non-nil slice", got)
	}
	assertSent(t, runner)
}

// TestBdStoreListParentIDsKeepsTheQuerysIDs pins that ParentIDs and IDs are
// conjunctive: the id read names only the children the query's IDs also name,
// and a query whose IDs name none of the children reads no rows.
func TestBdStoreListParentIDsKeepsTheQuerysIDs(t *testing.T) {
	t.Run("intersection", func(t *testing.T) {
		runner := newRecordingRunner(map[string]string{
			twoParentEdgesCmd: twoParentEdges,
			`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c2`: `[
				{"id":"tk-c2","title":"two","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2"}
			]`,
		})
		s := beads.NewBdStore(t.TempDir(), runner.run)
		got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IDs: []string{"tk-c2", "tk-zz"}, IncludeClosed: true})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c2"}) {
			t.Fatalf("List(ParentIDs+IDs) = %v, want [tk-c2]", ids)
		}
	})
	t.Run("disjoint", func(t *testing.T) {
		runner := newRecordingRunner(map[string]string{twoParentEdgesCmd: twoParentEdges})
		s := beads.NewBdStore(t.TempDir(), runner.run)
		got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IDs: []string{"tk-zz"}, IncludeClosed: true})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("List(ParentIDs+IDs) = %v, want none", listedIDs(got))
		}
		assertSent(t, runner, twoParentEdgesCmd)
	})
}

// TestBdStoreListParentIDsNarrowsToParentID pins the query carrying both
// selectors: its parent must be ParentID and also one of ParentIDs.
func TestBdStoreListParentIDsNarrowsToParentID(t *testing.T) {
	t.Run("listed", func(t *testing.T) {
		runner := newRecordingRunner(map[string]string{
			`bd dep list tk-p2 tk-p2 --direction=up --type=parent-child --json`: `[{"id":"tk-c2","dependency_type":"parent-child"}]`,
			`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c2`: `[
				{"id":"tk-c2","title":"two","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2"}
			]`,
		})
		s := beads.NewBdStore(t.TempDir(), runner.run)
		got, err := s.List(beads.ListQuery{ParentID: "tk-p2", ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c2"}) {
			t.Fatalf("List(ParentID+ParentIDs) = %v, want [tk-c2]", ids)
		}
	})
	t.Run("not listed", func(t *testing.T) {
		runner := newRecordingRunner(nil)
		s := beads.NewBdStore(t.TempDir(), runner.run)
		got, err := s.List(beads.ListQuery{ParentID: "tk-p3", ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("List(ParentID outside ParentIDs) = %v, want none", listedIDs(got))
		}
		assertSent(t, runner)
	})
}

// TestBdStoreListParentIDsReadsBothTiersByID pins the both-tiers shape when
// the id read leaves a child out: the id read goes to bd list, and the wisp
// tier is then read with bd query, which has no id list, so its rows are
// matched to the children in Go. The edge read already names wisp children,
// because bd reads both dependency tables.
func TestBdStoreListParentIDsReadsBothTiersByID(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-w1","dependency_type":"parent-child"}
		]`,
		`bd list --json --all --include-infra --include-gates --include-templates --limit 0 --id=tk-c1,tk-w1`: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"}
		]`,
		`bd query --json ephemeral=true --all --limit 0`: `[
			{"id":"tk-w1","title":"wisp child","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2"},
			{"id":"tk-w9","title":"other wisp","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","parent":"tk-p2"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{
		ParentIDs:     []string{"tk-p1", "tk-p2"},
		IncludeClosed: true,
		Sort:          beads.SortCreatedAsc,
		TierMode:      beads.TierBoth,
		AllowScan:     true,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1", "tk-w1"}) {
		t.Fatalf("List(ParentIDs, both tiers) = %v, want [tk-c1 tk-w1]; tk-w9 holds no edge to a listed parent", ids)
	}
	assertSent(t, runner,
		twoParentEdgesCmd,
		`bd list --json --all --include-infra --include-gates --include-templates --limit 0 --id=tk-c1,tk-w1`,
		`bd query --json ephemeral=true --all --limit 0`)
}

// TestBdStoreListParentIDsSkipsTheWispLegWhenTheIDReadHoldsEveryChild pins the
// both-tiers read once bd list has answered for every child. bd list reads the
// wisp plane for the ids too, so the wisp leg, which would read every wisp row
// to match the children in Go, is not run.
func TestBdStoreListParentIDsSkipsTheWispLegWhenTheIDReadHoldsEveryChild(t *testing.T) {
	childrenCmd := `bd list --json --all --include-infra --include-gates --include-templates --limit 0 --id=tk-c1,tk-w1`
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-w1","dependency_type":"parent-child"}
		]`,
		childrenCmd: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"},
			{"id":"tk-w1","title":"wisp child","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2","ephemeral":true}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{
		ParentIDs:     []string{"tk-p1", "tk-p2"},
		IncludeClosed: true,
		Sort:          beads.SortCreatedAsc,
		TierMode:      beads.TierBoth,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1", "tk-w1"}) {
		t.Fatalf("List(ParentIDs, both tiers) = %v, want [tk-c1 tk-w1]", ids)
	}
	if !got[1].Ephemeral {
		t.Fatalf("tk-w1 came back without Ephemeral; bd list marks a wisp row ephemeral")
	}
	assertSent(t, runner, twoParentEdgesCmd, childrenCmd)
}

// TestBdStoreListIDsWispTierSkipsTheWispLegWhenBdListReturnsEveryID pins the
// wisp-tier read of ids that bd list returned from both planes. A row bd list
// returned from the issues plane answers its id too: the wisp leg could not
// find that id on the wisp plane. The tier match then keeps only the wisp.
func TestBdStoreListIDsWispTierSkipsTheWispLegWhenBdListReturnsEveryID(t *testing.T) {
	idCmd := `bd list --json --include-infra --include-gates --include-templates --limit 0 --id=tk-c1,tk-w1`
	runner := newRecordingRunner(map[string]string{
		idCmd: `[
			{"id":"tk-c1","title":"issue","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"},
			{"id":"tk-w1","title":"wisp","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","ephemeral":true}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{IDs: []string{"tk-w1", "tk-c1"}, TierMode: beads.TierWisps})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-w1"}) {
		t.Fatalf("List(IDs, wisps) = %v, want [tk-w1]", ids)
	}
	assertSent(t, runner, idCmd)
}

// TestBdStoreListParentIDsPartialReadKeepingNoChildFails pins the partial
// child read whose surviving rows are all children of other parents. It is no
// answer about the listed parents, so the list fails with an error that names
// how many parents it read, not a partial empty answer, the way a partial bd
// list read that matches nothing fails.
func TestBdStoreListParentIDsPartialReadKeepingNoChildFails(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-x9","dependency_type":"parent-child"}
		]`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1,tk-x9`: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"not-a-time","parent":"tk-p1"},
			{"id":"tk-x9","title":"stranger","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","parent":"tk-p20"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true})
	if err == nil {
		t.Fatalf("List = %v, want an error: the partial read kept no child", listedIDs(got))
	}
	if beads.IsPartialResult(err) {
		t.Fatalf("List error = %v, want a hard error, not a PartialResultError", err)
	}
	if !strings.Contains(err.Error(), "2 parent(s)") || !strings.Contains(err.Error(), "tk-c1") {
		t.Fatalf("List error = %v, want it to name the 2 parents and the row bd could not parse", err)
	}
	if len(got) != 0 {
		t.Fatalf("List = %v alongside its error, want nothing", listedIDs(got))
	}
}

// TestBdStoreListParentIDsPartialReadNamesTheParents pins the partial child
// read that kept a child: the child comes back with a PartialResultError that
// names how many parents the list read.
func TestBdStoreListParentIDsPartialReadNamesTheParents(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: twoParentEdges,
		twoChildrenCmd: `[
			{"id":"tk-c2","title":"two","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p2"},
			{"id":"tk-c1","title":"one","status":"closed","issue_type":"task","created_at":"not-a-time","parent":"tk-p1"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true})
	if !beads.IsPartialResult(err) {
		t.Fatalf("List error = %v, want a PartialResultError", err)
	}
	if !strings.Contains(err.Error(), "2 parent(s)") {
		t.Fatalf("List error = %v, want it to name the 2 parents", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c2"}) {
		t.Fatalf("List = %v, want the child that parsed, [tk-c2]", ids)
	}
}

// TestBdStoreListParentIDsCutsThePageAfterTheParentMatch pins that a limit is
// applied to the exact children, never by bd ahead of the parent match: a
// bd-side limit could spend the page on a row the match then drops.
func TestBdStoreListParentIDsCutsThePageAfterTheParentMatch(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		twoParentEdgesCmd: `[
			{"id":"tk-c1","dependency_type":"parent-child"},
			{"id":"tk-c2","dependency_type":"parent-child"},
			{"id":"tk-x9","dependency_type":"parent-child"}
		]`,
		`bd list --json --all --include-infra --include-gates --limit 0 --id=tk-c1,tk-c2,tk-x9`: `[
			{"id":"tk-x9","title":"stranger","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p20"},
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z","parent":"tk-p1"},
			{"id":"tk-c2","title":"two","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","parent":"tk-p2"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{
		ParentIDs:     []string{"tk-p1", "tk-p2"},
		IncludeClosed: true,
		Sort:          beads.SortCreatedAsc,
		Limit:         1,
		AllowScan:     true,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1"}) {
		t.Fatalf("List(ParentIDs, Limit 1) = %v, want [tk-c1]", ids)
	}
}

// TestBdStoreListParentIDsFailsWhenTheEdgeReadFails pins that a failed edge
// read fails the list. Reading it as "no children" would report every
// parent's children gone.
func TestBdStoreListParentIDsFailsWhenTheEdgeReadFails(t *testing.T) {
	runner := newRecordingRunner(nil).fail(twoParentEdgesCmd, errors.New("dolt: lost connection to the server"))
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true, AllowScan: true})
	if err == nil {
		t.Fatalf("List = %v, want the edge read's error", listedIDs(got))
	}
	if !strings.Contains(err.Error(), "lost connection") {
		t.Fatalf("List error = %v, want it to carry the edge read's failure", err)
	}
	assertSent(t, runner, twoParentEdgesCmd)
}

// TestBdStoreListParentIDsReadsEveryRowWhenTheBackendHasNoEdgeRead pins the
// store whose bd backend cannot answer bd dep list (the Postgres backend
// reports IssueRelations "not supported"). The list reads every row instead
// and still answers exactly.
func TestBdStoreListParentIDsReadsEveryRowWhenTheBackendHasNoEdgeRead(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		wholeRowsCmd: `[
			{"id":"tk-c1","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","parent":"tk-p1"},
			{"id":"tk-o1","title":"orphan","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z"},
			{"id":"tk-x9","title":"stranger","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","parent":"tk-p20"}
		]`,
	}).fail(twoParentEdgesCmd, fmt.Errorf(`operation "IssueRelations" not supported by the postgres backend`))
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{ParentIDs: []string{"tk-p1", "tk-p2"}, IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-c1"}) {
		t.Fatalf("List(ParentIDs) = %v, want [tk-c1]", ids)
	}
	assertSent(t, runner, twoParentEdgesCmd, wholeRowsCmd)
}

// TestBdStoreListSendsIDsAsAnIDFilter pins the IDs pushdown: the listed ids go
// to bd list as one --id filter, distinct and sorted so the argv does not
// depend on the caller's order, and an empty entry is left out because it
// names no bead.
func TestBdStoreListSendsIDsAsAnIDFilter(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		`bd list --json --status=open --include-infra --include-gates --limit 0 --id=tk-a,tk-b`: `[
			{"id":"tk-a","title":"a","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"},
			{"id":"tk-b","title":"b","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z"}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{IDs: []string{"tk-b", "", "tk-a", "tk-b"}, Status: "open"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); len(ids) != 2 {
		t.Fatalf("List(IDs) = %v, want tk-a and tk-b", ids)
	}
	assertSent(t, runner, `bd list --json --status=open --include-infra --include-gates --limit 0 --id=tk-a,tk-b`)
}

// TestBdStoreListIDsNamingNoBeadReadsNothing pins that IDs whose every entry is
// empty match no bead, so bd is not asked. bd reads an empty --id as no filter
// at all, so sending it would read the whole store.
func TestBdStoreListIDsNamingNoBeadReadsNothing(t *testing.T) {
	runner := newRecordingRunner(nil)
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{IDs: []string{""}, IncludeClosed: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("List(IDs of the empty id) = %#v, want an empty non-nil slice", got)
	}
	assertSent(t, runner)
}

// TestBdStoreListReadsAnIDBdListCannotCarryThroughAQuotedQuery pins the ids
// --id cannot carry: bd splits the value on commas and trims each id, so an id
// holding a comma or edge whitespace would reach bd as different ids and the
// bead would be lost. Such an id is read alone through bd query, whose quoted
// id= clause selects it exactly, the other ids still ride --id, and no read
// takes every row (wholeRowsCmd).
func TestBdStoreListReadsAnIDBdListCannotCarryThroughAQuotedQuery(t *testing.T) {
	for _, tc := range []struct {
		odd, clause string
	}{
		{odd: "tk-a,b", clause: `id="tk-a,b"`},
		{odd: " tk-a", clause: `id=" tk-a"`},
		{odd: "tk-a\t", clause: "id=\"tk-a\t\""},
		{odd: `tk-"a"\,b`, clause: `id="tk-\"a\"\\,b"`},
	} {
		t.Run(fmt.Sprintf("%q", tc.odd), func(t *testing.T) {
			listCmd := `bd list --json --all --include-infra --include-gates --limit 0 --id=tk-b`
			queryCmd := `bd query --json ` + tc.clause + ` --all --limit 0`
			runner := newRecordingRunner(map[string]string{
				listCmd: `[{"id":"tk-b","title":"b","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z"}]`,
				queryCmd: fmt.Sprintf(`[
					{"id":%q,"title":"odd","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"}
				]`, tc.odd),
			})
			s := beads.NewBdStore(t.TempDir(), runner.run)

			got, err := s.List(beads.ListQuery{IDs: []string{tc.odd, "tk-b"}, IncludeClosed: true, Sort: beads.SortCreatedAsc})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{tc.odd, "tk-b"}) {
				t.Fatalf("List(IDs) = %q, want [%q tk-b]", ids, tc.odd)
			}
			assertSent(t, runner, listCmd, queryCmd)
		})
	}
}

// TestBdStoreListMatchesAnIDNoBoundedReadSelectsInGo pins the ids neither read
// can carry. bd's query lexer reads one byte at a time, so a non-ASCII byte in
// an id that --id cannot carry reaches the comparison altered, and an id longer
// than one argument may not survive exec. Only a read of every row selects
// such an id, so the list takes that one read and matches in Go, exactly.
func TestBdStoreListMatchesAnIDNoBoundedReadSelectsInGo(t *testing.T) {
	for name, odd := range map[string]string{
		"non-ASCII beside a comma": "tk-é,b",
		"longer than an argument":  "tk-" + strings.Repeat("x", 64<<10),
	} {
		t.Run(name, func(t *testing.T) {
			runner := newRecordingRunner(map[string]string{
				wholeRowsCmd: fmt.Sprintf(`[
					{"id":%q,"title":"odd","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"},
					{"id":"tk-b","title":"b","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z"},
					{"id":"tk-c","title":"c","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z"}
				]`, odd),
			})
			s := beads.NewBdStore(t.TempDir(), runner.run)

			got, err := s.List(beads.ListQuery{IDs: []string{odd, "tk-b"}, IncludeClosed: true, Sort: beads.SortCreatedAsc})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{odd, "tk-b"}) {
				t.Fatalf("List(IDs) = %d ids, want the odd id and tk-b", len(ids))
			}
			assertSent(t, runner, wholeRowsCmd)
		})
	}
}

// TestBdStoreListReadsALongIDListInChunks pins the size bound on the --id
// value: Linux refuses a single argument over 128 KiB, so 12,000 ids of ten
// bytes, 131,999 bytes joined, go to bd as three reads whose values each stay
// within 64 KiB. Together the reads name every id once, and no read takes
// every row.
func TestBdStoreListReadsALongIDListInChunks(t *testing.T) {
	ids := make([]string, 0, 12000)
	for i := 0; i < cap(ids); i++ {
		ids = append(ids, fmt.Sprintf("tk-%07d", i))
	}
	row := func(id, created string) string {
		return fmt.Sprintf(`{"id":%q,"title":"t","status":"open","issue_type":"task","created_at":%q}`, id, created)
	}
	var mu sync.Mutex
	var values []string
	run := func(_, name string, args ...string) ([]byte, error) {
		value := ""
		for _, arg := range args {
			if v, ok := strings.CutPrefix(arg, "--id="); ok {
				value = v
			}
		}
		if name != "bd" || args[0] != "list" || value == "" {
			return nil, fmt.Errorf("unexpected read: %s %s", name, strings.Join(args, " "))
		}
		mu.Lock()
		values = append(values, value)
		mu.Unlock()
		var rows []string
		for _, id := range strings.Split(value, ",") {
			switch id {
			case "tk-0000042":
				rows = append(rows, row(id, "2026-05-01T00:00:00Z"))
			case "tk-0011999":
				rows = append(rows, row(id, "2026-05-02T00:00:00Z"))
			}
		}
		return []byte("[" + strings.Join(rows, ",") + "]"), nil
	}
	s := beads.NewBdStore(t.TempDir(), run)

	got, err := s.List(beads.ListQuery{IDs: ids, IncludeClosed: true, Sort: beads.SortCreatedAsc})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := listedIDs(got); !reflect.DeepEqual(got, []string{"tk-0000042", "tk-0011999"}) {
		t.Fatalf("List(IDs) = %v, want [tk-0000042 tk-0011999]", got)
	}
	if len(values) != 3 {
		t.Fatalf("bd list reads = %d, want 3", len(values))
	}
	var named []string
	for _, v := range values {
		if len(v) > 64<<10 {
			t.Fatalf("an --id value is %d bytes, over 64 KiB", len(v))
		}
		named = append(named, strings.Split(v, ",")...)
	}
	if !reflect.DeepEqual(named, ids) {
		t.Fatalf("the reads named %d ids, want each of the %d ids once, in order", len(named), len(ids))
	}
}

// TestBdStoreListIDsKeepsTheWispLegUnbounded pins the wisp leg of an IDs read.
// bd query has no id list, so the ids are matched in Go after the read, and a
// bd-side limit there would cut the tier before the match and lose the
// requested wisp.
func TestBdStoreListIDsKeepsTheWispLegUnbounded(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		`bd query --json ephemeral=true --limit 0`: `[
			{"id":"tk-w8","title":"other","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z","ephemeral":true},
			{"id":"tk-w1","title":"wanted","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z","ephemeral":true}
		]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{IDs: []string{"tk-w1"}, Limit: 1, TierMode: beads.TierWisps})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{"tk-w1"}) {
		t.Fatalf("List(IDs, wisps, Limit 1) = %v, want [tk-w1]", ids)
	}
}
