package beads_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
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
		AllowScan:     true,
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

// TestBdStoreListParentIDsReadsBothTiersByID pins the both-tiers shape: the id
// read goes to bd list, and the wisp tier is read with bd query, which has no
// id list, so its rows are matched to the children in Go. The edge read
// already names wisp children, because bd reads both dependency tables.
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

// TestBdStoreListIDsBdCannotSpellAreMatchedInGo pins the ids --id cannot carry:
// bd splits the value on commas and trims each id, so an id holding a comma or
// edge whitespace would reach bd as different ids and the bead would be lost.
// Such a list goes to bd unfiltered and is matched in Go, exactly.
func TestBdStoreListIDsBdCannotSpellAreMatchedInGo(t *testing.T) {
	for _, odd := range []string{"tk-a,b", " tk-a", "tk-a\t"} {
		t.Run(fmt.Sprintf("%q", odd), func(t *testing.T) {
			runner := newRecordingRunner(map[string]string{
				wholeRowsCmd: fmt.Sprintf(`[
					{"id":%q,"title":"odd","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"},
					{"id":"tk-b","title":"b","status":"open","issue_type":"task","created_at":"2026-05-02T00:00:00Z"},
					{"id":"tk-c","title":"c","status":"open","issue_type":"task","created_at":"2026-05-03T00:00:00Z"}
				]`, odd),
			})
			s := beads.NewBdStore(t.TempDir(), runner.run)

			got, err := s.List(beads.ListQuery{IDs: []string{odd, "tk-b"}, IncludeClosed: true})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if ids := listedIDs(got); !reflect.DeepEqual(ids, []string{odd, "tk-b"}) {
				t.Fatalf("List(IDs) = %q, want [%q tk-b]", ids, odd)
			}
			assertSent(t, runner, wholeRowsCmd)
		})
	}
}

// TestBdStoreListIDsTooLongForOneArgumentAreMatchedInGo pins the size bound on
// the --id value: Linux refuses a single argument over 128 KiB, so an id list
// longer than that (12,000 ids of 11 bytes each) goes to bd unfiltered and is
// matched in Go.
func TestBdStoreListIDsTooLongForOneArgumentAreMatchedInGo(t *testing.T) {
	ids := make([]string, 0, 12000)
	for i := 0; i < cap(ids); i++ {
		ids = append(ids, fmt.Sprintf("tk-%07d", i))
	}
	runner := newRecordingRunner(map[string]string{
		wholeRowsCmd: `[{"id":"tk-0000042","title":"one","status":"open","issue_type":"task","created_at":"2026-05-01T00:00:00Z"}]`,
	})
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{IDs: ids, IncludeClosed: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := listedIDs(got); !reflect.DeepEqual(got, []string{"tk-0000042"}) {
		t.Fatalf("List(IDs) = %v, want [tk-0000042]", got)
	}
	assertSent(t, runner, wholeRowsCmd)
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
