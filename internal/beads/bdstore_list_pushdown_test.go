package beads_test

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

const (
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
