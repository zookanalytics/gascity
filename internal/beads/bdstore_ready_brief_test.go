package beads_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// bdReadyPageRows is the page a brief ready read asks bd for first.
const bdReadyPageRows = 5000

const (
	briefPageCmd      = `bd ready --json --include-ephemeral --limit 5000 --brief`
	briefUnboundedCmd = `bd ready --json --include-ephemeral --limit 0 --brief`
)

// readyRowsJSON renders n open task rows named bd-row-<i>, then a row for each
// extra id, as the array bd ready prints.
func readyRowsJSON(t *testing.T, n int, extra ...string) string {
	t.Helper()
	rows := make([]map[string]string, 0, n+len(extra))
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]string{"id": fmt.Sprintf("bd-row-%d", i), "title": "ready", "status": "open", "issue_type": "task", "created_at": "2025-01-15T10:30:00Z"})
	}
	for _, id := range extra {
		rows = append(rows, map[string]string{"id": id, "title": "ready", "status": "open", "issue_type": "task", "created_at": "2025-01-15T10:31:00Z"})
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("marshal ready rows: %v", err)
	}
	return string(data)
}

func containsReadyID(items []beads.Bead, id string) bool {
	for _, b := range items {
		if b.ID == id {
			return true
		}
	}
	return false
}

// TestBdStoreReadyBriefReadsOnePageOfBriefRows pins the brief read's shape: one
// bd call for a page of --brief rows, which is the whole ready set when the page
// is not full.
func TestBdStoreReadyBriefReadsOnePageOfBriefRows(t *testing.T) {
	runner := newRecordingRunner(map[string]string{briefPageCmd: readyRowsJSON(t, 2)})

	got, err := beads.NewBdStore(t.TempDir(), runner.run).Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Brief: true})
	if err != nil {
		t.Fatalf("Ready(brief): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Ready(brief) = %v, want the page's 2 rows", listedIDs(got))
	}
	assertSent(t, runner, briefPageCmd)
}

// TestBdStoreReadyBriefRereadsTheWholeSetWhenThePageFills pins that a brief
// read returns the whole ready set. A page that comes back full may have
// stopped short of it, so the read is taken again with no limit, and a row past
// the page reaches the caller.
func TestBdStoreReadyBriefRereadsTheWholeSetWhenThePageFills(t *testing.T) {
	runner := newRecordingRunner(map[string]string{
		briefPageCmd:      readyRowsJSON(t, bdReadyPageRows),
		briefUnboundedCmd: readyRowsJSON(t, bdReadyPageRows, "bd-past-the-page"),
	})

	got, err := beads.NewBdStore(t.TempDir(), runner.run).Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Brief: true})
	if err != nil {
		t.Fatalf("Ready(brief): %v", err)
	}
	if len(got) != bdReadyPageRows+1 || !containsReadyID(got, "bd-past-the-page") {
		t.Fatalf("Ready(brief) returned %d rows (row past the page present: %t), want the whole set of %d", len(got), containsReadyID(got, "bd-past-the-page"), bdReadyPageRows+1)
	}
	assertSent(t, runner, briefPageCmd, briefUnboundedCmd)
}

// TestBdStoreReadyBriefRereadsAPageWithACorruptRow pins that the page is
// counted by the rows bd printed, not by the rows that parsed: a full page with
// one corrupt row still stopped at the limit, so the read is taken again whole.
func TestBdStoreReadyBriefRereadsAPageWithACorruptRow(t *testing.T) {
	page := readyRowsJSON(t, bdReadyPageRows-1)
	page = strings.TrimSuffix(page, "]") + `,{"id":"bd-corrupt","status":"open","issue_type":"task","created_at":"not-a-time"}]`
	runner := newRecordingRunner(map[string]string{
		briefPageCmd:      page,
		briefUnboundedCmd: readyRowsJSON(t, bdReadyPageRows, "bd-past-the-page"),
	})

	got, err := beads.NewBdStore(t.TempDir(), runner.run).Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Brief: true})
	if err != nil {
		t.Fatalf("Ready(brief): %v", err)
	}
	if !containsReadyID(got, "bd-past-the-page") {
		t.Fatalf("Ready(brief) returned %d rows without the row past the page; a full page holding a corrupt row was taken as the whole set", len(got))
	}
	assertSent(t, runner, briefPageCmd, briefUnboundedCmd)
}

// TestBdStoreReadyBriefFallsBackWhenBdLacksTheFlag pins the read on a bd older
// than 1.2.0, which rejects --brief: the read is retried with full rows, and
// every later store over the same scope skips the refused attempt.
func TestBdStoreReadyBriefFallsBackWhenBdLacksTheFlag(t *testing.T) {
	const (
		refusedCmd  = `bd ready --json --limit 5000 --brief`
		fullRowsCmd = `bd ready --json --limit 5000`
	)
	refusal := errors.New("exit status 1: Error: unknown flag: --brief")
	dir := t.TempDir()

	first := newRecordingRunner(map[string]string{fullRowsCmd: readyRowsJSON(t, 1)}).fail(refusedCmd, refusal)
	got, err := beads.NewBdStore(dir, first.run).Ready(beads.ReadyQuery{Brief: true})
	if err != nil {
		t.Fatalf("Ready(brief) on a bd without --brief: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Ready(brief) = %v, want the full-row read's 1 row", listedIDs(got))
	}
	assertSent(t, first, refusedCmd, fullRowsCmd)

	second := newRecordingRunner(map[string]string{fullRowsCmd: readyRowsJSON(t, 1)}).fail(refusedCmd, refusal)
	if _, err := beads.NewBdStore(dir, second.run).Ready(beads.ReadyQuery{Brief: true}); err != nil {
		t.Fatalf("second store's Ready(brief): %v", err)
	}
	assertSent(t, second, fullRowsCmd)
}

// TestBdStoreReadyBriefSurfacesOtherFailures pins that only bd's own rejection
// of --brief earns the full-row retry. A usage error whose help text merely
// lists the flag, and any other failure, is the read's error.
func TestBdStoreReadyBriefSurfacesOtherFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "another unknown flag", err: errors.New("exit status 1: Error: unknown flag: --include-ephemeral\nFlags:\n      --brief   Omit the free-form text")},
		{name: "a store failure", err: errors.New("exit status 1: Error: database is locked")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := newRecordingRunner(nil).fail(briefPageCmd, tc.err)
			if _, err := beads.NewBdStore(t.TempDir(), runner.run).Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Brief: true}); err == nil {
				t.Fatal("Ready(brief) error = nil, want the read's failure")
			}
			assertSent(t, runner, briefPageCmd)
		})
	}
}
