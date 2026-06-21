package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/doctor"
)

// fakeWispDB models one Dolt database's wisp tables: a row is live when its
// issue_id is present in wispIDs, an orphan otherwise. It stores real issue_ids
// per row (not just counts) so a cleanup that drops a live row fails the test.
type fakeWispDB struct {
	hasWisps bool
	wispIDs  map[string]bool
	// rows maps a wisp auxiliary table name to the owning issue_id of each row.
	// Multiple rows may share an issue_id (e.g. several events for one wisp).
	rows map[string][]string
}

func (d *fakeWispDB) orphanRows(table string) int64 {
	var n int64
	for _, issueID := range d.rows[table] {
		if !d.wispIDs[issueID] {
			n++
		}
	}
	return n
}

func (d *fakeWispDB) liveRows(table string) int64 {
	var n int64
	for _, issueID := range d.rows[table] {
		if d.wispIDs[issueID] {
			n++
		}
	}
	return n
}

// deleteOrphans removes up to limit orphan rows from table, preserving every
// live row, and returns how many it removed — mirroring the bounded
// DELETE ... LIMIT the production client issues.
func (d *fakeWispDB) deleteOrphans(table string, limit int) int64 {
	kept := make([]string, 0, len(d.rows[table]))
	var removed int64
	for _, issueID := range d.rows[table] {
		if !d.wispIDs[issueID] && removed < int64(limit) {
			removed++
			continue
		}
		kept = append(kept, issueID)
	}
	d.rows[table] = kept
	return removed
}

type fakeWispOrphanClient struct {
	dbs   map[string]*fakeWispDB
	order []string

	listErr   error
	tablesErr map[string]error
	countErr  map[string]error
	deleteErr map[string]error

	deleteCalls []fakeWispDeleteCall
	closed      bool
}

type fakeWispDeleteCall struct {
	db    string
	table string
	limit int
}

func (c *fakeWispOrphanClient) ListUserDatabases(_ context.Context) ([]string, error) {
	if c.listErr != nil {
		return nil, c.listErr
	}
	return append([]string(nil), c.order...), nil
}

func (c *fakeWispOrphanClient) WispOrphanTables(_ context.Context, db string) ([]string, error) {
	if err := c.tablesErr[db]; err != nil {
		return nil, err
	}
	d := c.dbs[db]
	if d == nil || !d.hasWisps {
		return nil, nil
	}
	// Return the known tables that exist in this db, in canonical order.
	var out []string
	for _, table := range wispOrphanTables {
		if _, ok := d.rows[table]; ok {
			out = append(out, table)
		}
	}
	return out, nil
}

func (c *fakeWispOrphanClient) CountOrphans(_ context.Context, db, table string) (int64, error) {
	if err := c.countErr[db+"/"+table]; err != nil {
		return 0, err
	}
	return c.dbs[db].orphanRows(table), nil
}

func (c *fakeWispOrphanClient) DeleteOrphans(_ context.Context, db, table string, limit int) (int64, error) {
	c.deleteCalls = append(c.deleteCalls, fakeWispDeleteCall{db: db, table: table, limit: limit})
	if err := c.deleteErr[db+"/"+table]; err != nil {
		return 0, err
	}
	return c.dbs[db].deleteOrphans(table, limit), nil
}

func (c *fakeWispOrphanClient) Close() error {
	c.closed = true
	return nil
}

// newFakeWispClient builds a two-database fixture: one bd store with orphans in
// every auxiliary table plus live rows that must be preserved, and one database
// without wisp tables that the scan must skip.
func newFakeWispClient() *fakeWispOrphanClient {
	store := &fakeWispDB{
		hasWisps: true,
		wispIDs:  map[string]bool{"live-1": true, "live-2": true},
		rows: map[string][]string{
			// 3 orphan label rows, 2 live label rows.
			"wisp_labels": {"gone-1", "live-1", "gone-2", "gone-3", "live-2"},
			// 4 orphan event rows, 1 live.
			"wisp_events": {"gone-1", "gone-1", "gone-2", "live-1", "gone-4"},
			// 2 orphan comment rows, 1 live.
			"wisp_comments": {"gone-2", "live-2", "gone-5"},
			// 1 orphan dependency row (source issue_id gone), 1 live.
			"wisp_dependencies": {"gone-3", "live-1"},
		},
	}
	notAStore := &fakeWispDB{hasWisps: false, rows: map[string][]string{}}
	return &fakeWispOrphanClient{
		dbs:   map[string]*fakeWispDB{"store": store, "scratch": notAStore},
		order: []string{"store", "scratch"},
	}
}

func TestScanWispOrphans_CountsPerTableAndSkipsNonStores(t *testing.T) {
	client := newFakeWispClient()
	res := scanWispOrphans(context.Background(), client)

	if len(res.Errors) != 0 {
		t.Fatalf("unexpected scan errors: %v", res.Errors)
	}
	if res.DatabasesScanned != 1 {
		t.Fatalf("DatabasesScanned = %d, want 1 (scratch db has no wisp tables)", res.DatabasesScanned)
	}
	// 3 + 4 + 2 + 1 = 10 orphan rows total.
	if res.Total != 10 {
		t.Fatalf("Total = %d, want 10", res.Total)
	}

	got := map[string]int64{}
	for _, c := range res.Counts {
		if c.Database != "store" {
			t.Fatalf("unexpected database in counts: %q", c.Database)
		}
		got[c.Table] = c.Count
	}
	want := map[string]int64{
		"wisp_labels":       3,
		"wisp_events":       4,
		"wisp_comments":     2,
		"wisp_dependencies": 1,
	}
	for table, n := range want {
		if got[table] != n {
			t.Errorf("orphan count for %s = %d, want %d", table, got[table], n)
		}
	}
}

func TestCleanupWispOrphans_RemovesOrphansPreservesLiveRows(t *testing.T) {
	client := newFakeWispClient()
	store := client.dbs["store"]

	// Capture the live-row population before cleanup so we can prove none are lost.
	liveBefore := map[string]int64{}
	for _, table := range wispOrphanTables {
		liveBefore[table] = store.liveRows(table)
	}

	res := cleanupWispOrphans(context.Background(), client, 2)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected cleanup errors: %v", res.Errors)
	}
	if res.Total != 10 {
		t.Fatalf("cleanup Total = %d, want 10", res.Total)
	}

	// Every orphan removed; every live row preserved.
	for _, table := range wispOrphanTables {
		if orphans := store.orphanRows(table); orphans != 0 {
			t.Errorf("after cleanup %s still has %d orphan rows", table, orphans)
		}
		if live := store.liveRows(table); live != liveBefore[table] {
			t.Errorf("after cleanup %s live rows = %d, want %d (live rows must be preserved)", table, live, liveBefore[table])
		}
	}

	// A re-scan must now report zero orphans.
	if rescan := scanWispOrphans(context.Background(), client); rescan.Total != 0 {
		t.Errorf("re-scan Total = %d, want 0", rescan.Total)
	}
}

func TestCleanupWispOrphans_BatchesBoundedDeletes(t *testing.T) {
	client := newFakeWispClient()
	cleanupWispOrphans(context.Background(), client, 2)

	// wisp_events has 4 orphans; with batch=2 the engine must issue at least
	// 2 bounded deletes (2 + 2), then a terminating empty/partial batch. Every
	// delete must carry the bounded limit, never an unbounded delete.
	var eventsCalls int
	for _, call := range client.deleteCalls {
		if call.limit != 2 {
			t.Errorf("delete call %+v used limit %d, want bounded batch 2", call, call.limit)
		}
		if call.table == "wisp_events" && call.db == "store" {
			eventsCalls++
		}
	}
	if eventsCalls < 2 {
		t.Errorf("wisp_events cleanup issued %d bounded deletes, want >= 2 (4 orphans / batch 2)", eventsCalls)
	}
}

func TestScanWispOrphans_RecordsPerDatabaseErrors(t *testing.T) {
	client := newFakeWispClient()
	client.countErr = map[string]error{"store/wisp_events": fmt.Errorf("boom")}

	res := scanWispOrphans(context.Background(), client)
	if len(res.Errors) == 0 {
		t.Fatal("expected a recorded scan error for the failing count")
	}
	// The other tables still get counted: 3 + 2 + 1 = 6.
	if res.Total != 6 {
		t.Fatalf("Total = %d, want 6 (failing table excluded, others counted)", res.Total)
	}
}

func TestWispOrphanCheck_RunReportsAndFixCleans(t *testing.T) {
	client := newFakeWispClient()
	check := newWispOrphanCheck("/city", nil)
	check.newClient = func() (wispOrphanClient, error) { return client, nil }
	check.warnThreshold = 5 // 10 orphans > 5 → warning

	res := check.Run(&doctor.CheckContext{})
	if res.Status != doctor.StatusWarning {
		t.Fatalf("Run status = %v, want Warning", res.Status)
	}
	if res.Severity != doctor.SeverityAdvisory {
		t.Errorf("Run severity = %v, want Advisory (orphans are hygiene, not a gate)", res.Severity)
	}
	if !check.CanFix() {
		t.Error("CanFix() = false, want true when orphans exceed threshold")
	}
	if !strings.Contains(res.Message, "10") {
		t.Errorf("Run message %q should report the orphan total", res.Message)
	}

	var out strings.Builder
	if err := check.Fix(&doctor.CheckContext{Output: &out}); err != nil {
		t.Fatalf("Fix returned error: %v", err)
	}
	// Fix must emit Dolt-GC/maintenance guidance after large removals.
	if !strings.Contains(strings.ToLower(out.String()), "gc") {
		t.Errorf("Fix output %q should include maintenance/GC guidance", out.String())
	}

	// After the fix the check passes.
	after := check.Run(&doctor.CheckContext{})
	if after.Status != doctor.StatusOK {
		t.Errorf("post-fix Run status = %v, want OK", after.Status)
	}
}

func TestWispOrphanCheck_BelowThresholdIsOK(t *testing.T) {
	client := newFakeWispClient()
	check := newWispOrphanCheck("/city", nil)
	check.newClient = func() (wispOrphanClient, error) { return client, nil }
	check.warnThreshold = 100 // 10 orphans < 100 → OK, no fix

	res := check.Run(&doctor.CheckContext{})
	if res.Status != doctor.StatusOK {
		t.Fatalf("Run status = %v, want OK below threshold", res.Status)
	}
	if check.CanFix() {
		t.Error("CanFix() = true below threshold; cleanup should not trigger for trivial residue")
	}
}

func TestWispOrphanCheck_NoManagedDoltSkips(t *testing.T) {
	check := newWispOrphanCheck("/city", nil)
	check.newClient = func() (wispOrphanClient, error) { return nil, errWispOrphanNoManagedDolt }

	res := check.Run(&doctor.CheckContext{})
	if res.Status != doctor.StatusOK {
		t.Fatalf("Run status = %v, want OK when managed Dolt is not running", res.Status)
	}
	if check.CanFix() {
		t.Error("CanFix() = true with no managed Dolt; nothing to fix")
	}
}

func TestWispOrphanSQL_OwningIssueIDPredicateAndBoundedDelete(t *testing.T) {
	count := wispOrphanCountSQL("wisp_labels")
	if !strings.Contains(count, "`wisp_labels`") {
		t.Errorf("count SQL %q should target the backtick-quoted table", count)
	}
	if !strings.Contains(count, "issue_id NOT IN (SELECT id FROM `wisps`)") {
		t.Errorf("count SQL %q should key on the owning issue_id vs wisps.id", count)
	}

	del := wispOrphanDeleteSQL("wisp_events", 5000)
	if !strings.Contains(del, "issue_id NOT IN (SELECT id FROM `wisps`)") {
		t.Errorf("delete SQL %q should key on the owning issue_id vs wisps.id", del)
	}
	if !strings.Contains(del, "LIMIT 5000") {
		t.Errorf("delete SQL %q must be bounded by LIMIT so it never monopolizes Dolt", del)
	}
}

func TestWispOrphanTables_AreTheUnconstrainedAuxiliaryTables(t *testing.T) {
	// Lock the auxiliary-table set so a future edit can't silently drop one.
	want := []string{"wisp_comments", "wisp_dependencies", "wisp_events", "wisp_labels"}
	got := append([]string(nil), wispOrphanTables...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("wispOrphanTables = %v, want %v", got, want)
	}
}
