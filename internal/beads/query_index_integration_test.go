//go:build integration

package beads

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	beadslib "github.com/steveyegge/beads"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads/queryindex"
)

// queryIndexServer is a throwaway Dolt server holding beads' real schema.
type queryIndexServer struct {
	port      int
	scopeRoot string
	queryLog  string
	db        *sql.DB
}

func startQueryIndexServer(t *testing.T) queryIndexServer {
	t.Helper()
	ctx := context.Background()
	queryLog := filepath.Join(t.TempDir(), "dolt.log")
	port := startTestDoltServer(t, testDoltServerOptions{queryLog: queryLog})
	scopeRoot := t.TempDir()
	beadsDir := filepath.Join(scopeRoot, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatalf("create .beads directory: %v", err)
	}
	metadata := fmt.Sprintf(`{"backend":"dolt","database":"beads","dolt_mode":"server","dolt_server_host":"127.0.0.1","dolt_server_port":%d}`, port)
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"), []byte(metadata), 0o644); err != nil {
		t.Fatalf("write metadata.json: %v", err)
	}
	storage, err := beadslib.OpenBestAvailable(ctx, beadsDir)
	if err != nil {
		t.Skipf("upstream native beads storage unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Fatalf("close upstream storage: %v", err)
		}
	})
	if err := storage.SetConfig(ctx, "issue_prefix", "gc"); err != nil {
		t.Fatalf("set issue prefix: %v", err)
	}
	accessor, ok := storage.(testRawDBGetter)
	if !ok {
		t.Skip("storage does not expose a raw DB")
	}
	return queryIndexServer{port: port, scopeRoot: scopeRoot, queryLog: queryLog, db: accessor.DB()}
}

// dedicated opens the kind of handle the controller builds indexes on.
func (s queryIndexServer) dedicated(context.Context) (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/beads?readTimeout=10m", s.port))
}

// TestQueryIndexSelfTestPredictsBeadsWritePath checks the self-test against
// beads' real schema on this server: the controller builds metadata indexes
// exactly when the self-test passes, and when it fails, the column change a
// beads schema migration makes really does break bd's aliased UPDATE.
func TestQueryIndexSelfTestPredictsBeadsWritePath(t *testing.T) {
	ctx := context.Background()
	server := startQueryIndexServer(t)
	store := queryindex.SQLStore("test", server.db, server.dedicated)
	before := readSelfTestFootprint(ctx, t, server.db)
	verdict, err := store.SelfTest(ctx)
	if err != nil {
		t.Fatalf("SelfTest: %v", err)
	}
	version, err := queryindex.DoltVersion(ctx, server.db)
	if err != nil {
		t.Fatalf("DoltVersion: %v", err)
	}
	t.Logf("Dolt %s self-test: %+v", version, verdict)
	assertSelfTestLeftNoTables(ctx, t, server.db, before)
	assertSelfTestSerializes(ctx, t, server, store)

	want, err := queryindex.Expected([]string{"anchor_bead"})
	if err != nil {
		t.Fatalf("Expected: %v", err)
	}
	var passLog []string
	maintainer := &queryindex.Maintainer{
		Quiet: queryindex.QuietOptions{QuietFor: 200 * time.Millisecond, Poll: 50 * time.Millisecond, MaxWait: 30 * time.Second},
		Logf: func(format string, args ...any) {
			passLog = append(passLog, fmt.Sprintf(format, args...))
		},
	}
	if err := maintainer.Pass(ctx, []queryindex.Store{store}, want); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	cat, err := queryindex.Inspect(ctx, server.db, want)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	for _, ix := range cat.Missing(want) {
		if ix.MetadataKey == "" || verdict.OK {
			t.Errorf("after a pass %s is missing (self-test OK: %v)\npass log:\n%s", ix, verdict.OK, strings.Join(passLog, "\n"))
		}
	}
	if !verdict.OK {
		for _, ix := range want {
			if ix.MetadataKey != "" && len(cat.Missing([]queryindex.Index{ix})) == 0 {
				t.Errorf("a pass built %s on a server that failed the self-test", ix)
			}
		}
	} else {
		// The pass built every metadata index; bead writes must still land.
		native, err := newNativeDoltStoreAt(ctx, server.scopeRoot, nil)
		if err != nil {
			t.Fatalf("newNativeDoltStoreAt: %v", err)
		}
		created, err := native.Create(Bead{Title: "write after the metadata indexes", Metadata: StringMap{"anchor_bead": "gc-anchor"}})
		if err != nil {
			t.Fatalf("the self-test passed, yet a bead create fails with the indexes built: %v", err)
		}
		if err := native.SetMetadata(created.ID, beadmeta.RootBeadIDMetadataKey, "gc-root"); err != nil {
			t.Fatalf("the self-test passed, yet a metadata update fails with the indexes built: %v", err)
		}
		if err := native.Close(created.ID); err != nil {
			t.Fatalf("the self-test passed, yet a close fails with the indexes built: %v", err)
		}
		got, err := native.Get(created.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Title != created.Title || got.Metadata["anchor_bead"] != "gc-anchor" || got.Metadata[beadmeta.RootBeadIDMetadataKey] != "gc-root" {
			t.Fatalf("the self-test passed, yet the bead reads back changed: %+v", got)
		}
	}

	// Replay the column change beads migration 0049 makes on a table that
	// carries a metadata index, then plan the aliased UPDATE bd issues on
	// every create.
	root := queryindex.MetadataIndex("issues", beadmeta.RootBeadIDMetadataKey)
	if !verdict.OK {
		if err := queryindex.Create(ctx, server.db, root); err != nil {
			t.Logf("this server cannot build %s at all (%v); the self-test reported: %s", root, err, verdict.Failure)
			return
		}
	}
	if _, err := server.db.ExecContext(ctx, "ALTER TABLE issues MODIFY COLUMN close_reason TEXT DEFAULT ''"); err != nil {
		t.Fatalf("replay the column change: %v", err)
	}
	_, err = server.db.ExecContext(ctx, "EXPLAIN PLAN UPDATE issues i SET i.is_blocked = i.is_blocked WHERE 1 = 0")
	switch {
	case err != nil && verdict.OK:
		t.Fatalf("the self-test passed, yet after the column change bd's aliased UPDATE fails: %v", err)
	case err != nil && !strings.Contains(err.Error(), "table not found"):
		t.Fatalf("bd's aliased UPDATE fails after the column change in a way the self-test does not replay: %v", err)
	case err == nil && !verdict.OK:
		t.Logf("bd's aliased UPDATE survived the column change on this server; the self-test failed on: %s", verdict.Failure)
	}
}

// selfTestLock mirrors the server-wide user lock queryindex.SelfTest holds.
const selfTestLock = "gascity.queryindex.selftest"

// assertSelfTestSerializes checks that a finished self-test released its lock,
// and that a self-test which cannot take the lock returns no verdict.
func assertSelfTestSerializes(ctx context.Context, t *testing.T, server queryIndexServer, store queryindex.Store) {
	t.Helper()
	db := server.db
	var free []string
	if err := scanStrings(ctx, db, "SELECT IS_FREE_LOCK('"+selfTestLock+"')", &free); err != nil {
		t.Fatalf("IS_FREE_LOCK: %v", err)
	}
	if len(free) != 1 || free[0] != "1" {
		t.Fatalf("the self-test left its lock held: IS_FREE_LOCK = %q", free)
	}
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open a connection to hold the lock: %v", err)
	}
	defer holder.Close() //nolint:errcheck // closing the connection also releases the lock
	var got int
	if err := holder.QueryRowContext(ctx, "SELECT GET_LOCK('"+selfTestLock+"', 0)").Scan(&got); err != nil || got != 1 {
		t.Fatalf("hold the self-test lock: got %d, err %v", got, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if verdict, err := store.SelfTest(waitCtx); err == nil {
		t.Fatalf("a self-test ran while another connection held its lock: %+v", verdict)
	}
	if err := holder.QueryRowContext(ctx, "SELECT RELEASE_LOCK('"+selfTestLock+"')").Scan(&got); err != nil || got != 1 {
		t.Fatalf("release the self-test lock: got %d, err %v", got, err)
	}
	// The abandoned self-test's GET_LOCK may still be waiting on the server
	// and take the lock as it frees. Its connection is gone, so the server
	// must free the lock again: a GET_LOCK that outwaits the abandoned one
	// gets it. The wait runs on a handle whose read deadline outlasts it.
	waiter, err := server.dedicated(ctx)
	if err != nil {
		t.Fatalf("open a handle to wait for the lock: %v", err)
	}
	defer waiter.Close() //nolint:errcheck // closing the handle also releases the lock
	waitConn, err := waiter.Conn(ctx)
	if err != nil {
		t.Fatalf("open a connection to wait for the lock: %v", err)
	}
	defer waitConn.Close() //nolint:errcheck // closing the connection also releases the lock
	var acquired sql.NullInt64
	if err := waitConn.QueryRowContext(ctx, "SELECT GET_LOCK('"+selfTestLock+"', 30)").Scan(&acquired); err != nil || !acquired.Valid || acquired.Int64 != 1 {
		t.Fatalf("the self-test lock stayed held after its connection went away: GET_LOCK = %v, err %v", acquired, err)
	}
	if err := waitConn.QueryRowContext(ctx, "SELECT RELEASE_LOCK('"+selfTestLock+"')").Scan(&got); err != nil || got != 1 {
		t.Fatalf("release the self-test lock: got %d, err %v", got, err)
	}
}

// selfTestFootprint is what the self-test may change in a store: its tables
// and its dolt_ignore patterns.
type selfTestFootprint struct {
	tables   map[string]bool
	patterns map[string]bool
}

func readSelfTestFootprint(ctx context.Context, t *testing.T, db *sql.DB) selfTestFootprint {
	t.Helper()
	var tables, patterns []string
	if err := scanStrings(ctx, db, "SHOW TABLES", &tables); err != nil {
		t.Fatalf("SHOW TABLES: %v", err)
	}
	if err := scanStrings(ctx, db, "SELECT pattern FROM dolt_ignore", &patterns); err != nil {
		t.Fatalf("read dolt_ignore: %v", err)
	}
	fp := selfTestFootprint{tables: map[string]bool{}, patterns: map[string]bool{}}
	for _, name := range tables {
		fp.tables[name] = true
	}
	for _, pattern := range patterns {
		fp.patterns[pattern] = true
	}
	return fp
}

// assertSelfTestLeftNoTables checks that the self-test dropped every table it
// created, and that the dolt_ignore pattern it registered keeps a table under
// it out of the working-set status and out of a commit that stages every
// table, as bd's commits do.
func assertSelfTestLeftNoTables(ctx context.Context, t *testing.T, db *sql.DB, before selfTestFootprint) {
	t.Helper()
	after := readSelfTestFootprint(ctx, t, db)
	for name := range after.tables {
		if !before.tables[name] {
			t.Errorf("the self-test left table %s behind", name)
		}
	}
	var added []string
	for pattern := range after.patterns {
		if !before.patterns[pattern] {
			added = append(added, pattern)
		}
	}
	if len(added) != 1 || !strings.HasSuffix(added[0], "*") {
		t.Fatalf("the self-test added dolt_ignore patterns %q, want one prefix pattern", added)
	}
	probe := strings.TrimSuffix(added[0], "*") + "_check"
	if _, err := db.ExecContext(ctx, "CREATE TABLE `"+probe+"` (id int PRIMARY KEY)"); err != nil {
		t.Fatalf("create %s: %v", probe, err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+probe+"`"); err != nil {
			t.Errorf("drop %s: %v", probe, err)
		}
	}()
	var status []string
	if err := scanStrings(ctx, db, "SELECT table_name FROM dolt_status", &status); err != nil {
		t.Fatalf("read dolt_status: %v", err)
	}
	if slices.Contains(status, probe) {
		t.Fatalf("dolt_status lists %s although %s ignores it", probe, added[0])
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'test: stage every table', '--allow-empty', '--author', 'test <test@example.com>')"); err != nil {
		t.Fatalf("DOLT_COMMIT -Am: %v", err)
	}
	var committed []string
	if err := scanStrings(ctx, db, "SELECT table_name FROM dolt_diff WHERE commit_hash = HASHOF('HEAD')", &committed); err != nil {
		t.Fatalf("read dolt_diff: %v", err)
	}
	if slices.Contains(committed, probe) {
		t.Fatalf("a commit that stages every table recorded %s although %s ignores it", probe, added[0])
	}
}

// TestQueryIndexesServeBeadsMetadataFilter pins the metadata index text to the
// SQL beads sends. The server's debug log records every query verbatim, so the
// test builds the metadata indexes, makes metadata lookups through the native
// store, reads the exact text of each lookup back from the log, and asks Dolt
// to plan that text. Every plan must read the index, not the table. The
// indexes are built directly, whatever the self-test says, because the pin is
// about the index text, not about this server's write path.
func TestQueryIndexesServeBeadsMetadataFilter(t *testing.T) {
	ctx := context.Background()
	server := startQueryIndexServer(t)
	db := server.db

	const packKey = "anchor_bead"
	want, err := queryindex.Expected([]string{packKey})
	if err != nil {
		t.Fatalf("Expected: %v", err)
	}
	for _, ix := range want {
		if err := queryindex.Create(ctx, db, ix); err != nil {
			if ix.MetadataKey != "" {
				t.Skipf("this Dolt server cannot build a metadata index on beads' tables: %v", err)
			}
			t.Fatalf("Create %s: %v", ix, err)
		}
		if err := queryindex.CommitSchema(ctx, db, ix.Table, "test: add "+ix.Name); err != nil {
			t.Fatalf("CommitSchema %s: %v", ix, err)
		}
	}
	// Dolt reports each functional index's expression in its own rendering;
	// the catalog finding nothing missing proves the two spellings compare
	// equal.
	cat, err := queryindex.Inspect(ctx, db, want)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if missing := cat.Missing(want); len(missing) > 0 {
		t.Fatalf("Inspect does not see built indexes: %v", missing)
	}
	var commits int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_log WHERE message LIKE 'test: add idx_issues_meta_%'").Scan(&commits); err != nil {
		t.Fatalf("read dolt_log: %v", err)
	}
	if commits == 0 {
		t.Fatal("no metadata index build was recorded in the Dolt history")
	}

	native, err := newNativeDoltStoreAt(ctx, server.scopeRoot, nil)
	if err != nil {
		t.Fatalf("newNativeDoltStoreAt: %v", err)
	}
	root, err := native.Create(Bead{Title: "workflow root"})
	if err != nil {
		t.Skipf("this Dolt server rejects bead writes once a metadata index exists: %v", err)
	}
	member, err := native.Create(Bead{Title: "closed workflow member"})
	if err != nil {
		t.Fatalf("Create member: %v", err)
	}
	for key, value := range map[string]string{beadmeta.RootBeadIDMetadataKey: root.ID, packKey: root.ID} {
		if err := native.SetMetadata(member.ID, key, value); err != nil {
			t.Fatalf("SetMetadata %s: %v", key, err)
		}
	}
	if err := native.Close(member.ID); err != nil {
		t.Fatalf("Close member: %v", err)
	}
	wisp, err := native.Create(Bead{Title: "wisp workflow member", Type: "message", Assignee: "builder", Ephemeral: true})
	if err != nil {
		t.Fatalf("Create wisp: %v", err)
	}
	if err := native.SetMetadata(wisp.ID, beadmeta.RootBeadIDMetadataKey, root.ID); err != nil {
		t.Fatalf("SetMetadata on wisp: %v", err)
	}

	lookups := []struct {
		name  string
		table string
		key   string
		query ListQuery
		want  string
	}{
		{"closed issue by gc.root_bead_id", "issues", beadmeta.RootBeadIDMetadataKey, ListQuery{Metadata: map[string]string{beadmeta.RootBeadIDMetadataKey: root.ID}, IncludeClosed: true}, member.ID},
		{"closed issue by a pack key", "issues", packKey, ListQuery{Metadata: map[string]string{packKey: root.ID}, IncludeClosed: true}, member.ID},
		{"wisp by gc.root_bead_id", "wisps", beadmeta.RootBeadIDMetadataKey, ListQuery{TierMode: TierWisps, Metadata: map[string]string{beadmeta.RootBeadIDMetadataKey: root.ID}, IncludeClosed: true}, wisp.ID},
	}
	for _, lookup := range lookups {
		// The lookup's statements are the ones the server logs while it runs.
		offset := fileSize(t, server.queryLog)
		got, err := native.List(lookup.query)
		if err != nil {
			t.Fatalf("%s: List: %v", lookup.name, err)
		}
		if !containsBeadID(got, lookup.want) {
			t.Fatalf("%s: List returned %d beads without %s", lookup.name, len(got), lookup.want)
		}
		ix := queryindex.MetadataIndex(lookup.table, lookup.key)
		keyIndexes := []string{queryindex.MetadataIndex("issues", lookup.key).Name, queryindex.MetadataIndex("wisps", lookup.key).Name}
		read := false
		for _, query := range loggedQueries(t, server.queryLog, offset) {
			if !strings.Contains(query, "JSON_EXTRACT(metadata") || !strings.Contains(query, lookup.key) {
				continue
			}
			plan := explainPlan(ctx, t, db, query)
			if strings.Contains(plan, "!hidden!"+ix.Name+"!") {
				read = true
			}
			if !strings.Contains(plan, "!hidden!"+keyIndexes[0]+"!") && !strings.Contains(plan, "!hidden!"+keyIndexes[1]+"!") {
				t.Errorf("%s: a statement filtering on %s reads no index for it.\nquery:\n%s\nplan:\n%s", lookup.name, lookup.key, query, plan)
			}
		}
		if !read {
			t.Errorf("%s: no statement the lookup sent read %s", lookup.name, ix.Name)
		}
	}

	// gascity's own direct SQL keeps the id lookup and the gc.root_bead_id
	// lookup in separate UNION arms (internal/api/convoy_sql.go). Each arm
	// reads an index.
	rootPredicate := "JSON_UNQUOTE(JSON_EXTRACT(i.metadata, '" + beadmeta.JSONPath(beadmeta.RootBeadIDMetadataKey) + "')) = " + sqlQuote(root.ID)
	union := "SELECT i.id FROM issues i WHERE i.id = " + sqlQuote(root.ID) + " UNION ALL SELECT i.id FROM issues i WHERE " + rootPredicate
	plan := explainPlan(ctx, t, db, union)
	if scansTable(plan) || !strings.Contains(plan, "!hidden!"+queryindex.MetadataIndex("issues", beadmeta.RootBeadIDMetadataKey).Name+"!") {
		t.Errorf("an arm of the UNION ALL lookup reads the whole table:\n%s", plan)
	}
}

var loggedQueryRe = regexp.MustCompile(`query="((?:[^"\\]|\\.)*)"`)

// loggedQueries returns the distinct query texts a debug-level Dolt server
// logged past byte offset of its log, in first-seen order.
func loggedQueries(t *testing.T, path string, offset int64) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open dolt query log: %v", err)
	}
	defer f.Close() //nolint:errcheck // read-only test file
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		t.Fatalf("seek dolt query log: %v", err)
	}
	seen := map[string]bool{}
	var out []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for scanner.Scan() {
		for _, m := range loggedQueryRe.FindAllStringSubmatch(scanner.Text(), -1) {
			query, err := strconv.Unquote(`"` + m[1] + `"`)
			if err != nil || seen[query] {
				continue
			}
			seen[query] = true
			out = append(out, query)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read dolt query log: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("dolt query log %s holds no query= fields past offset %d; the server's debug log format may have changed", path, offset)
	}
	return out
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func explainPlan(ctx context.Context, t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var lines []string
	if err := scanStrings(ctx, db, "EXPLAIN PLAN "+query, &lines); err != nil {
		t.Fatalf("EXPLAIN PLAN: %v\nquery:\n%s", err, query)
	}
	return strings.Join(lines, "\n") + "\n"
}

func scanStrings(ctx context.Context, db *sql.DB, query string, out *[]string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck // read-only rows; Err below reports failures
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		*out = append(*out, s)
	}
	return rows.Err()
}

// scansTable reports whether a Dolt plan reads a table without an index: its
// full-scan node is a line ending in "Table".
func scansTable(plan string) bool {
	for _, line := range strings.Split(plan, "\n") {
		if strings.HasSuffix(strings.TrimSpace(line), "Table") {
			return true
		}
	}
	return false
}

func containsBeadID(list []Bead, id string) bool {
	for _, b := range list {
		if b.ID == id {
			return true
		}
	}
	return false
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
