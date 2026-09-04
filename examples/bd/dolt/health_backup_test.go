// Package dolt_test validates the backup-freshness block of the dolt pack's
// health command. The block used to measure the migration-backup-* rollback
// snapshot — an artifact family this city never produces — so dolt_stale could
// never go true and every consumer read clean through a total backup outage
// (gc-ny33h). These tests pin the corrected contract: the check measures the
// real scheduled destinations ($GC_BACKUP_ARTIFACT_DIR/<db>, default
// .dolt-backup/<db>), verifies each by manifest-must-be-newest ordering rather
// than age alone, and reports a three-state dolt_stale so an unmeasured
// destination renders null, never false.
package dolt_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// backupDB mirrors one entry of the backups.databases array.
type backupDB struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	AgeSec int    `json:"age_sec"`
}

// backupsReport mirrors the health report's backups object. DoltStale is a
// pointer so a JSON null (the "could not measure" state) is distinguishable
// from false.
type backupsReport struct {
	DoltFreshness string     `json:"dolt_freshness"`
	DoltAgeSec    int        `json:"dolt_age_sec"`
	DoltStale     *bool      `json:"dolt_stale"`
	Databases     []backupDB `json:"databases"`
}

// backupFixture is a city laid out for a backup-block test: a Dolt data dir
// holding live databases and a separate backup artifact dir the test seeds with
// destinations and controlled mtimes.
type backupFixture struct {
	cityPath  string
	dataDir   string
	backupDir string
	root      string
	extraEnv  []string
}

// newBackupFixture builds the city, .beads metadata, an empty data dir, and an
// empty backup artifact dir. Callers add databases with addDatabase and seed
// destinations with seedManifest/seedChunk before running.
func newBackupFixture(t *testing.T) *backupFixture {
	t.Helper()
	cityPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityPath, ".beads"), 0o755); err != nil {
		t.Fatalf("mkdir .beads: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, ".beads", "metadata.json"),
		[]byte(`{"database":"dolt","backend":"dolt","dolt_database":"hq"}`), 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	return &backupFixture{
		cityPath:  cityPath,
		dataDir:   t.TempDir(),
		backupDir: t.TempDir(),
		root:      repoRoot(t),
	}
}

// addDatabase creates an on-disk Dolt database directory so the backup
// enumeration (a data-dir scan) discovers it.
func (f *backupFixture) addDatabase(t *testing.T, name string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(f.dataDir, name, ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir db %s: %v", name, err)
	}
}

// destDir returns the backup destination directory for a database, creating it.
func (f *backupFixture) destDir(t *testing.T, name string) string {
	t.Helper()
	d := filepath.Join(f.backupDir, name)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatalf("mkdir dest %s: %v", name, err)
	}
	return d
}

// seedFile writes a file into a database's destination with an mtime of
// now-ageSec, so ordering between manifest and chunks is deterministic.
func (f *backupFixture) seedFile(t *testing.T, db, filename string, ageSec int) {
	t.Helper()
	p := filepath.Join(f.destDir(t, db), filename)
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	mt := time.Now().Add(-time.Duration(ageSec) * time.Second)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatalf("chtimes %s: %v", p, err)
	}
}

// run invokes health.sh --json against the fixture with the server probes
// stubbed to report unreachable, and returns the parsed backups object. The
// backup check is deliberately independent of server reachability, so a downed
// server (the moment a backup matters most) must not suppress it.
func (f *backupFixture) run(t *testing.T) backupsReport {
	t.Helper()
	binDir := t.TempDir()
	// Stub every server-probe helper to fail fast: no live server, no delay.
	for _, name := range []string{"gc", "lsof", "nc", "dolt"} {
		writeExecutable(t, filepath.Join(binDir, name), "#!/bin/sh\nexit 1\n")
	}

	env := append(filteredEnv(
		"GC_CITY_PATH", "GC_PACK_DIR", "GC_DOLT_DATA_DIR",
		"GC_DOLT_HOST", "GC_DOLT_PORT", "GC_DOLT_USER", "GC_DOLT_PASSWORD",
		"GC_HEALTH_SKIP_ZOMBIE_SCAN", "GC_BACKUP_ARTIFACT_DIR",
		"GC_DOLT_BACKUP_GRACE_SEC", "GC_DOLT_BACKUP_STALE_H", "GC_BACKUP_DATABASES",
		"PATH",
	),
		"GC_CITY_PATH="+f.cityPath,
		"GC_PACK_DIR="+f.root,
		"GC_DOLT_DATA_DIR="+f.dataDir,
		"GC_DOLT_HOST=127.0.0.1",
		"GC_DOLT_PORT=3306",
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
		"GC_HEALTH_SKIP_ZOMBIE_SCAN=1",
		"GC_BACKUP_ARTIFACT_DIR="+f.backupDir,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	env = append(env, f.extraEnv...)

	cmd := exec.Command("sh", filepath.Join(f.root, healthScript), "--json")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("health.sh --json failed: %v\n%s", err, out)
	}
	var report struct {
		Backups backupsReport `json:"backups"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("parse health JSON: %v\n%s", err, out)
	}
	return report.Backups
}

// findDB returns the per-database entry for name, or fails.
func findDB(t *testing.T, dbs []backupDB, name string) backupDB {
	t.Helper()
	for _, d := range dbs {
		if d.Name == name {
			return d
		}
	}
	t.Fatalf("database %q absent from backups.databases: %+v", name, dbs)
	return backupDB{}
}

// TestHealthBackupsHealthyManifestNewest: a destination whose manifest is the
// newest file (chunks older) is OK, and the aggregate dolt_stale is false.
func TestHealthBackupsHealthyManifestNewest(t *testing.T) {
	f := newBackupFixture(t)
	f.addDatabase(t, "hq")
	f.seedFile(t, "hq", "abc123.darc", 3600) // chunk written an hour ago
	f.seedFile(t, "hq", "manifest", 60)      // manifest committed after it

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != false {
		t.Fatalf("dolt_stale = %v, want false for a healthy manifest-newest destination", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "ok" {
		t.Fatalf("hq state = %q, want ok", got.State)
	}
}

// TestHealthBackupsTornSyncIsStale: a chunk strictly newer than the manifest,
// past the in-flight grace, is a torn sync — uploaded but never committed — and
// must flag stale. An age-only probe cannot see this.
func TestHealthBackupsTornSyncIsStale(t *testing.T) {
	f := newBackupFixture(t)
	f.extraEnv = []string{"GC_DOLT_BACKUP_GRACE_SEC=0"} // no in-flight grace
	f.addDatabase(t, "hq")
	f.seedFile(t, "hq", "manifest", 300)     // manifest committed 5m ago
	f.seedFile(t, "hq", "newchunk.darc", 60) // a chunk landed AFTER it

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != true {
		t.Fatalf("dolt_stale = %v, want true for a torn sync (chunk newer than manifest)", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "torn" {
		t.Fatalf("hq state = %q, want torn", got.State)
	}
}

// TestHealthBackupsInFlightIsUnknown: a chunk newer than the manifest but
// within the grace window is an in-flight sync, not a torn one — reported
// unknown, so the aggregate is null (never false).
func TestHealthBackupsInFlightIsUnknown(t *testing.T) {
	f := newBackupFixture(t)
	f.extraEnv = []string{"GC_DOLT_BACKUP_GRACE_SEC=900"}
	f.addDatabase(t, "hq")
	f.seedFile(t, "hq", "manifest", 120)     // manifest 2m ago
	f.seedFile(t, "hq", "newchunk.darc", 30) // chunk 30s ago — mid-sync

	b := f.run(t)
	if b.DoltStale != nil {
		t.Fatalf("dolt_stale = %v, want null for an in-flight sync", *b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "unknown" {
		t.Fatalf("hq state = %q, want unknown", got.State)
	}
}

// TestHealthBackupsStaleManifest: a manifest that is the newest file but older
// than 2x the backup cadence flags stale.
func TestHealthBackupsStaleManifest(t *testing.T) {
	f := newBackupFixture(t)
	f.extraEnv = []string{"GC_DOLT_BACKUP_STALE_H=12"}
	f.addDatabase(t, "hq")
	f.seedFile(t, "hq", "oldchunk.darc", 14*3600) // chunk 14h ago
	f.seedFile(t, "hq", "manifest", 13*3600)      // manifest newest but 13h old

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != true {
		t.Fatalf("dolt_stale = %v, want true for a manifest older than 2x cadence", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "stale" {
		t.Fatalf("hq state = %q, want stale", got.State)
	}
}

// TestHealthBackupsMissingDestinationIsStale: a live database with no backup
// destination at all was never backed up — a known-bad state, not a clean one.
func TestHealthBackupsMissingDestinationIsStale(t *testing.T) {
	f := newBackupFixture(t)
	f.addDatabase(t, "hq") // on disk, but no .dolt-backup/hq seeded

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != true {
		t.Fatalf("dolt_stale = %v, want true for a database with no backup destination", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "missing" {
		t.Fatalf("hq state = %q, want missing", got.State)
	}
}

// TestHealthBackupsNoDatabasesIsNull: with no local databases there is nothing
// to measure, so the aggregate is null (could-not-measure), never false. This
// pins the core regression: an unmeasured backup must not read clean.
func TestHealthBackupsNoDatabasesIsNull(t *testing.T) {
	f := newBackupFixture(t) // empty data dir

	b := f.run(t)
	if b.DoltStale != nil {
		t.Fatalf("dolt_stale = %v, want null when no databases are measurable", *b.DoltStale)
	}
	if len(b.Databases) != 0 {
		t.Fatalf("backups.databases = %+v, want empty", b.Databases)
	}
}

// TestHealthBackupsIgnoresMigrationBackupSnapshot is the direct regression
// guard for gc-ny33h: a fresh migration-backup-* rollback snapshot must NOT
// make a database whose real destination is missing read as clean. The old
// block measured migration-backup-* and reported dolt_stale=false through
// exactly this state.
func TestHealthBackupsIgnoresMigrationBackupSnapshot(t *testing.T) {
	f := newBackupFixture(t)
	f.addDatabase(t, "hq") // real destination .dolt-backup/hq is absent

	// A fresh rollback snapshot the old probe would have read as a backup.
	snap := filepath.Join(f.cityPath, "migration-backup-"+strconv.Itoa(int(time.Now().Unix())))
	if err := os.MkdirAll(snap, 0o755); err != nil {
		t.Fatalf("mkdir migration-backup: %v", err)
	}

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != true {
		t.Fatalf("dolt_stale = %v, want true: a fresh migration-backup-* must not mask a missing real destination", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "missing" {
		t.Fatalf("hq state = %q, want missing", got.State)
	}
}

// TestHealthBackupsAggregateFlagsWorstDatabase: the aggregate is the worst
// verdict across databases. One healthy destination alongside one that was
// never backed up must read stale, and each database keeps its own per-entry
// state.
func TestHealthBackupsAggregateFlagsWorstDatabase(t *testing.T) {
	f := newBackupFixture(t)
	// "gco" is healthy (manifest newest); "hq" has no destination at all.
	f.addDatabase(t, "gco")
	f.addDatabase(t, "hq")
	f.seedFile(t, "gco", "abc123.darc", 3600)
	f.seedFile(t, "gco", "manifest", 60)

	b := f.run(t)
	if b.DoltStale == nil || *b.DoltStale != true {
		t.Fatalf("dolt_stale = %v, want true when any database is unbacked", b.DoltStale)
	}
	if got := findDB(t, b.Databases, "gco"); got.State != "ok" {
		t.Fatalf("gco state = %q, want ok", got.State)
	}
	if got := findDB(t, b.Databases, "hq"); got.State != "missing" {
		t.Fatalf("hq state = %q, want missing", got.State)
	}
}
