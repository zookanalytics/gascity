package queryindex

import (
	"reflect"
	"strings"
	"testing"
)

func TestMetadataIndexStatementMatchesBeadsPredicate(t *testing.T) {
	ix := MetadataIndex("issues", "gc.root_bead_id")
	want := "CREATE INDEX IF NOT EXISTS `" + ix.Name + "` ON `issues` ((JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.\"gc.root_bead_id\"'))))"
	if got := ix.CreateStatement(); got != want {
		t.Fatalf("CreateStatement() =\n  %s\nwant\n  %s", got, want)
	}
}

func TestColumnIndexStatement(t *testing.T) {
	ix := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	want := "CREATE INDEX IF NOT EXISTS `idx_wisps_status_type` ON `wisps` (`status`, `issue_type`)"
	if got := ix.CreateStatement(); got != want {
		t.Fatalf("CreateStatement() = %s, want %s", got, want)
	}
}

func TestMetadataIndexName(t *testing.T) {
	a := MetadataIndex("issues", "gc.root_bead_id")
	if !strings.HasPrefix(a.Name, "idx_issues_meta_gc_root_bead_id_") {
		t.Fatalf("name %q does not carry the table and key slug", a.Name)
	}
	if again := MetadataIndex("issues", "gc.root_bead_id"); again.Name != a.Name {
		t.Fatalf("name is not deterministic: %q then %q", a.Name, again.Name)
	}
	// Keys that slug alike still get different names, or the second key's
	// CREATE INDEX IF NOT EXISTS would find the first key's index and stop.
	if b := MetadataIndex("issues", "gc_root_bead_id"); b.Name == a.Name {
		t.Fatalf("keys gc.root_bead_id and gc_root_bead_id share name %q", a.Name)
	}
	if w := MetadataIndex("wisps", "gc.root_bead_id"); w.Name == a.Name {
		t.Fatalf("issues and wisps share name %q", a.Name)
	}
}

func TestMetadataIndexNameFitsIdentifierLimit(t *testing.T) {
	long := "a" + strings.Repeat("very.long/key_", 20)
	for _, table := range []string{"issues", "wisps"} {
		ix := MetadataIndex(table, long)
		if len(ix.Name) > maxIdentifierLen {
			t.Fatalf("name %q is %d bytes, over the %d-byte identifier limit", ix.Name, len(ix.Name), maxIdentifierLen)
		}
		if other := MetadataIndex(table, long+"x"); other.Name == ix.Name {
			t.Fatalf("truncated names collide: %q", ix.Name)
		}
	}
}

func TestExpectedOrdersSDKKeysFirstAndCoversBothTables(t *testing.T) {
	got, err := Expected([]string{"anchor_bead", "gc.root_bead_id", "branch", "anchor_bead"})
	if err != nil {
		t.Fatalf("Expected: %v", err)
	}
	var names []string
	for _, ix := range got {
		names = append(names, ix.String())
	}
	want := []string{
		"wisp_labels(issue_id)",
		"wisps(status, issue_type)",
		"issues(metadata gc.root_bead_id)",
		"wisps(metadata gc.root_bead_id)",
		"issues(metadata anchor_bead)",
		"wisps(metadata anchor_bead)",
		"issues(metadata branch)",
		"wisps(metadata branch)",
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("Expected() =\n  %v\nwant\n  %v", names, want)
	}
}

func TestExpectedRejectsInvalidKey(t *testing.T) {
	if _, err := Expected([]string{"anchor_bead", "x') OR 1=1 -- "}); err == nil {
		t.Fatal("Expected accepted a key that would break out of the JSON path literal")
	}
}
