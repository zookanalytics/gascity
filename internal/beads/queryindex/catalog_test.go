package queryindex

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestNormalizeExpressionMatchesDoltRendering(t *testing.T) {
	ours := MetadataIndex("issues", "gc.root_bead_id").expression()
	// information_schema.statistics.EXPRESSION as Dolt 2.4.2 reports the
	// index CreateStatement builds.
	dolt := `(json_unquote(json_extract(metadata, '$."gc.root_bead_id"')))`
	if normalizeExpression(ours) != normalizeExpression(dolt) {
		t.Fatalf("normalized forms differ:\n  ours %q\n  dolt %q", normalizeExpression(ours), normalizeExpression(dolt))
	}
	// bd's interpolated literal escapes the double quotes; it names the same path.
	escaped := "JSON_UNQUOTE(JSON_EXTRACT(`metadata`, '$.\\\"gc.root_bead_id\\\"'))"
	if normalizeExpression(escaped) != normalizeExpression(ours) {
		t.Fatalf("escaped form %q normalizes to %q, want %q", escaped, normalizeExpression(escaped), normalizeExpression(ours))
	}
}

func TestNormalizeExpressionKeepsLiteralCase(t *testing.T) {
	lower := `json_unquote(json_extract(metadata, '$."anchor_bead"'))`
	upper := `json_unquote(json_extract(metadata, '$."ANCHOR_BEAD"'))`
	if normalizeExpression(lower) == normalizeExpression(upper) {
		t.Fatal("metadata keys are case-sensitive, but two keys differing only in case normalized alike")
	}
}

func TestNormalizeExpressionStripsOnlyEnclosingParentheses(t *testing.T) {
	// The outer pair of "(a) + (b)" does not enclose the whole expression.
	if got := normalizeExpression("(a) + (b)"); got != "(a)+(b)" {
		t.Fatalf("normalizeExpression = %q, want %q", got, "(a)+(b)")
	}
	if got := normalizeExpression("((a + b))"); got != "a+b" {
		t.Fatalf("normalizeExpression = %q, want %q", got, "a+b")
	}
}

func TestCatalogMissing(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	anchor := MetadataIndex("issues", "anchor_bead")
	wispRoot := MetadataIndex("wisps", "gc.root_bead_id")
	labels := ColumnIndex("wisp_labels", "idx_wisp_labels_issue_id", "issue_id")
	statusType := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	ghost := MetadataIndex("ghost_table", "gc.root_bead_id")

	cat := catalogFromRows(
		[]string{"issues", "WISPS", "wisp_labels"},
		[]statisticsRow{
			{table: "issues", index: "PRIMARY", seq: 1, column: nullString("id")},
			// An equivalent index under another name covers the key.
			{table: "issues", index: "operator_root_idx", seq: 1, expression: nullString(`(json_unquote(json_extract(metadata, '$."gc.root_bead_id"')))`)},
			// A composite primary key whose first column is issue_id does not
			// cover a single-column issue_id index.
			{table: "wisp_labels", index: "PRIMARY", seq: 1, column: nullString("issue_id")},
			{table: "wisp_labels", index: "PRIMARY", seq: 2, column: nullString("label")},
			// Rows arrive in any order; seq decides the key-part order.
			{table: "wisps", index: "idx_wisps_status_type", seq: 2, column: nullString("issue_type")},
			{table: "wisps", index: "idx_wisps_status_type", seq: 1, column: nullString("status")},
		},
	)
	got := cat.Missing([]Index{labels, statusType, root, wispRoot, anchor, ghost})
	want := []Index{labels, wispRoot, anchor}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Missing() = %v, want %v", got, want)
	}
}

func TestCatalogMissingColumnOrderMatters(t *testing.T) {
	cat := catalogFromRows(
		[]string{"wisps"},
		[]statisticsRow{
			{table: "wisps", index: "idx_type_status", seq: 1, column: nullString("issue_type")},
			{table: "wisps", index: "idx_type_status", seq: 2, column: nullString("status")},
		},
	)
	statusType := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	if got := cat.Missing([]Index{statusType}); len(got) != 1 {
		t.Fatalf("(issue_type, status) was taken to cover (status, issue_type): Missing() = %v", got)
	}
}

func TestIsNothingToCommit(t *testing.T) {
	if !isNothingToCommit(errString("Error 1105 (HY000): nothing to commit")) {
		t.Fatal("Dolt's nothing-to-commit error was not recognized")
	}
	if isNothingToCommit(errString("Error 1105 (HY000): merge conflict")) {
		t.Fatal("an unrelated commit error was taken for nothing-to-commit")
	}
	if isNothingToCommit(nil) {
		t.Fatal("nil error was taken for nothing-to-commit")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
