// Package queryindex keeps the secondary indexes bead queries rely on present
// on Dolt-backed bead stores.
//
// A metadata index is a functional index on the expression beads' metadata
// filter compares, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."<key>"')). bd
// interpolates the JSON path into its SQL as a literal, so Dolt plans a lookup
// by the key as an index range instead of reading the JSON of every row, which
// on a store of mostly closed beads is the whole table.
//
// Dolt builds an index in the time it takes to read every row, tens of seconds
// on a store of tens of thousands of beads. If the table takes a write during
// the build, committing the DDL merges that write under the database's branch
// lock for about as long again, and every write to the database waits behind
// it. A Maintainer therefore builds one index at a time, and only once the
// table has stopped changing.
package queryindex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// maxIdentifierLen is the longest index name MySQL and Dolt accept.
const maxIdentifierLen = 64

// Index is one secondary index on a bead-store table: either an index on
// columns, or a metadata index on one bead metadata key.
type Index struct {
	// Table is the indexed table.
	Table string
	// Name is the name the index is created with. Whether a store already
	// has the index is decided by its key parts, not by this name.
	Name string
	// Columns are the indexed columns in order. A metadata index has none.
	Columns []string
	// MetadataKey is the bead metadata key a metadata index covers.
	MetadataKey string
}

// SDKMetadataKeys are the bead metadata keys gascity's own queries look up
// with closed beads included, where no status index narrows the rows read.
var SDKMetadataKeys = []string{beadmeta.RootBeadIDMetadataKey}

// metadataTables are the bead tables a metadata index is built on.
var metadataTables = []string{"issues", "wisps"}

// columnIndexes are the column indexes gascity keeps on beads' ephemeral
// tables:
//   - wisp_labels(issue_id): bd's list hydration groups wisp labels by issue.
//   - wisps(status, issue_type): mail and message lookups filter on both.
var columnIndexes = []Index{
	ColumnIndex("wisp_labels", "idx_wisp_labels_issue_id", "issue_id"),
	ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type"),
}

// ColumnIndex returns an index named name on table's columns, in order.
func ColumnIndex(table, name string, columns ...string) Index {
	return Index{Table: table, Name: name, Columns: append([]string(nil), columns...)}
}

// MetadataIndex returns the metadata index on table for bead metadata key.
// It is named idx_<table>_meta_<slug>_<hash>: the slug keeps the name
// readable, and a hash of the exact key keeps keys that slug alike, such as
// gc.root_bead_id and gc_root_bead_id, on different names.
func MetadataIndex(table, key string) Index {
	sum := sha256.Sum256([]byte(key))
	suffix := "_" + hex.EncodeToString(sum[:4])
	prefix := "idx_" + table + "_meta_"
	slug := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, strings.ToLower(key))
	room := maxIdentifierLen - len(prefix) - len(suffix)
	if room < 0 {
		room = 0
	}
	if len(slug) > room {
		slug = slug[:room]
	}
	return Index{Table: table, Name: prefix + slug + suffix, MetadataKey: key}
}

// Expected returns the indexes a bead store carries: gascity's column
// indexes, then a metadata index on issues and on wisps for each SDK key and
// each of packKeys, in that order and without repeats. A key that is not a
// valid bead metadata key is an error: it could not appear in beads' filter,
// and it is spliced into the index expression.
func Expected(packKeys []string) ([]Index, error) {
	out := append([]Index(nil), columnIndexes...)
	seen := map[string]bool{}
	for _, key := range append(append([]string(nil), SDKMetadataKeys...), packKeys...) {
		if seen[key] {
			continue
		}
		if err := beadmeta.ValidateKey(key); err != nil {
			return nil, err
		}
		seen[key] = true
		for _, table := range metadataTables {
			out = append(out, MetadataIndex(table, key))
		}
	}
	return out, nil
}

// expression is the SQL a metadata index covers: beads' metadata filter
// predicate with the path as the literal bd sends.
func (ix Index) expression() string {
	return "JSON_UNQUOTE(JSON_EXTRACT(metadata, '" + beadmeta.JSONPath(ix.MetadataKey) + "'))"
}

// keyParts returns the index's key parts in the form Catalog compares.
func (ix Index) keyParts() []string {
	if ix.MetadataKey != "" {
		return []string{normalizeExpression(ix.expression())}
	}
	parts := make([]string, len(ix.Columns))
	for i, column := range ix.Columns {
		parts[i] = strings.ToLower(column)
	}
	return parts
}

// CreateStatement returns the DDL that builds the index. Dolt fails a single
// ALTER TABLE that adds several functional indexes, so each index is its own
// statement.
func (ix Index) CreateStatement() string {
	if ix.MetadataKey != "" {
		return fmt.Sprintf("CREATE INDEX IF NOT EXISTS `%s` ON `%s` ((%s))", ix.Name, ix.Table, ix.expression())
	}
	columns := make([]string, len(ix.Columns))
	for i, column := range ix.Columns {
		columns[i] = "`" + column + "`"
	}
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS `%s` ON `%s` (%s)", ix.Name, ix.Table, strings.Join(columns, ", "))
}

// String names the index by what it covers, for logs and reports.
func (ix Index) String() string {
	if ix.MetadataKey != "" {
		return ix.Table + "(metadata " + ix.MetadataKey + ")"
	}
	return ix.Table + "(" + strings.Join(ix.Columns, ", ") + ")"
}
