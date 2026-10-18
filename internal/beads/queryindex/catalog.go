package queryindex

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Querier runs a read on a database connection. *sql.DB and *sql.Conn
// satisfy it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Catalog is what a database holds of the tables a set of indexes names: which
// of them exist, and the key parts of every index on each.
type Catalog struct {
	tables  map[string]bool
	indexes map[string][][]string
}

// statisticsRow is one key part of one index, as information_schema.statistics
// reports it. A functional key part has no column and carries an expression.
type statisticsRow struct {
	table      string
	index      string
	seq        int
	column     sql.NullString
	expression sql.NullString
}

// Inspect reads, from the connection's current database, which tables want
// names exist and the indexes they carry.
func Inspect(ctx context.Context, db Querier, want []Index) (Catalog, error) {
	tables := tableNames(want)
	if len(tables) == 0 {
		return catalogFromRows(nil, nil), nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(tables)), ", ")
	args := make([]any, len(tables))
	for i, table := range tables {
		args[i] = table
	}

	present, err := queryStrings(ctx, db,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ("+placeholders+")", args...)
	if err != nil {
		return Catalog{}, fmt.Errorf("reading tables: %w", err)
	}

	rows, err := db.QueryContext(ctx,
		"SELECT table_name, index_name, seq_in_index, column_name, expression FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name IN ("+placeholders+")", args...)
	if err != nil {
		return Catalog{}, fmt.Errorf("reading indexes: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only rows; Err below reports failures
	var stats []statisticsRow
	for rows.Next() {
		var r statisticsRow
		if err := rows.Scan(&r.table, &r.index, &r.seq, &r.column, &r.expression); err != nil {
			return Catalog{}, fmt.Errorf("reading indexes: %w", err)
		}
		stats = append(stats, r)
	}
	if err := rows.Err(); err != nil {
		return Catalog{}, fmt.Errorf("reading indexes: %w", err)
	}
	return catalogFromRows(present, stats), nil
}

// catalogFromRows assembles a Catalog from the tables present and their
// statistics rows, in any order.
func catalogFromRows(tables []string, stats []statisticsRow) Catalog {
	cat := Catalog{tables: map[string]bool{}, indexes: map[string][][]string{}}
	for _, table := range tables {
		cat.tables[strings.ToLower(table)] = true
	}
	type indexKey struct{ table, index string }
	parts := map[indexKey][]statisticsRow{}
	var order []indexKey
	for _, r := range stats {
		k := indexKey{strings.ToLower(r.table), r.index}
		if _, ok := parts[k]; !ok {
			order = append(order, k)
		}
		parts[k] = append(parts[k], r)
	}
	for _, k := range order {
		rs := parts[k]
		sort.Slice(rs, func(i, j int) bool { return rs[i].seq < rs[j].seq })
		key := make([]string, len(rs))
		for i, r := range rs {
			if r.column.Valid && r.column.String != "" {
				key[i] = strings.ToLower(r.column.String)
			} else {
				key[i] = normalizeExpression(r.expression.String)
			}
		}
		cat.indexes[k.table] = append(cat.indexes[k.table], key)
	}
	return cat
}

// Missing returns the indexes in want the catalog does not cover, in want's
// order. An index is covered when an index on its table has the same key
// parts in the same order, whatever its name, so an equivalent index that
// beads or an operator created is not built a second time. An index on a
// table the database lacks is left out: there is nothing to index yet.
func (c Catalog) Missing(want []Index) []Index {
	var out []Index
	for _, ix := range want {
		table := strings.ToLower(ix.Table)
		if !c.tables[table] {
			continue
		}
		if !c.covers(table, ix.keyParts()) {
			out = append(out, ix)
		}
	}
	return out
}

func (c Catalog) covers(table string, key []string) bool {
	for _, existing := range c.indexes[table] {
		if slices.Equal(existing, key) {
			return true
		}
	}
	return false
}

func tableNames(indexes []Index) []string {
	seen := map[string]bool{}
	var out []string
	for _, ix := range indexes {
		if !seen[ix.Table] {
			seen[ix.Table] = true
			out = append(out, ix.Table)
		}
	}
	return out
}

func queryStrings(ctx context.Context, db Querier, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // read-only rows; Err below reports failures
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// normalizeExpression puts an index expression in a form that compares equal
// for every spelling of the same expression: outside string literals, case,
// whitespace and backticks drop out; a literal keeps its exact content with
// MySQL escapes resolved, because metadata keys are case-sensitive; and
// parentheses that enclose the whole expression are removed.
func normalizeExpression(expr string) string {
	var b strings.Builder
	runes := []rune(strings.TrimSpace(expr))
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '\'', '"':
			quote := r
			var lit strings.Builder
			for i++; i < len(runes); i++ {
				c := runes[i]
				if c == '\\' && i+1 < len(runes) {
					i++
					lit.WriteRune(runes[i])
					continue
				}
				if c == quote {
					if i+1 < len(runes) && runes[i+1] == quote {
						i++
						lit.WriteRune(quote)
						continue
					}
					break
				}
				lit.WriteRune(c)
			}
			b.WriteRune('\'')
			b.WriteString(strings.ReplaceAll(lit.String(), "'", "''"))
			b.WriteRune('\'')
		case '`', ' ', '\t', '\n', '\r':
		default:
			b.WriteString(strings.ToLower(string(r)))
		}
	}
	out := b.String()
	for enclosedInParens(out) {
		out = out[1 : len(out)-1]
	}
	return out
}

// enclosedInParens reports whether s's first parenthesis closes at its last
// character, outside string literals. normalizeExpression writes every
// literal single-quoted with quotes doubled, which is all it has to skip.
func enclosedInParens(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	inLiteral := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inLiteral {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inLiteral = false
			}
			continue
		}
		switch c {
		case '\'':
			inLiteral = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}
