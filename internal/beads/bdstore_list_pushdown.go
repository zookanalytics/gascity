package beads

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// bdListIDFlagMaxBytes bounds the value of one --id argument and the length of
// one quoted id. Linux refuses a single argument longer than 128 KiB
// (MAX_ARG_STRLEN), and the bound keeps half of that as margin: about 6,000
// ids of ten bytes in one --id value.
const bdListIDFlagMaxBytes = 64 << 10

// bdIDReads returns the reads that select ids, given listArgs, the bd list
// read the query's other filters spell. Every id is read by id, so the reads
// are bounded by the ids however many there are.
//
// bd list splits an --id value on commas and trims each id. The ids that
// survive that unchanged are sorted, so the argv does not depend on the
// caller's order, and sent in --id values of at most bdListIDFlagMaxBytes,
// one bd list read per value. An id holding a comma or edge whitespace is read
// alone through bd query, whose quoted id= clause selects it exactly from
// either table; includeClosed adds --all to that read, as it does to the bd
// list read. ok is false when an id fits neither read: bd's query lexer reads
// one byte at a time, so a NUL or non-ASCII byte reaches the comparison
// altered, and an id over the bound may not survive exec. The caller then
// reads every row and matches the ids in Go.
func bdIDReads(listArgs, ids []string, includeClosed bool) (reads [][]string, ok bool) {
	var listed []string
	for _, id := range distinctNonEmpty(ids) {
		switch {
		case len(id) > bdListIDFlagMaxBytes:
			return nil, false
		case !strings.Contains(id, ",") && strings.TrimSpace(id) == id:
			listed = append(listed, id)
		case bdQueryCanQuoteID(id):
			read := []string{"query", "--json", "id=" + quoteBdQueryValue(id)}
			if includeClosed {
				read = append(read, "--all")
			}
			reads = append(reads, append(read, "--limit", "0"))
		default:
			return nil, false
		}
	}
	sort.Strings(listed)
	byValue := make([][]string, 0, 1)
	for start := 0; start < len(listed); {
		end, size := start, 0
		for end < len(listed) && (end == start || size+1+len(listed[end]) <= bdListIDFlagMaxBytes) {
			if end > start {
				size++
			}
			size += len(listed[end])
			end++
		}
		read := slices.Clip(listArgs)
		byValue = append(byValue, append(read, "--id="+strings.Join(listed[start:end], ",")))
		start = end
	}
	return append(byValue, reads...), true
}

// bdQueryCanQuoteID reports whether quoteBdQueryString carries id to bd's query
// comparison unchanged: no NUL and no non-ASCII byte.
func bdQueryCanQuoteID(id string) bool {
	for i := 0; i < len(id); i++ {
		if id[i] == 0 || id[i] >= 0x80 {
			return false
		}
	}
	return true
}

// listChildren answers a query scoped to ParentIDs with the beads whose
// ParentID is one of the listed parents, matched against the rest of the query.
//
// bd list --parent takes a single parent. So the children are found from the
// parent-child edges that target the parents, which one bd dep list reads for
// every parent (parentChildIDs), and only those children are then read, by id,
// through the query's tiers.
//
// The answer is exact. A child is kept only when its ParentID, which bd derives
// from the child's own parent-child edge, is one of the parents, and the page
// is cut after that match. An empty entry names no parent, and a ParentID set
// alongside narrows the parents to itself. On a backend that cannot read
// edges, every row is read instead and kept to the same parents.
func (s *BdStore) listChildren(query ListQuery) ([]Bead, error) {
	parents := distinctNonEmpty(query.ParentIDs)
	if query.ParentID != "" {
		if !slices.Contains(parents, query.ParentID) {
			return []Bead{}, nil
		}
		parents = []string{query.ParentID}
	}
	if len(parents) == 0 {
		return []Bead{}, nil
	}
	sort.Strings(parents)
	childQuery := query
	childQuery.ParentIDs = nil
	childQuery.Limit = 0
	children, err := s.parentChildIDs(parents)
	switch {
	case err == nil:
		children = keepIDsIn(children, query.IDs)
		if len(children) == 0 {
			return []Bead{}, nil
		}
		childQuery.ParentID = ""
		childQuery.IDs = children
	case !isBdOperationUnsupported(err):
		return nil, fmt.Errorf("bd list: reading the parent-child edges of %d parent(s): %w", len(parents), err)
	}
	rows, err := s.listByTier(childQuery)
	var partial *PartialResultError
	if err != nil && !errors.As(err, &partial) {
		return nil, fmt.Errorf("bd list: reading the children of %d parent(s): %w", len(parents), err)
	}
	kept := ApplyListQuery(rows, query)
	if partial == nil {
		return kept, nil
	}
	// A partial read that kept no child of these parents is no answer about
	// them, the way listViaBDList treats a partial read that matched nothing.
	if len(kept) == 0 {
		return nil, fmt.Errorf("bd list: reading the children of %d parent(s): the read was partial and kept no child: %w", len(parents), partial.Err)
	}
	return kept, fmt.Errorf("bd list: reading the children of %d parent(s): %w", len(parents), err)
}

// keepIDsIn returns the ids that also appear in within, or every id when
// within is empty.
func keepIDsIn(ids, within []string) []string {
	if len(within) == 0 {
		return ids
	}
	allowed := make(map[string]bool, len(within))
	for _, id := range within {
		allowed[id] = true
	}
	kept := ids[:0]
	for _, id := range ids {
		if allowed[id] {
			kept = append(kept, id)
		}
	}
	return kept
}

// parentChildIDs returns the ids of the beads holding a parent-child edge to
// one of parents, distinct and sorted. One bd dep list --direction=up
// --type=parent-child names every parent, spelled by bdDepListBatchArgs so that
// a parent bd cannot find has no children rather than failing the read. Every
// supported bd answers it with the dependent issues, read from both dependency
// tables, so wisp children are named too. bd may list a child once per
// spelling of a repeated parent, and once per listed parent it holds an edge
// to, so the ids are deduplicated.
func (s *BdStore) parentChildIDs(parents []string) ([]string, error) {
	args, _ := bdDepListBatchArgs(parents, "--direction=up", "--type=parent-child")
	out, err := s.runBDTransientRead(args...)
	if err != nil {
		return nil, err
	}
	data := extractJSON(out)
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	var rows []bdDepIssue
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("bd dep list: parsing JSON: raw=%q: %w", truncateRawOutput(data, 200), err)
	}
	s.noteServerRows(len(rows))
	children := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.DepType == "parent-child" {
			children = append(children, row.ID)
		}
	}
	children = distinctNonEmpty(children)
	sort.Strings(children)
	return children, nil
}
