package beads

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// bdListIDFlagMaxBytes bounds the value of the one --id argument a bd list
// read sends. Linux refuses a single argument longer than 128 KiB
// (MAX_ARG_STRLEN), and the bound keeps half of that as margin: about 6,000
// ids of ten bytes.
const bdListIDFlagMaxBytes = 64 << 10

// bdListIDFilter returns the --id value that selects ids in bd list: the
// distinct non-empty ids, sorted so the argv does not depend on the caller's
// order, joined with commas. ok is false when the list cannot be sent that
// way. bd splits the value on commas and trims each id, so an id holding a
// comma or edge whitespace would reach bd as a different id and its bead would
// be lost, and a value over bdListIDFlagMaxBytes may not survive exec. Such a
// list is read without --id and matched in Go.
func bdListIDFilter(ids []string) (string, bool) {
	seen := make(map[string]bool, len(ids))
	distinct := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		if strings.Contains(id, ",") || strings.TrimSpace(id) != id {
			return "", false
		}
		seen[id] = true
		distinct = append(distinct, id)
	}
	if len(distinct) == 0 {
		return "", false
	}
	sort.Strings(distinct)
	value := strings.Join(distinct, ",")
	if len(value) > bdListIDFlagMaxBytes {
		return "", false
	}
	return value, true
}

// namesNoBead reports whether an IDs selector can match nothing: every entry
// is empty, and no bead holds the empty id.
func namesNoBead(ids []string) bool {
	for _, id := range ids {
		if id != "" {
			return false
		}
	}
	return len(ids) > 0
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
	parents := listedParentIDs(query)
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
	if err != nil && !IsPartialResult(err) {
		return nil, err
	}
	return ApplyListQuery(keepChildrenOf(rows, parents), query), err
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
// --type=parent-child names every parent. Every supported bd answers it with
// the dependent issues, read from both dependency tables, so wisp children are
// named too.
//
// A lone parent is spelled twice. bd reads two or more ids in batch mode, which
// skips an id it cannot resolve, where a single id it cannot resolve fails the
// command. Spelled twice, a parent bd cannot find has no children, as in a
// batch. bd may list the children of a repeated id once per spelling, so the
// ids are deduplicated.
func (s *BdStore) parentChildIDs(parents []string) ([]string, error) {
	args := append([]string{"dep", "list"}, parents...)
	if len(parents) == 1 {
		args = append(args, parents[0])
	}
	args = append(args, "--direction=up", "--type=parent-child", "--json")
	out, err := s.runBDTransientRead(args...)
	if err != nil {
		return nil, err
	}
	data := extractJSON(out)
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	var rows []struct {
		ID             string `json:"id"`
		DependencyType string `json:"dependency_type"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("bd dep list: parsing JSON: raw=%q: %w", truncateRawOutput(data, 200), err)
	}
	s.noteServerRows(len(rows))
	seen := make(map[string]bool, len(rows))
	children := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.DependencyType != "parent-child" || row.ID == "" || seen[row.ID] {
			continue
		}
		seen[row.ID] = true
		children = append(children, row.ID)
	}
	sort.Strings(children)
	return children, nil
}
