package beads

// The native store's keyed children read (gc-5k8udo, gc-cvl9w): the children
// of many parents in a bounded number of role requests instead of a whole-tier
// listing cut in Go or one request per bead.
//
// The children's rows ride GetExactBatch's keyed listing first, the one SQL
// statement a native (dolt or Postgres) IssueReader answers for any id count,
// and fall back to per-id Get only when the reader refuses that shape (an
// http-native IssueReader refuses IDFilter client-side, which GetExactBatch
// reports as ErrExactBatchGetUnsupported), the same cheap-first/fan-out rule
// filterReadyByWorkOutcome follows and for the same reason (#6491).

import (
	"context"
	"errors"
	"fmt"

	beadslib "github.com/steveyegge/beads"
	"github.com/steveyegge/beads/issueops"
)

var _ ChildrenBatchLister = (*NativeDoltStore)(nil)

// ChildrenOfAny reads the children of every parent: one Relations read per
// parent for the inbound parent-child edges (the edge rows name the children,
// so this never lists a tier), then the children's rows by exact id through
// childRows. It answers what Children(id, IncludeClosed, opts...) answers
// per parent, as ChildrenBatchLister promises: closed children included,
// deduplicated, in created order. molecule.ListSubtree passes no
// IncludeClosed and still reaches an open descendant through a closed
// intermediate. A parent this store holds no row for has no children here, as
// Children reports; a failed read is an error.
func (s *NativeDoltStore) ChildrenOfAny(parentIDs []string, opts ...QueryOpt) ([]Bead, error) {
	parents := distinctNonEmpty(parentIDs)
	if len(parents) == 0 {
		return nil, nil
	}
	query := ListQuery{
		IncludeClosed: true,
		TierMode:      TierModeFromOpts(opts),
		Sort:          SortCreatedAsc,
	}
	var ids []string
	err := s.withReadRetry(func(ctx context.Context, storage beadslib.Storage) error {
		ids = ids[:0]
		relations, err := storage.IssueRelations()
		if err != nil {
			return nativeStoreError(parents[0], err)
		}
		seen := make(map[string]bool)
		for _, parent := range parents {
			issues, err := relations.Related(ctx, issueops.RelatedRequest{ID: parent, Direction: issueops.RelationIn})
			if errors.Is(err, issueops.ErrNotFound) {
				continue // no row here: no children here, as Children answers
			}
			if err != nil {
				return nativeStoreError(parent, err)
			}
			for _, issue := range issues {
				if issue == nil || issue.ID == "" || issue.DependencyType != beadslib.DepParentChild || seen[issue.ID] {
					continue
				}
				seen[issue.ID] = true
				ids = append(ids, issue.ID)
			}
		}
		return nil
	})
	if err == nil && len(ids) > 0 {
		var rows map[string]Bead
		rows, err = s.childRows(ids)
		if err == nil {
			children := make([]Bead, 0, len(ids))
			for _, id := range ids {
				if b, ok := rows[id]; ok {
					children = append(children, b)
				}
			}
			return ApplyListQuery(children, query), nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("listing the children of %d parent(s) starting at %q: %w", len(parents), parents[0], err)
	}
	return nil, nil
}

// childRows reads ids by exact id through GetExactBatch, whose rows carry the
// dependency set Get gives them. A reader that refuses the keyed listing
// (http-native) is read one Get per id instead. An id the backend does not
// return is left out; a row whose metadata does not project is left out too,
// as a listing leaves it out.
func (s *NativeDoltStore) childRows(ids []string) (map[string]Bead, error) {
	rows, _, err := s.GetExactBatch(ids)
	if err == nil {
		return rows, nil
	}
	if !errors.Is(err, ErrExactBatchGetUnsupported) {
		return nil, err
	}
	rows = make(map[string]Bead, len(ids))
	for _, id := range ids {
		b, err := s.Get(id)
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrMetadataParse) {
			continue
		}
		if err != nil {
			return nil, err
		}
		rows[id] = b
	}
	return rows, nil
}
