package beads

// The native store's keyed batch reads (gc-5k8udo, gc-cvl9w): the children of
// many parents and many beads by exact id, each in a bounded number of role
// requests instead of a whole-tier listing cut in Go or one request per bead.
//
// Both ride the reader role's List(IDFilter) first, the one SQL statement a
// native (dolt or Postgres) IssueReader answers for any id count, and fall back
// to per-id Get only when the reader refuses that shape (an http-native
// IssueReader refuses IDFilter client-side with a typed ErrUnsupported), the
// same cheap-first/fan-out rule filterReadyByWorkOutcome follows and for the
// same reason (#6491). Edges come from EdgeReader in the chunks DepListBatch
// uses, so a batch row carries the dependency set Get would give it.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	beadslib "github.com/steveyegge/beads"
	"github.com/steveyegge/beads/issueops"
)

var (
	_ ChildrenBatchLister = (*NativeDoltStore)(nil)
	_ ExactBatchGetter    = (*NativeDoltStore)(nil)
)

// nativeExactReadChunkSize bounds how many ids one List(IDFilter) or ReadEdges
// call carries; it is the server-enforced anchor cap this package already
// relies on (see nativeReadyVetoListChunkSize and nativeDepListBatchChunk).
const nativeExactReadChunkSize = nativeServerEdgeAnchorCap

// ChildrenOfAny reads the children of every parent: one Relations read per
// parent for the inbound parent-child edges (the edge rows name the children,
// so this never lists a tier), then the children's rows by exact id through
// readExactRows. It answers what Children(id, opts...) answers per parent,
// deduplicated, in created order. A parent this store holds no row for has no
// children here, as Children reports; a failed read is an error.
func (s *NativeDoltStore) ChildrenOfAny(parentIDs []string, opts ...QueryOpt) ([]Bead, error) {
	parents := distinctNonEmpty(parentIDs)
	if len(parents) == 0 {
		return nil, nil
	}
	query := ListQuery{
		IncludeClosed: HasOpt(opts, IncludeClosed),
		TierMode:      TierModeFromOpts(opts),
		Sort:          SortCreatedAsc,
	}
	var out []Bead
	err := s.withReadRetry(func(ctx context.Context, storage beadslib.Storage) error {
		relations, err := storage.IssueRelations()
		if err != nil {
			return nativeStoreError(parents[0], err)
		}
		var ids []string
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
		rows, err := s.readExactRows(ctx, storage, ids)
		if err != nil {
			return err
		}
		children := make([]Bead, 0, len(ids))
		for _, id := range ids {
			if b, ok := rows[id]; ok {
				children = append(children, b)
			}
		}
		out = ApplyListQuery(children, query)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing the children of %d parent(s) starting at %q: %w", len(parents), parents[0], err)
	}
	return out, nil
}

// GetExactBatch reads ids by exact id, the way Get reads one, in chunked
// List(IDFilter) calls (or per-id Get where the reader refuses IDFilter). Every
// requested id it did not answer is listed in unresolved, in input order: the
// bead is absent, or its row does not project, which Get reports as
// ErrMetadataParse. The caller resolves those with Get.
func (s *NativeDoltStore) GetExactBatch(ids []string) (map[string]Bead, []string, error) {
	distinct := distinctNonEmpty(ids)
	found := make(map[string]Bead, len(distinct))
	if len(distinct) == 0 {
		return found, nil, nil
	}
	err := s.withReadRetry(func(ctx context.Context, storage beadslib.Storage) error {
		rows, err := s.readExactRows(ctx, storage, distinct)
		if err != nil {
			return err
		}
		clear(found)
		for id, b := range rows {
			found[id] = b
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("getting %d beads starting at %q: %w", len(distinct), distinct[0], err)
	}
	var unresolved []string
	for _, id := range ids {
		if _, ok := found[id]; !ok && id != "" {
			unresolved = append(unresolved, id)
		}
	}
	return found, unresolved, nil
}

// readExactRows reads ids by exact id inside one read attempt: chunked
// List(IDFilter) through the reader role, then the rows' edges through
// EdgeReader so each bead carries the dependency set Get gives it. A reader that
// refuses IDFilter (http-native) is read one Get per id instead, which already
// carries the edges. An id the backend does not return is left out; a row whose
// metadata does not project is left out too, as List leaves it out.
func (s *NativeDoltStore) readExactRows(ctx context.Context, storage beadslib.Storage, ids []string) (map[string]Bead, error) {
	rows := make(map[string]Bead, len(ids))
	if len(ids) == 0 {
		return rows, nil
	}
	reader, err := storage.IssueReader()
	if err != nil {
		return nil, nativeStoreError(ids[0], err)
	}
	unlimited := 0
	for start := 0; start < len(ids); start += nativeExactReadChunkSize {
		chunk := ids[start:min(start+nativeExactReadChunkSize, len(ids))]
		req := nativeListReadRequest()
		req.IDFilter = strings.Join(chunk, ",")
		req.Limit = &unlimited
		page, err := reader.List(ctx, req)
		if err != nil {
			var unsupported *beadslib.ErrUnsupported
			if !errors.As(err, &unsupported) {
				return nil, nativeStoreError(chunk[0], err)
			}
			return s.readExactRowsByGet(ctx, storage, reader, ids)
		}
		s.noteRows(len(page.Items))
		for _, row := range page.Items {
			bead, err := beadFromNativeIssueRow(row)
			if err != nil {
				if isNativeIssueMetadataParseError(err) {
					continue
				}
				return nil, err
			}
			if bead.ID != "" {
				rows[bead.ID] = bead
			}
		}
	}
	// The listing's rows carry a parent, not the edge set; Get's rows do.
	edgeReader, err := storage.EdgeReader()
	if err != nil {
		return nil, nativeStoreError(ids[0], err)
	}
	for start := 0; start < len(ids); start += nativeExactReadChunkSize {
		chunk := ids[start:min(start+nativeExactReadChunkSize, len(ids))]
		result, err := edgeReader.ReadEdges(ctx, issueops.EdgeReadRequest{IDs: chunk})
		if err != nil {
			return nil, nativeStoreError(chunk[0], err)
		}
		for _, anchor := range result.Anchors {
			bead, ok := rows[anchor.ID]
			if !ok {
				continue
			}
			if anchor.Missing {
				delete(rows, anchor.ID) // deleted between the two reads
				continue
			}
			bead.Dependencies = anchorDeps(anchor.ID, anchor)
			rows[anchor.ID] = bead
		}
	}
	return rows, nil
}

// readExactRowsByGet is readExactRows' fallback for a reader that refuses
// List(IDFilter): one Get per id, each with its edges, a miss left out.
func (s *NativeDoltStore) readExactRowsByGet(ctx context.Context, storage beadslib.Storage, reader issueops.Reader, ids []string) (map[string]Bead, error) {
	rows := make(map[string]Bead, len(ids))
	for _, id := range ids {
		details, err := reader.Get(ctx, issueops.GetRequest{ID: id})
		if errors.Is(err, issueops.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, nativeStoreError(id, err)
		}
		if details == nil {
			continue
		}
		edges, err := readAnchorEdges(ctx, storage, id)
		if err != nil {
			return nil, err
		}
		if edges.Missing {
			continue
		}
		bead, err := beadFromNativeIssueDetails(details, edges)
		if err != nil {
			if isNativeIssueMetadataParseError(err) {
				continue
			}
			return nil, err
		}
		rows[id] = bead
	}
	return rows, nil
}
