package beads

import (
	"context"
	"slices"
	"strings"
	"testing"

	beadslib "github.com/steveyegge/beads"
	"github.com/steveyegge/beads/issueops"
)

// listRecordingStorage records every reader-role List request so a test can
// assert the batch reads name their ids (IDFilter) and never list a tier.
type listRecordingStorage struct {
	*nativeDoltMemStorage
	lists []issueops.ListRequest
}

func (s *listRecordingStorage) IssueReader() (issueops.Reader, error) {
	inner, err := s.nativeDoltMemStorage.IssueReader()
	if err != nil {
		return nil, err
	}
	return &listRecordingReader{Reader: inner, storage: s}, nil
}

type listRecordingReader struct {
	issueops.Reader
	storage *listRecordingStorage
}

func (r *listRecordingReader) List(ctx context.Context, req issueops.ListRequest) (issueops.IssuePage, error) {
	r.storage.lists = append(r.storage.lists, req)
	return r.Reader.List(ctx, req)
}

func seedChildrenBatchFixture(t *testing.T) (*NativeDoltStore, *listRecordingStorage, map[string]Bead) {
	t.Helper()
	storage := &listRecordingStorage{nativeDoltMemStorage: newNativeDoltMemStorage()}
	store := newNativeDoltStoreForTest(storage)
	create := func(title, parent string) Bead {
		b, err := store.Create(Bead{Title: title, ParentID: parent})
		if err != nil {
			t.Fatalf("Create %s: %v", title, err)
		}
		return b
	}
	fx := map[string]Bead{}
	fx["a"] = create("parent a", "")
	fx["b"] = create("parent b", "")
	fx["a1"] = create("a child 1", fx["a"].ID)
	fx["a2"] = create("a child 2 (closed)", fx["a"].ID)
	fx["b1"] = create("b child 1", fx["b"].ID)
	fx["u"] = create("unrelated", "")
	if err := store.Close(fx["a2"].ID); err != nil {
		t.Fatalf("Close a2: %v", err)
	}
	storage.lists = nil
	return store, storage, fx
}

func batchIDsOf(beads []Bead) []string {
	out := make([]string, 0, len(beads))
	for _, b := range beads {
		out = append(out, b.ID)
	}
	return out
}

// Every List the batch reads issue names its ids; none lists a tier.
func assertEveryListIsExact(t *testing.T, storage *listRecordingStorage, maxCalls int) {
	t.Helper()
	if len(storage.lists) == 0 {
		t.Fatal("the batch read issued no reader List at all")
	}
	if len(storage.lists) > maxCalls {
		t.Fatalf("the batch read issued %d reader List calls, want at most %d", len(storage.lists), maxCalls)
	}
	for _, req := range storage.lists {
		if strings.TrimSpace(req.IDFilter) == "" {
			t.Fatalf("a batch read listed a tier without an IDFilter: %+v", req)
		}
		if req.Limit == nil || *req.Limit != 0 {
			t.Fatalf("a batch read's List did not spell an unlimited page: %+v", req)
		}
	}
}

func TestNativeDoltStoreChildrenOfAnyReadsEveryParentsChildrenByExactID(t *testing.T) {
	store, storage, fx := seedChildrenBatchFixture(t)

	got, err := store.ChildrenOfAny([]string{fx["a"].ID, fx["b"].ID, fx["b"].ID, ""})
	if err != nil {
		t.Fatalf("ChildrenOfAny: %v", err)
	}
	if want := []string{fx["a1"].ID, fx["a2"].ID, fx["b1"].ID}; !slices.Equal(batchIDsOf(got), want) {
		t.Fatalf("ChildrenOfAny = %v, want every child %v, closed included, in created order", batchIDsOf(got), want)
	}
	for _, child := range got {
		if child.ParentID == "" {
			t.Fatalf("child %s carries no ParentID; callers group by it", child.ID)
		}
		if !slices.ContainsFunc(child.Dependencies, func(d Dep) bool { return d.Type == string(beadslib.DepParentChild) && d.DependsOnID == child.ParentID }) {
			t.Fatalf("child %s lacks its parent-child edge: %+v", child.ID, child.Dependencies)
		}
	}
	assertEveryListIsExact(t, storage, 1)
}

func TestNativeDoltStoreChildrenOfAnyMatchesChildrenWithClosedIncluded(t *testing.T) {
	store, _, fx := seedChildrenBatchFixture(t)

	got, err := store.ChildrenOfAny([]string{fx["a"].ID, fx["b"].ID}, IncludeClosed)
	if err != nil {
		t.Fatalf("ChildrenOfAny(IncludeClosed): %v", err)
	}
	if want := []string{fx["a1"].ID, fx["a2"].ID, fx["b1"].ID}; !slices.Equal(batchIDsOf(got), want) {
		t.Fatalf("ChildrenOfAny(IncludeClosed) = %v, want %v", batchIDsOf(got), want)
	}
	// Per parent, the batch answers what Children(id, IncludeClosed) answers.
	for _, parent := range []string{fx["a"].ID, fx["b"].ID} {
		single, err := store.Children(parent, IncludeClosed)
		if err != nil {
			t.Fatalf("Children(%s): %v", parent, err)
		}
		var fromBatch []string
		for _, b := range got {
			if b.ParentID == parent {
				fromBatch = append(fromBatch, b.ID)
			}
		}
		if !slices.Equal(fromBatch, batchIDsOf(single)) {
			t.Fatalf("parent %s: batch %v, Children %v", parent, fromBatch, batchIDsOf(single))
		}
	}
}

func TestNativeDoltStoreChildrenOfAnyAnswersAbsentAndChildlessParentsWithNothing(t *testing.T) {
	store, storage, fx := seedChildrenBatchFixture(t)

	got, err := store.ChildrenOfAny([]string{"gc-missing", fx["u"].ID})
	if err != nil {
		t.Fatalf("ChildrenOfAny over an absent and a childless parent: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ChildrenOfAny = %v, want nothing", batchIDsOf(got))
	}
	if len(storage.lists) != 0 {
		t.Fatalf("with no children to read the batch still listed: %+v", storage.lists)
	}
	if got, err := store.ChildrenOfAny(nil); err != nil || len(got) != 0 {
		t.Fatalf("ChildrenOfAny(nil) = %v, %v; want nothing", got, err)
	}
}

func TestNativeDoltStoreGetExactBatchAnswersAsGetDoesAndListsOnlyTheIDs(t *testing.T) {
	store, storage, fx := seedChildrenBatchFixture(t)

	ids := []string{fx["a1"].ID, "gc-missing", fx["a2"].ID, fx["a1"].ID, fx["u"].ID}
	found, unresolved, err := store.GetExactBatch(ids)
	if err != nil {
		t.Fatalf("GetExactBatch: %v", err)
	}
	if want := []string{"gc-missing"}; !slices.Equal(unresolved, want) {
		t.Fatalf("unresolved = %v, want %v", unresolved, want)
	}
	if len(found) != 3 {
		t.Fatalf("found %d beads, want 3: %v", len(found), found)
	}
	for _, id := range []string{fx["a1"].ID, fx["a2"].ID, fx["u"].ID} {
		batch, ok := found[id]
		if !ok {
			t.Fatalf("GetExactBatch did not answer %s", id)
		}
		single, err := store.Get(id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if batch.ID != single.ID || batch.Title != single.Title || batch.Status != single.Status || batch.ParentID != single.ParentID {
			t.Fatalf("GetExactBatch row %+v differs from Get %+v", batch, single)
		}
		if len(batch.Dependencies) != len(single.Dependencies) {
			t.Fatalf("GetExactBatch row %s carries %d edges, Get carries %d", id, len(batch.Dependencies), len(single.Dependencies))
		}
	}
	assertEveryListIsExact(t, storage, 1)

	if found, unresolved, err := store.GetExactBatch(nil); err != nil || len(found) != 0 || unresolved != nil {
		t.Fatalf("GetExactBatch(nil) = %v, %v, %v; want empty", found, unresolved, err)
	}
}
