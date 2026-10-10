package beads

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"
)

// exactBatchBacking is a backing store with the exact batch read. It records
// each batch read and Get it answers; err fails every batch read.
type exactBatchBacking struct {
	Store
	batches [][]string
	gets    []string
	err     error
}

func (s *exactBatchBacking) Get(id string) (Bead, error) {
	s.gets = append(s.gets, id)
	return s.Store.Get(id)
}

func (s *exactBatchBacking) GetExactBatch(ids []string) (map[string]Bead, []string, error) {
	s.batches = append(s.batches, append([]string(nil), ids...))
	if s.err != nil {
		return nil, nil, s.err
	}
	found := make(map[string]Bead, len(ids))
	var unresolved []string
	for _, id := range ids {
		b, err := s.Store.Get(id)
		if err != nil {
			unresolved = append(unresolved, id)
			continue
		}
		found[id] = b
	}
	return found, unresolved, nil
}

// TestCachingStoreGetExactBatchAnswersAsGetDoes pins the cached exact batch
// read: each id is answered from where Get answers it. A cached row costs no
// backing read, the ids Get reads straight from the backing store share one
// backing batch read, and an id Get must refresh or reports deleted is left to
// Get.
func TestCachingStoreGetExactBatchAnswersAsGetDoes(t *testing.T) {
	seed := func(t *testing.T, backing Store) (*CachingStore, map[string]string) {
		t.Helper()
		mem := backing
		if b, ok := backing.(*exactBatchBacking); ok {
			mem = b.Store
		}
		ids := map[string]string{}
		for _, title := range []string{"open", "dirty", "deleted", "closed"} {
			b, err := mem.Create(Bead{Title: title})
			if err != nil {
				t.Fatalf("Create(%s): %v", title, err)
			}
			ids[title] = b.ID
		}
		if err := mem.Close(ids["closed"]); err != nil {
			t.Fatalf("Close: %v", err)
		}
		cs := NewCachingStoreForTest(backing, nil)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		cs.mu.Lock()
		cs.dirty[ids["dirty"]] = struct{}{}
		cs.deletedSeq[ids["deleted"]] = cs.mutationSeq + 1
		cs.mu.Unlock()
		return cs, ids
	}

	t.Run("cached rows and backing rows", func(t *testing.T) {
		backing := &exactBatchBacking{Store: NewMemStore()}
		cs, ids := seed(t, backing)
		if _, cached := cs.beads[ids["closed"]]; cached {
			t.Fatal("the closed bead is cached after Prime; the fixture has no backing-only row")
		}
		backing.batches, backing.gets = nil, nil

		request := []string{ids["open"], ids["closed"], "gc-404", ids["dirty"], ids["deleted"], ids["open"]}
		found, unresolved, err := cs.GetExactBatch(request)
		if err != nil {
			t.Fatalf("GetExactBatch: %v", err)
		}
		if got := slices.Sorted(maps.Keys(found)); !reflect.DeepEqual(got, slices.Sorted(slices.Values([]string{ids["open"], ids["closed"]}))) {
			t.Fatalf("found = %v, want the cached open row and the backing's closed row", got)
		}
		if want := []string{"gc-404", ids["dirty"], ids["deleted"]}; !reflect.DeepEqual(unresolved, want) {
			t.Fatalf("unresolved = %v, want %v", unresolved, want)
		}
		if want := [][]string{{ids["closed"], "gc-404"}}; !reflect.DeepEqual(backing.batches, want) {
			t.Fatalf("backing batch reads = %v, want one read of the rows Get takes from the backing store %v", backing.batches, want)
		}
		if len(backing.gets) != 0 {
			t.Fatalf("backing Get calls = %v, want none", backing.gets)
		}
		for id, b := range found {
			single, err := cs.Get(id)
			if err != nil || !reflect.DeepEqual(single, b) {
				t.Fatalf("Get(%s) = %+v, %v; want the batch's bead %+v", id, single, err, b)
			}
		}
		if _, err := cs.Get(ids["deleted"]); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(deleted) err = %v, want ErrNotFound for the id the batch left unresolved", err)
		}
	})

	t.Run("a cached batch needs no backing read", func(t *testing.T) {
		backing := &exactBatchBacking{Store: NewMemStore()}
		cs, ids := seed(t, backing)
		other, err := cs.Create(Bead{Title: "another open row"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		backing.batches, backing.gets = nil, nil
		found, unresolved, err := cs.GetExactBatch([]string{ids["open"], other.ID})
		if err != nil || len(unresolved) != 0 || len(found) != 2 {
			t.Fatalf("GetExactBatch = %v, %v, %v; want both rows from the cache", found, unresolved, err)
		}
		if len(backing.batches) != 0 || len(backing.gets) != 0 {
			t.Fatalf("backing reads = batches %v, gets %v; want none", backing.batches, backing.gets)
		}
	})

	t.Run("a backing store without the batch read", func(t *testing.T) {
		cs, ids := seed(t, NewMemStore())
		found, unresolved, err := cs.GetExactBatch([]string{ids["open"], ids["closed"]})
		if err != nil {
			t.Fatalf("GetExactBatch: %v", err)
		}
		if _, ok := found[ids["open"]]; !ok || len(found) != 1 {
			t.Fatalf("found = %v, want only the cached row", slices.Collect(maps.Keys(found)))
		}
		if want := []string{ids["closed"]}; !reflect.DeepEqual(unresolved, want) {
			t.Fatalf("unresolved = %v, want the backing row left to Get %v", unresolved, want)
		}
	})

	t.Run("a failed backing batch read", func(t *testing.T) {
		backing := &exactBatchBacking{Store: NewMemStore()}
		cs, ids := seed(t, backing)
		backing.err = errors.New("dolt: connection refused")
		if _, _, err := cs.GetExactBatch([]string{ids["open"], ids["closed"]}); err == nil || !errors.Is(err, backing.err) {
			t.Fatalf("GetExactBatch err = %v, want the backing failure", err)
		}
	})
}
