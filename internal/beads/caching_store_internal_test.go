package beads

import (
	"context"
	"errors"
	"testing"
)

func TestCachingStoreRunReconciliationDetectsLabelContentChanges(t *testing.T) {
	t.Parallel()

	backing := NewMemStore()
	bead, err := backing.Create(Bead{Title: "Task", Labels: []string{"old"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	if err := backing.Update(bead.ID, UpdateOpts{
		Labels:       []string{"new"},
		RemoveLabels: []string{"old"},
	}); err != nil {
		t.Fatalf("Update backing: %v", err)
	}

	cache.runReconciliation()

	got, err := cache.Get(bead.ID)
	if err != nil {
		t.Fatalf("Get after reconcile: %v", err)
	}
	if len(got.Labels) != 1 || got.Labels[0] != "new" {
		t.Fatalf("Labels = %v, want [new]", got.Labels)
	}
}

func TestCachingStoreUpdateInvalidatesStaleCacheWhenRefreshFails(t *testing.T) {
	t.Parallel()

	backing := &refreshFailingStore{Store: NewMemStore()}
	bead, err := backing.Create(Bead{Title: "before"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	title := "after"
	backing.failNextGet = true
	if err := cache.Update(bead.ID, UpdateOpts{Title: &title}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := cache.Get(bead.ID)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got.Title != "after" {
		t.Fatalf("Title = %q, want after", got.Title)
	}
}

func TestCachingStoreDepListUpFallsThroughToBackingTruth(t *testing.T) {
	t.Parallel()

	backing := NewMemStore()
	root, err := backing.Create(Bead{Title: "root"})
	if err != nil {
		t.Fatalf("Create root: %v", err)
	}
	left, err := backing.Create(Bead{Title: "left"})
	if err != nil {
		t.Fatalf("Create left: %v", err)
	}
	right, err := backing.Create(Bead{Title: "right"})
	if err != nil {
		t.Fatalf("Create right: %v", err)
	}
	if err := backing.DepAdd(left.ID, root.ID, "blocks"); err != nil {
		t.Fatalf("DepAdd left: %v", err)
	}
	if err := backing.DepAdd(right.ID, root.ID, "blocks"); err != nil {
		t.Fatalf("DepAdd right: %v", err)
	}

	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	// Populate only one downward dep entry in the cache, leaving reverse lookups
	// incomplete unless they fall through to the backing store.
	if _, err := cache.DepList(left.ID, "down"); err != nil {
		t.Fatalf("DepList left down: %v", err)
	}

	deps, err := cache.DepList(root.ID, "up")
	if err != nil {
		t.Fatalf("DepList root up: %v", err)
	}
	if len(deps) != 2 {
		t.Fatalf("DepList(root, up) = %d deps, want 2", len(deps))
	}
}

type refreshFailingStore struct {
	Store
	failNextGet bool
}

func (s *refreshFailingStore) Get(id string) (Bead, error) {
	if s.failNextGet {
		s.failNextGet = false
		return Bead{}, errors.New("transient get failure")
	}
	return s.Store.Get(id)
}
