package operation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMemStoreCreateAndGet(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	op := Operation{ID: "op-1", Kind: "workspace.create", Phase: Pending, CreatedAt: time.Now()}
	created, err := s.Create(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "op-1" {
		t.Errorf("ID = %q, want %q", created.ID, "op-1")
	}

	got, err := s.Get(ctx, "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "workspace.create" {
		t.Errorf("Kind = %q, want %q", got.Kind, "workspace.create")
	}
}

func TestMemStoreGetNotFound(t *testing.T) {
	s := NewMemStore()
	_, err := s.Get(context.Background(), "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestMemStoreCreateDuplicate(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	op := Operation{ID: "op-1", Kind: "test", Phase: Pending, CreatedAt: time.Now()}
	if _, err := s.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, op); err == nil {
		t.Error("duplicate Create should return error")
	}
}

func TestMemStoreCreateMissingID(t *testing.T) {
	s := NewMemStore()
	_, err := s.Create(context.Background(), Operation{Kind: "test", Phase: Pending})
	if err == nil {
		t.Error("Create with empty ID should return error")
	}
}

func TestMemStoreUpdate(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	op := Operation{ID: "op-1", Kind: "test", Phase: Pending, CreatedAt: time.Now()}
	if _, err := s.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	op.Phase = Running
	if err := s.Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "op-1")
	if got.Phase != Running {
		t.Errorf("Phase = %s, want %s", got.Phase, Running)
	}
}

func TestMemStoreUpdateNotFound(t *testing.T) {
	s := NewMemStore()
	err := s.Update(context.Background(), Operation{ID: "nonexistent"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestMemStoreListAll(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	for _, id := range []string{"op-1", "op-2", "op-3"} {
		if _, err := s.Create(ctx, Operation{ID: id, Kind: "test", Phase: Pending, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List(ctx, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("List returned %d operations, want 3", len(got))
	}
}

func TestMemStoreListFilterByKind(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	create(t, s, Operation{ID: "op-1", Kind: "workspace.create", Phase: Pending, CreatedAt: time.Now()})
	create(t, s, Operation{ID: "op-2", Kind: "bundle.compile", Phase: Pending, CreatedAt: time.Now()})
	got, err := s.List(ctx, Filter{Kind: "workspace.create"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List returned %d operations, want 1", len(got))
	}
	if got[0].ID != "op-1" {
		t.Errorf("ID = %q, want %q", got[0].ID, "op-1")
	}
}

func TestMemStoreListFilterByPhase(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	create(t, s, Operation{ID: "op-1", Kind: "test", Phase: Pending, CreatedAt: time.Now()})
	create(t, s, Operation{ID: "op-2", Kind: "test", Phase: Running, CreatedAt: time.Now()})
	got, err := s.List(ctx, Filter{Phase: Running})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List returned %d, want 1", len(got))
	}
	if got[0].ID != "op-2" {
		t.Errorf("ID = %q, want %q", got[0].ID, "op-2")
	}
}

func TestMemStoreListFilterByResourceID(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	create(t, s, Operation{ID: "op-1", Kind: "test", Phase: Pending, ResourceID: "ws-1", CreatedAt: time.Now()})
	create(t, s, Operation{ID: "op-2", Kind: "test", Phase: Pending, ResourceID: "ws-2", CreatedAt: time.Now()})
	got, err := s.List(ctx, Filter{ResourceID: "ws-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List returned %d, want 1", len(got))
	}
}

func TestMemStoreListLimit(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	for i := range 5 {
		create(t, s, Operation{ID: fmt.Sprintf("op-%d", i), Kind: "test", Phase: Pending, CreatedAt: time.Now()})
	}
	got, err := s.List(ctx, Filter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("List with limit 2 returned %d, want 2", len(got))
	}
}

func TestMemStoreListEmpty(t *testing.T) {
	s := NewMemStore()
	got, err := s.List(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("List on empty store returned %d, want 0", len(got))
	}
}

func create(t *testing.T, s *MemStore, op Operation) {
	t.Helper()
	if _, err := s.Create(context.Background(), op); err != nil {
		t.Fatal(err)
	}
}
