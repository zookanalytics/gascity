package orders

import (
	"errors"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

type rowsErrorStore struct {
	*beads.MemStore
	rows []beads.Bead
	err  error
}

func (s *rowsErrorStore) List(_ beads.ListQuery) ([]beads.Bead, error) {
	return s.rows, s.err
}

func TestLastRunFuncForStoreReturnsLatestRun(t *testing.T) {
	store := beads.NewMemStore()

	first, err := store.Create(beads.Bead{
		Title:  "order:digest",
		Status: "closed",
		Labels: []string{"order-run:digest"},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond)

	second, err := store.Create(beads.Bead{
		Title:  "order:digest",
		Status: "closed",
		Labels: []string{"order-run:digest", "wisp-failed"},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := LastRunFuncForStore(store)("digest")
	if err != nil {
		t.Fatalf("LastRunFuncForStore(): %v", err)
	}
	if !got.Equal(second.CreatedAt) {
		t.Fatalf("LastRunFuncForStore() = %s, want %s (latest run should remain authoritative)", got, second.CreatedAt)
	}
	if !second.CreatedAt.After(first.CreatedAt) {
		t.Fatalf("test setup invalid: second.CreatedAt=%s, first.CreatedAt=%s", second.CreatedAt, first.CreatedAt)
	}
}

func TestLastRunFuncForStoreReturnsZeroWhenNoRunsExist(t *testing.T) {
	store := beads.NewMemStore()

	got, err := LastRunFuncForStore(store)("digest")
	if err != nil {
		t.Fatalf("LastRunFuncForStore(): %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("LastRunFuncForStore() = %s, want zero time", got)
	}
}

func TestLastRunFuncForStoreUsesRowsFromPartialTierError(t *testing.T) {
	want := time.Date(2026, 5, 15, 7, 0, 0, 0, time.UTC)
	store := &rowsErrorStore{
		MemStore: beads.NewMemStore(),
		rows: []beads.Bead{{
			ID:        "run-1",
			Title:     "digest",
			CreatedAt: want,
			Labels:    []string{"order-run:digest"},
		}},
		err: errors.New("wisps tier unavailable"),
	}

	got, err := LastRunFuncForStore(store)("digest")
	if err != nil {
		t.Fatalf("LastRunFuncForStore(): %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("LastRunFuncForStore() = %s, want %s from surviving rows", got, want)
	}
}
