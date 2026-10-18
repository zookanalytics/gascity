package beads_test

import (
	"context"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// fieldSetRecorder records the queries a CachingStore sends its backing.
type fieldSetRecorder struct {
	beads.Store
	mu      sync.Mutex
	lists   []beads.ListQuery
	readies []beads.ReadyQuery
}

func (r *fieldSetRecorder) List(query beads.ListQuery) ([]beads.Bead, error) {
	r.mu.Lock()
	r.lists = append(r.lists, query)
	r.mu.Unlock()
	return r.Store.List(query)
}

func (r *fieldSetRecorder) Ready(query ...beads.ReadyQuery) ([]beads.Bead, error) {
	r.mu.Lock()
	if len(query) > 0 {
		r.readies = append(r.readies, query[0])
	} else {
		r.readies = append(r.readies, beads.ReadyQuery{})
	}
	r.mu.Unlock()
	return r.Store.Ready(query...)
}

func (r *fieldSetRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lists, r.readies = nil, nil
}

func (r *fieldSetRecorder) seen() ([]beads.ListQuery, []beads.ReadyQuery) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]beads.ListQuery(nil), r.lists...), append([]beads.ReadyQuery(nil), r.readies...)
}

// primedFieldSetCache returns a primed cache over a MemStore holding one
// ready bead with text, and the recorder between them.
func primedFieldSetCache(t *testing.T) (*beads.CachingStore, *fieldSetRecorder, *beads.MemStore, beads.Bead) {
	t.Helper()
	mem := beads.NewMemStore()
	b, err := mem.Create(beads.Bead{Title: "with text", Description: "cached text", Assignee: "worker", Metadata: map[string]string{"route": "pool"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := &fieldSetRecorder{Store: mem}
	cs := beads.NewCachingStoreForTest(rec, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	rec.reset()
	return cs, rec, mem, b
}

// A field set selects no rows, so a core Ready takes the route the zero-query
// Ready takes: the cache, when it can vouch for the answer.
func TestCachingStoreCoreReadyTakesTheZeroQueryRoute(t *testing.T) {
	cs, rec, _, b := primedFieldSetCache(t)

	full, err := cs.Ready()
	if err != nil {
		t.Fatal(err)
	}
	_, fullBacking := rec.seen()
	rec.reset()
	core, err := cs.Ready(beads.ReadyQuery{Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	_, coreBacking := rec.seen()

	if len(fullBacking) != 0 {
		t.Fatalf("the zero-query Ready read the backing %v; the fixture must let the cache answer", fullBacking)
	}
	if len(coreBacking) != 0 {
		t.Errorf("a core Ready read the backing %v, want the cache: a field set is not a selector", coreBacking)
	}
	if len(full) != 1 || full[0].Description != "cached text" {
		t.Fatalf("full Ready = %+v, want %s with its text", full, b.ID)
	}
	if len(core) != 1 || core[0].ID != b.ID || core[0].Description != "" || core[0].Metadata["route"] != "pool" {
		t.Fatalf("core Ready = %+v, want %s without its text and with its metadata", core, b.ID)
	}
}

// A core read served from the cache clears the text on the rows it returns,
// never on the cached row.
func TestCachingStoreCoreListFromCacheLeavesTheCachedRowWhole(t *testing.T) {
	cs, rec, _, b := primedFieldSetCache(t)

	core, err := cs.List(beads.ListQuery{AllowScan: true, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	if lists, _ := rec.seen(); len(lists) != 0 {
		t.Fatalf("a core list on a live cache read the backing %v", lists)
	}
	if len(core) != 1 || core[0].Description != "" || core[0].Assignee != "worker" {
		t.Fatalf("core List = %+v, want %s without its text", core, b.ID)
	}
	if cached, ok := cs.CachedList(beads.ListQuery{AllowScan: true, Fields: beads.FieldsCore}); !ok || len(cached) != 1 || cached[0].Description != "" {
		t.Fatalf("core CachedList = %+v (ok=%v), want the row without its text", cached, ok)
	}
	if ready, err := cs.Handles().Cached.Ready(beads.ReadyQuery{Fields: beads.FieldsCore}); err != nil || len(ready) != 1 || ready[0].Description != "" {
		t.Fatalf("core cached-handle Ready = %+v (err=%v), want the row without its text", ready, err)
	}
	whole, err := cs.Get(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if whole.Description != "cached text" {
		t.Fatalf("cached row Description = %q after core reads, want %q", whole.Description, "cached text")
	}
}

// A projected Live list reads the backing with its field set and installs
// nothing: its rows carry no text, and installing one would erase the cached
// text of its bead.
func TestCachingStoreProjectedLiveListInstallsNothing(t *testing.T) {
	cs, rec, mem, b := primedFieldSetCache(t)
	newTitle := "renamed behind the cache"
	if err := mem.Update(b.ID, beads.UpdateOpts{Title: &newTitle}); err != nil {
		t.Fatal(err)
	}

	live, err := cs.List(beads.ListQuery{AllowScan: true, Live: true, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].Title != newTitle || live[0].Description != "" {
		t.Fatalf("projected live List = %+v, want the backing row without its text", live)
	}
	lists, _ := rec.seen()
	if len(lists) != 1 || lists[0].Fields != beads.FieldsCore {
		t.Fatalf("backing lists = %+v, want one carrying FieldsCore", lists)
	}

	cached, ok := cs.CachedList(beads.ListQuery{AllowScan: true})
	if !ok || len(cached) != 1 {
		t.Fatalf("CachedList = %+v (ok=%v), want the one cached row", cached, ok)
	}
	if cached[0].Description != "cached text" {
		t.Errorf("cached Description = %q after a projected live list, want %q", cached[0].Description, "cached text")
	}
	if cached[0].Title == newTitle {
		t.Errorf("a projected live list installed its row into the cache")
	}

	if _, err := cs.List(beads.ListQuery{ParentID: "no-such-parent", Fields: beads.FieldsCore}); err != nil {
		t.Fatal(err)
	}
	if lists, _ := rec.seen(); len(lists) != 2 || lists[1].Fields != beads.FieldsCore {
		t.Fatalf("backing lists = %+v, want the parent list to carry FieldsCore", lists)
	}
}

// Every read a cache sends its backing carries the caller's field set.
func TestCachingStoreBackingReadsCarryTheFieldSet(t *testing.T) {
	mem := beads.NewMemStore()
	if _, err := mem.Create(beads.Bead{Title: "open", Description: "open text"}); err != nil {
		t.Fatal(err)
	}
	closed, err := mem.Create(beads.Bead{Title: "closed", Description: "closed text"})
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Close(closed.ID); err != nil {
		t.Fatal(err)
	}
	rec := &fieldSetRecorder{Store: mem}
	cold := beads.NewCachingStoreForTest(rec, nil)

	rows, err := cold.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("cold core List = %+v, want both rows", rows)
	}
	ready, err := cold.Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range append(rows, ready...) {
		if b.Description != "" {
			t.Errorf("row %s has Description %q, want empty on a core read", b.ID, b.Description)
		}
	}
	lists, readies := rec.seen()
	if len(lists) == 0 || len(readies) == 0 {
		t.Fatalf("a cold cache answered without its backing: lists=%v readies=%v", lists, readies)
	}
	for _, q := range lists {
		if q.Fields != beads.FieldsCore {
			t.Errorf("backing list %+v dropped the field set", q)
		}
	}
	for _, q := range readies {
		if q.Fields != beads.FieldsCore {
			t.Errorf("backing ready %+v dropped the field set", q)
		}
	}

	primed, primedRec, _, _ := primedFieldSetCache(t)
	merged, err := primed.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range merged {
		if b.Description != "" {
			t.Errorf("merged closed-inclusive row %s has Description %q, want empty", b.ID, b.Description)
		}
	}
	if lists, _ := primedRec.seen(); len(lists) != 1 || lists[0].Fields != beads.FieldsCore {
		t.Fatalf("closed-history backing lists = %+v, want one carrying FieldsCore", lists)
	}
}
