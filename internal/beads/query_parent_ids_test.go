package beads

import (
	"reflect"
	"slices"
	"testing"
)

// parentIDsFixture holds two listed parents with a child each, an unlisted
// parent with a child, and a bead with no parent.
type parentIDsFixture struct {
	store                                  *MemStore
	one, two, unlisted                     Bead
	childOfOne, childOfTwo, childElsewhere Bead
	orphan                                 Bead
}

func newParentIDsFixture(t *testing.T) parentIDsFixture {
	t.Helper()
	f := parentIDsFixture{store: NewMemStore()}
	create := func(title, parent string) Bead {
		t.Helper()
		b, err := f.store.Create(Bead{Title: title, ParentID: parent})
		if err != nil {
			t.Fatalf("create %s: %v", title, err)
		}
		return b
	}
	f.one = create("parent one", "")
	f.two = create("parent two", "")
	f.unlisted = create("unlisted parent", "")
	f.childOfOne = create("child of one", f.one.ID)
	f.childOfTwo = create("child of two", f.two.ID)
	f.childElsewhere = create("child of the unlisted parent", f.unlisted.ID)
	f.orphan = create("no parent", "")
	return f
}

func sortedIDsOf(items []Bead) []string {
	return slices.Sorted(slices.Values(beadIDsOf(items)))
}

// TestListQueryParentIDsSelectsExactlyTheChildren pins that a store which
// matches in Go answers ParentIDs exactly: the children of the listed parents,
// and neither the unlisted parent's child nor a bead with no parent.
func TestListQueryParentIDsSelectsExactlyTheChildren(t *testing.T) {
	f := newParentIDsFixture(t)

	got, err := f.store.List(ListQuery{ParentIDs: []string{f.one.ID, f.two.ID}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := slices.Sorted(slices.Values([]string{f.childOfOne.ID, f.childOfTwo.ID}))
	if ids := sortedIDsOf(got); !reflect.DeepEqual(ids, want) {
		t.Fatalf("List(ParentIDs) = %v, want exactly the children %v, not %s or %s", ids, want, f.childElsewhere.ID, f.orphan.ID)
	}
}

// TestListQueryParentIDsIsAFilterNotAScan pins that a ParentIDs query needs no
// AllowScan: naming the parents bounds the answer, the way naming ids does.
func TestListQueryParentIDsIsAFilterNotAScan(t *testing.T) {
	if !(ListQuery{ParentIDs: []string{"ga-1"}}).HasFilter() {
		t.Fatal("ListQuery{ParentIDs} reports no filter, so it would require AllowScan")
	}
	if (ListQuery{}).HasFilter() {
		t.Fatal("the empty query reports a filter; HasFilter is not discriminating")
	}
}

// TestListQueryParentIDsEmptyEntryNamesNoParent pins that an empty entry names
// no parent. A bead with no parent has an empty ParentID, and matching it to an
// empty entry would hand every unparented bead to a caller walking children.
func TestListQueryParentIDsEmptyEntryNamesNoParent(t *testing.T) {
	f := newParentIDsFixture(t)

	got, err := f.store.List(ListQuery{ParentIDs: []string{""}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(ParentIDs=[\"\"]) = %v, want no bead; %s has no parent and must not match", beadIDsOf(got), f.orphan.ID)
	}
	got, err = f.store.List(ListQuery{ParentIDs: []string{"", f.one.ID}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := beadIDsOf(got); !reflect.DeepEqual(ids, []string{f.childOfOne.ID}) {
		t.Fatalf("List(ParentIDs=[\"\" %s]) = %v, want only its child %s", f.one.ID, ids, f.childOfOne.ID)
	}
}

// TestListQueryParentIDsComposesWithParentID pins that ParentID and ParentIDs
// narrow conjunctively: the parent must be ParentID and also one of ParentIDs.
func TestListQueryParentIDsComposesWithParentID(t *testing.T) {
	f := newParentIDsFixture(t)

	got, err := f.store.List(ListQuery{ParentID: f.two.ID, ParentIDs: []string{f.one.ID, f.two.ID}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if ids := beadIDsOf(got); !reflect.DeepEqual(ids, []string{f.childOfTwo.ID}) {
		t.Fatalf("List(ParentID+ParentIDs) = %v, want [%s]", ids, f.childOfTwo.ID)
	}
	got, err = f.store.List(ListQuery{ParentID: f.unlisted.ID, ParentIDs: []string{f.one.ID, f.two.ID}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(ParentID outside ParentIDs) = %v, want no bead", beadIDsOf(got))
	}
}

// TestDistinctNonEmptyKeepsFirstSeenOrderAndNeverTrims pins the id-list
// normalization every store shares: empties and repeats drop out, the order is
// the order ids first appear, and an id with edge whitespace is its own id.
func TestDistinctNonEmptyKeepsFirstSeenOrderAndNeverTrims(t *testing.T) {
	got := distinctNonEmpty([]string{"b", "", "a", "b", " a", "a ", "a"})
	want := []string{"b", "a", " a", "a "}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("distinctNonEmpty = %q, want %q", got, want)
	}
	if got := distinctNonEmpty([]string{"", ""}); len(got) != 0 {
		t.Fatalf("distinctNonEmpty of empty ids = %q, want none", got)
	}
}
