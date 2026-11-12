package beads

import (
	"reflect"
	"testing"
)

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
