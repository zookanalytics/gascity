package convoy

import (
	"fmt"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// seedConvoys builds a store with convoyCount convoys, each tracking
// membersEach members, and returns the convoy ids. It is the fixture for the
// scaling benchmarks below.
func seedConvoys(tb testing.TB, convoyCount, membersEach int) (*beads.MemStore, []string) {
	tb.Helper()
	store := beads.NewMemStore()
	ids := make([]string, 0, convoyCount)
	for i := 0; i < convoyCount; i++ {
		convoy, err := store.Create(beads.Bead{Title: fmt.Sprintf("convoy %d", i), Type: "convoy"})
		if err != nil {
			tb.Fatalf("create convoy: %v", err)
		}
		ids = append(ids, convoy.ID)
		for j := 0; j < membersEach; j++ {
			m, err := store.Create(beads.Bead{Title: fmt.Sprintf("m %d/%d", i, j), Type: "task", Status: "open"})
			if err != nil {
				tb.Fatalf("create member: %v", err)
			}
			if err := TrackItem(store, convoy.ID, m.ID, store); err != nil {
				tb.Fatalf("track member: %v", err)
			}
		}
	}
	return store, ids
}

// BenchmarkConvoyMembers contrasts the per-convoy read (one Members call per
// convoy) against the batched read (MembersBatch, one bounded pass) as the
// convoy count grows. `gc convoy list` resolves members across many convoys at
// once, so it takes the batched path; this contrast measures the cost that
// motivates it. The per-convoy variant is superlinear because each convoy
// re-scans the whole store; the batched variant stays roughly linear.
func BenchmarkConvoyMembers(b *testing.B) {
	const membersEach = 4
	for _, n := range []int{50, 100, 200, 400} {
		store, ids := seedConvoys(b, n, membersEach)

		b.Run(fmt.Sprintf("per-convoy/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, id := range ids {
					if _, err := Members(store, id, true); err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run(fmt.Sprintf("batched/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := MembersBatch(store, ids, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
