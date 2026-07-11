package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

// The read-only API check must stay consistent with dispatch across the
// migration to the durable file cursor: when the file has no record yet, it
// falls back to the legacy order:<scoped> + seq:<N> cursor carried on the
// order's order-run history beads, rather than treating the cursor as 0 and
// reporting the whole historical event window as due.
func TestCheckOrderTriggerForAPIEventCursorFallsBackToHistory(t *testing.T) {
	dir := t.TempDir()
	ep := events.NewFake()
	ep.Record(events.Event{Type: events.BeadClosed, Actor: "test"})
	head, err := ep.LatestSeq()
	if err != nil {
		t.Fatalf("LatestSeq: %v", err)
	}

	a := orders.Order{Name: "release-watch", Trigger: "event", On: events.BeadClosed}

	// No file cursor yet; the legacy cursor lives on the order-run history bead.
	history := []orderHistoryStoreBead{{
		storeRef: "city",
		bead: beads.Bead{
			ID:     "cursor-1",
			Labels: []string{"order-run:release-watch", "order:release-watch", fmt.Sprintf("seq:%d", head)},
		},
	}}
	if result := checkOrderTriggerForAPI(a, time.Now(), dir, history, ep); result.Due {
		t.Fatalf("event order due=%v, want false (legacy history cursor covers the window): %s", result.Due, result.Reason)
	}

	// Once the file cursor exists it wins, even with empty history.
	if err := orders.AdvanceEventCursor(dir, "release-watch", head); err != nil {
		t.Fatalf("AdvanceEventCursor: %v", err)
	}
	if result := checkOrderTriggerForAPI(a, time.Now(), dir, nil, ep); result.Due {
		t.Fatalf("event order due=%v after file cursor seeded, want false", result.Due)
	}

	// A genuinely new event past the cursor is reported due.
	ep.Record(events.Event{Type: events.BeadClosed, Actor: "test"})
	if result := checkOrderTriggerForAPI(a, time.Now(), dir, nil, ep); !result.Due {
		t.Fatal("event order due=false for a new event past the cursor, want true")
	}
}
