package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/api"
	"github.com/gastownhall/gascity/internal/beads"
)

// manyConvoys is the convoy count the progress fan-out tests check: several
// times convoyProgressConcurrency, so the reads need more than one batch.
const manyConvoys = 3 * convoyProgressConcurrency

// manyConvoyChecksServer answers the check of gc-i with i of manyConvoys
// children closed, so each view shows whether it is its own convoy's. failID,
// when set, makes that convoy's check answer 500. The first check waits until a
// second check is in flight beside it, and the test fails if none arrives,
// because then the checks ran one at a time. It also fails if more than
// convoyProgressConcurrency checks are ever in flight together.
func manyConvoyChecksServer(t *testing.T, failID string) *httptest.Server {
	t.Helper()
	var inFlight, maxInFlight atomic.Int32
	var firstWaited atomic.Bool
	together := make(chan struct{})
	var closeTogether sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			prev := maxInFlight.Load()
			if n <= prev || maxInFlight.CompareAndSwap(prev, n) {
				break
			}
		}
		if n >= 2 {
			closeTogether.Do(func() { close(together) })
		}
		if !firstWaited.Swap(true) {
			select {
			case <-together:
			case <-time.After(10 * time.Second):
				t.Error("no second convoy check arrived while the first was in flight; the checks ran one at a time")
			}
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v0/city/test-city/convoy/"), "/check")
		if id == failID {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"status": http.StatusInternalServerError, "title": "Internal Server Error", "detail": "internal: check exploded"}) //nolint:errcheck
			return
		}
		closed, err := strconv.Atoi(strings.TrimPrefix(id, "gc-"))
		if err != nil {
			t.Errorf("check for unexpected convoy %q (path %s)", id, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"convoy_id": id, "total": manyConvoys, "closed": closed, "complete": closed == manyConvoys}) //nolint:errcheck
	}))
	t.Cleanup(func() {
		srv.Close()
		if n := maxInFlight.Load(); n > convoyProgressConcurrency {
			t.Errorf("checks in flight together = %d, want at most %d", n, convoyProgressConcurrency)
		}
	})
	return srv
}

func manyConvoyBeads() []beads.Bead {
	convoys := make([]beads.Bead, 0, manyConvoys)
	for i := manyConvoys; i >= 1; i-- {
		convoys = append(convoys, beads.Bead{ID: fmt.Sprintf("gc-%d", i), Title: fmt.Sprintf("convoy-%d", i), Type: "convoy", Status: "open"})
	}
	return convoys
}

// The progress reads of a convoy listing run together, within the bound, and
// each convoy gets its own check's answer in list order.
func TestFetchConvoyProgressRunsChecksTogetherWithinItsBound(t *testing.T) {
	srv := manyConvoyChecksServer(t, "")
	convoys := manyConvoyBeads()

	progress, err := fetchConvoyProgress(api.NewCityScopedClient(srv.URL, "test-city"), convoys)
	if err != nil {
		t.Fatalf("fetchConvoyProgress: %v", err)
	}
	if len(progress) != len(convoys) {
		t.Fatalf("progress views = %d, want %d", len(progress), len(convoys))
	}
	for i, convoy := range convoys {
		want, _ := strconv.Atoi(strings.TrimPrefix(convoy.ID, "gc-"))
		if progress[i].ConvoyID != convoy.ID || progress[i].Closed != want {
			t.Fatalf("progress[%d] = %+v, want the check of %s (%d closed)", i, progress[i], convoy.ID, want)
		}
	}
}

// One failed check fails the whole read, as the sequential read did.
func TestFetchConvoyProgressReportsAFailedCheck(t *testing.T) {
	srv := manyConvoyChecksServer(t, fmt.Sprintf("gc-%d", manyConvoys/2))

	progress, err := fetchConvoyProgress(api.NewCityScopedClient(srv.URL, "test-city"), manyConvoyBeads())
	if err == nil {
		t.Fatalf("fetchConvoyProgress = %d views, nil error; want the failed check's error", len(progress))
	}
	if progress != nil {
		t.Fatalf("fetchConvoyProgress returned %d views beside its error", len(progress))
	}
}
