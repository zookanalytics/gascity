package dashboardbff

import (
	"context"
	"time"

	"github.com/gastownhall/gascity/internal/runproj"
)

func (t *cityRunTailer) runCensus(ctx context.Context) runproj.CanonicalRunCensus {
	timer := time.NewTimer(runColdLoadWait)
	defer timer.Stop()
	select {
	case <-t.readyCh:
	case <-ctx.Done():
	case <-timer.C:
	}

	t.mu.RLock()
	counts := t.census
	ready := t.ready
	incomplete := t.summary.LanesPartial
	t.mu.RUnlock()

	response := runproj.CanonicalRunCensus{
		Ready:        ready,
		StatusCounts: counts,
		Partial:      incomplete,
	}
	if !ready {
		response.Partial = true
		response.PartialReasons = []string{"run projection is warming"}
	} else if incomplete {
		response.PartialReasons = []string{"run projection is incomplete"}
	}
	return response
}

// RunCensus returns a bounded canonical status census from the plane's warm
// incremental projector. The bool is false only when cityName is unknown.
func (p *Plane) RunCensus(ctx context.Context, cityName string) (runproj.CanonicalRunCensus, bool) {
	tailer, ok := p.cityRunTailer(cityName)
	if !ok {
		return runproj.CanonicalRunCensus{}, false
	}
	return tailer.runCensus(ctx), true
}

func (t *cityRunTailer) runProjection() runproj.RunProjectionSnapshot {
	t.mu.RLock()
	snapshot := runproj.RunProjectionSnapshot{
		Ready:        t.ready,
		Beads:        t.beads,
		DecodeMisses: t.decodeMisses,
		Partial:      t.summary.LanesPartial,
	}
	t.mu.RUnlock()
	if !snapshot.Ready {
		snapshot.Partial = true
	}
	return snapshot
}

// RunProjection returns the non-blocking bead snapshot from the plane's warm
// incremental projector. The bool is false only when cityName is unknown.
func (p *Plane) RunProjection(_ context.Context, cityName string) (runproj.RunProjectionSnapshot, bool) {
	tailer, ok := p.cityRunTailer(cityName)
	if !ok {
		return runproj.RunProjectionSnapshot{}, false
	}
	return tailer.runProjection(), true
}

// RunProjectionMissInGrace reports whether a projected point-read miss is still
// inside the tailer's bounded unknown-run warming window.
func (p *Plane) RunProjectionMissInGrace(_ context.Context, cityName, runID string) bool {
	tailer, ok := p.cityRunTailer(cityName)
	if !ok || tailer.unknownRuns == nil {
		return false
	}
	return tailer.unknownRuns.inGrace(runID)
}

// ForgetRunProjectionMiss clears a run's unknown-run marker once the warm
// projection resolves it.
func (p *Plane) ForgetRunProjectionMiss(_ context.Context, cityName, runID string) {
	tailer, ok := p.cityRunTailer(cityName)
	if !ok || tailer.unknownRuns == nil {
		return
	}
	tailer.unknownRuns.forget(runID)
}

// SyncRunProjection gives a caller that just recorded bead events (a run
// cancel) read-your-writes on the city's next run read, and starts no tailer.
// When the city's tailer is running, it folds the event log through its
// current end into the warm projection before it returns, after waiting out a
// cold replay still in progress. When none is running, there is no warm
// projection to be stale: it returns true at once, and a tailer that starts
// later replays the log, the caller's events included. It reports false when
// the city is unknown, or when ctx ended or the tailer stopped before its
// fold; a tailer still running folds the events on its next poll.
func (p *Plane) SyncRunProjection(ctx context.Context, cityName string) bool {
	eventsPath, ok := p.resolveCityEventsPath(cityName)
	if !ok {
		return false
	}
	tailer, running := p.runTailers.running(cityName, eventsPath)
	if !running {
		return true
	}
	return tailer.sync(ctx)
}
