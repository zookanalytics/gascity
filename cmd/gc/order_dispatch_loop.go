package main

import (
	"context"
	"time"

	"github.com/gastownhall/gascity/internal/config"
)

// orderDispatchLoop runs order dispatch on its own goroutine, at its own
// cadence, for the life of ctx.
//
// Order dispatch used to run inline at the top of the reconciler tick. That
// made every declared order cadence a function of reconciler latency: the
// phases that follow dispatch in a tick (session sync, desired-state build,
// reconcile, process-table and worktree sweeps) are unbounded — run()'s own
// startup comment concedes "a cold-start reconcile can take minutes" — and
// time.Ticker coalesces every patrol tick that elapses while the loop is busy.
// An order declaring every = "60s" therefore fired at whatever interval the
// reconciler happened to leave, with no signal that its cadence had slipped.
//
// Placing dispatch first within the tick, which the old code did deliberately,
// only ordered it ahead of the slow work inside one tick. It could not affect
// how often a tick began, which is what actually sets the dispatch rate.
//
// The loop deliberately keeps its own ticker rather than reusing the patrol
// ticker: the point is to stop sharing a clock with reconcile work at all.
//
// It deliberately does NOT call beads.SetReconcilerTickTrigger. That global is
// a swap/restore pair the reconciler already drives per tick, documented as
// single-tenant best-effort; a second concurrent swapper would interleave the
// restores and attribute bd calls to the wrong trigger. Leaving it alone keeps
// attribution no worse than it was.
func (cr *CityRuntime) orderDispatchLoop(ctx context.Context, cityRoot string) {
	interval := cr.orderDispatchInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// safeTick keeps a panicking dispatch from taking the loop (and
			// with it every future order) down, matching how the reconciler
			// goroutine guards its own tick body.
			cr.safeTick(func() {
				cr.dispatchOrders(ctx, cityRoot)
			}, "order-dispatch")

			// Re-read the cadence each pass so a hot reload of city.toml takes
			// effect without restarting the controller. Reset (not a new
			// Ticker) so the change applies from the next interval and no tick
			// is dropped in between.
			if next := cr.orderDispatchInterval(); next != interval {
				interval = next
				ticker.Reset(interval)
			}
		}
	}
}

// currentConfig returns the live city config under the service-state lock.
//
// cr.cfg is owned by the reconciler goroutine, which swaps it on reload. Any
// other goroutine — the order-dispatch loop and everything it calls — must read
// it through here, not off the field, or the read races that swap.
func (cr *CityRuntime) currentConfig() *config.City {
	cr.serviceStateMu.RLock()
	defer cr.serviceStateMu.RUnlock()
	return cr.cfg
}

// orderDispatchInterval is how often the order-dispatch loop evaluates which
// orders are due. It tracks the patrol interval, because that was the cadence
// order dispatch nominally ran at before it was given its own goroutine, and
// because it is already the operator's "how often does the controller look at
// things" dial.
func (cr *CityRuntime) orderDispatchInterval() time.Duration {
	cfg := cr.currentConfig()
	if cfg == nil {
		return defaultOrderDispatchInterval
	}
	if d := cfg.Daemon.PatrolIntervalDuration(); d > 0 {
		return d
	}
	return defaultOrderDispatchInterval
}

// defaultOrderDispatchInterval backs orderDispatchInterval when config carries
// no usable patrol interval. It matches the daemon's own patrol-interval
// default so an unconfigured city dispatches at the rate it always did.
const defaultOrderDispatchInterval = 30 * time.Second
