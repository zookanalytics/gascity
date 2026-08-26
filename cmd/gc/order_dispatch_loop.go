package main

import (
	"context"
	"time"
)

// orderDispatchLoop runs order dispatch on its own goroutine, at its own
// cadence, for the life of ctx.
//
// The loop keeps its own ticker rather than reusing the patrol ticker. Sharing
// a clock with reconcile work is what ties a declared order cadence to
// reconciler latency: the reconcile phases are unbounded, and time.Ticker
// coalesces every tick that elapses while the loop is busy.
//
// It must not call beads.SetReconcilerTickTrigger. That global is a
// swap/restore pair the reconciler already drives per tick and is documented
// single-tenant best-effort; a second concurrent swapper would interleave the
// restores and attribute bd calls to the wrong trigger.
func (cr *CityRuntime) orderDispatchLoop(ctx context.Context, cityRoot string) {
	interval := cr.orderDispatchInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// A dispatch pass writes tracking beads, so it sheds under IO pressure the
	// way the tick does. The episode is a local, which is what makes it
	// goroutine-owned: the reconciler counts its own skips in cr.tickFSPressure
	// and neither loop may force the other's pass.
	var pressure fsPressureEpisode

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// trace is nil: trace cycles belong to the reconciler tick and are
			// not safe to open from a second goroutine. The skip/force EVENT is
			// still recorded, so shedding stays observable.
			if !cr.shouldSkipForFSPressure(&pressure, nil, "order-dispatch") {
				// safeTick keeps a panicking dispatch from taking the loop, and
				// with it every future order, down.
				cr.safeTick(func() {
					cr.dispatchOrders(ctx, cityRoot)
				}, "order-dispatch")
			}

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

// orderDispatchInterval is how often the order-dispatch loop evaluates which
// orders are due. It tracks the patrol interval, which is already the
// operator's "how often does the controller look at things" dial.
func (cr *CityRuntime) orderDispatchInterval() time.Duration {
	cfg := cr.serviceConfigSnapshot()
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
// default, so an unconfigured city dispatches at the daemon's rate.
const defaultOrderDispatchInterval = 30 * time.Second
