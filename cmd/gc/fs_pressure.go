package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/gastownhall/gascity/internal/events"
)

// Default threshold for "some avg60" before we skip a supervisor tick. A
// value of 50.0 means that if tasks were stalled on IO more than 50% of the
// last 60 seconds, the tick is skipped to avoid piling on more writes.
const defaultFSPressureThreshold = 50.0

// fsPressureThresholdEnv is the env var that overrides defaultFSPressureThreshold.
const fsPressureThresholdEnv = "GC_SUPERVISOR_FS_PRESSURE_THRESHOLD"

// maxConsecutiveFSPressureSkips bounds pressure shedding so liveness work can
// still make progress under sustained external IO pressure.
const maxConsecutiveFSPressureSkips = 5

const (
	fsPressureOutcomeSkipped = "skipped"
	fsPressureOutcomeForced  = "forced"
)

var (
	fsPressureInvalidThresholdWarned atomic.Bool
	fsPressureReadErrorWarned        atomic.Bool
)

type fsPressureStatus struct {
	Avg60     float64
	Threshold float64
	High      bool
}

// fsPressureThreshold returns the currently configured IO pressure threshold.
// Invalid env values fall back to the default.
func fsPressureThreshold() float64 {
	return fsPressureThresholdWithWarning(nil)
}

func fsPressureThresholdWithWarning(stderr io.Writer) float64 {
	raw := os.Getenv(fsPressureThresholdEnv)
	if raw == "" {
		return defaultFSPressureThreshold
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100 {
		if stderr != nil && fsPressureInvalidThresholdWarned.CompareAndSwap(false, true) {
			fmt.Fprintf(stderr, "supervisor: invalid %s=%q, using default %.1f\n", //nolint:errcheck // best-effort stderr
				fsPressureThresholdEnv, raw, defaultFSPressureThreshold)
		}
		return defaultFSPressureThreshold
	}
	return v
}

func currentFSPressureStatus(stderr io.Writer) (fsPressureStatus, bool) {
	threshold := fsPressureThresholdWithWarning(stderr)
	avg60, err := readFSPressureAvg60(fsPressurePath)
	if err != nil {
		// Fail open: if we can't read PSI, don't block work.
		if stderr != nil && fsPressureReadErrorWarned.CompareAndSwap(false, true) {
			fmt.Fprintf(stderr, "supervisor: FS pressure unavailable at %s: %v; proceeding without backpressure\n", //nolint:errcheck // best-effort stderr
				fsPressurePath, err)
		}
		return fsPressureStatus{}, false
	}
	return fsPressureStatus{
		Avg60:     avg60,
		Threshold: threshold,
		High:      avg60 > threshold,
	}, true
}

func logFSPressureSkip(stderr io.Writer, status fsPressureStatus) {
	if stderr != nil {
		fmt.Fprintf(stderr, "supervisor: FS pressure high (some avg60=%.2f > threshold=%.1f), skipping tick\n", //nolint:errcheck // best-effort stderr
			status.Avg60, status.Threshold)
	}
}

func recordFSPressureSkippedTickEvent(rec events.Recorder, cityName, trigger string, status fsPressureStatus, consecutiveSkips int, outcome string) {
	if rec == nil {
		return
	}
	payload := events.SupervisorFSPressureSkippedTickPayload{
		Avg60:               status.Avg60,
		Threshold:           status.Threshold,
		ConsecutiveSkips:    consecutiveSkips,
		MaxConsecutiveSkips: maxConsecutiveFSPressureSkips,
		Outcome:             outcome,
		Trigger:             trigger,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	rec.Record(events.Event{
		Type:    events.SupervisorFSPressureSkippedTick,
		Actor:   "supervisor",
		Subject: cityName,
		Payload: raw,
	})
}

func recordFSPressureSkippedTickTrace(trace *sessionReconcilerTraceCycle, trigger string, status fsPressureStatus, consecutiveSkips int) {
	if trace == nil {
		return
	}
	trace.RecordControllerDecision(TraceSiteSupervisorFSPressure, TraceReasonFSPressure, TraceOutcomeSkipped, fsPressureTraceFields(trigger, status, consecutiveSkips, fsPressureOutcomeSkipped))
}

func recordFSPressureForcedTickTrace(trace *sessionReconcilerTraceCycle, trigger string, status fsPressureStatus, consecutiveSkips int) {
	if trace == nil {
		return
	}
	trace.RecordControllerDecision(TraceSiteSupervisorFSPressure, TraceReasonFSPressure, TraceOutcomeApplied, fsPressureTraceFields(trigger, status, consecutiveSkips, fsPressureOutcomeForced))
}

func fsPressureTraceFields(trigger string, status fsPressureStatus, consecutiveSkips int, outcome string) map[string]any {
	return map[string]any{
		"avg60":                 status.Avg60,
		"threshold":             status.Threshold,
		"consecutive_skips":     consecutiveSkips,
		"max_consecutive_skips": maxConsecutiveFSPressureSkips,
		"outcome":               outcome,
		"trigger":               trigger,
	}
}

// fsPressureEpisode is one shedding episode's counters. Each loop that sheds
// under IO pressure owns its own: the reconciler tick and the order-dispatch
// loop run on separate goroutines and shed independently, so sharing counters
// between them would both race and let one loop's skips force the other's tick.
type fsPressureEpisode struct {
	consecutiveSkips int
	episodeLogged    bool
}

func (ep *fsPressureEpisode) reset() {
	ep.consecutiveSkips = 0
	ep.episodeLogged = false
}

func (cr *CityRuntime) resetFSPressureEpisode() {
	cr.tickFSPressure.reset()
}

// shouldSkipTickForFSPressure gates only the patrol/poke tick path after
// config reload and before managed-Dolt preflight, session sync, demand build,
// and reconciliation. Pressure-skipped ticks still drain already queued
// convergence requests; nudge-dispatch, control-dispatcher, socket-driven
// convergence requests, and manual reload refreshes are separate high-priority
// paths and are not covered by this gate. Order dispatch is gated too, but by
// its own episode on its own goroutine — see orderDispatchLoop.
func (cr *CityRuntime) shouldSkipTickForFSPressure(trace *sessionReconcilerTraceCycle, trigger string) bool {
	return cr.shouldSkipForFSPressure(&cr.tickFSPressure, trace, trigger)
}

// shouldSkipForFSPressure applies the shedding policy to one caller's episode.
// ep must be owned by the calling goroutine.
func (cr *CityRuntime) shouldSkipForFSPressure(ep *fsPressureEpisode, trace *sessionReconcilerTraceCycle, trigger string) bool {
	status, ok := currentFSPressureStatus(cr.stderr)
	if !ok || !status.High {
		ep.reset()
		return false
	}

	if ep.consecutiveSkips >= maxConsecutiveFSPressureSkips {
		if cr.stderr != nil {
			fmt.Fprintf(cr.stderr, "supervisor: FS pressure high (some avg60=%.2f > threshold=%.1f), forcing tick after %d skipped ticks\n", //nolint:errcheck // best-effort stderr
				status.Avg60, status.Threshold, ep.consecutiveSkips)
		}
		recordFSPressureForcedTickTrace(trace, trigger, status, ep.consecutiveSkips)
		recordFSPressureSkippedTickEvent(cr.rec, cr.cityName, trigger, status, ep.consecutiveSkips, fsPressureOutcomeForced)
		ep.reset()
		return false
	}

	ep.consecutiveSkips++
	if !ep.episodeLogged {
		logFSPressureSkip(cr.stderr, status)
	}
	ep.episodeLogged = true
	recordFSPressureSkippedTickTrace(trace, trigger, status, ep.consecutiveSkips)
	recordFSPressureSkippedTickEvent(cr.rec, cr.cityName, trigger, status, ep.consecutiveSkips, fsPressureOutcomeSkipped)
	return true
}
