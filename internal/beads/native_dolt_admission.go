package beads

import (
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// ErrDoltServerSaturated signals shared-server saturation on a native Dolt store
// open. It is returned both when the process-wide admission gate is backing off
// (rejecting before any dial) and when an admitted open itself hits a saturation
// signal at the handshake or config-read boundary. Callers treat it as a
// transient "back off and retry later" signal — not a hard open failure — and
// must not fall back to a path that dials the same overloaded server.
var ErrDoltServerSaturated = errors.New("dolt server saturated: backing off (collective)")

// Admission-gate tuning. A shared Dolt sql-server has a finite connection
// ceiling (max_connections); once it saturates, new handshakes time out with
// "i/o timeout" and every layer re-dials, feeding the saturation. The gate
// detects that regime per server (host:port) and makes all native-store opens
// to that server back off together instead of re-dialing into the storm.
const (
	// doltSaturationWindow is how far back recent open outcomes are considered
	// when deciding whether the server is saturated.
	doltSaturationWindow = 20 * time.Second

	// doltSaturationMinSamples is the minimum number of saturation-classified
	// opens within the window before the gate may trip. It keeps a couple of
	// stray timeouts on an otherwise-healthy server from backing the fleet off.
	doltSaturationMinSamples = 5

	// doltSaturationTripFraction is the share of recent opens that must be
	// saturation-classified to trip. Evaluating a ratio (rather than a
	// consecutive-failure count) is what lets the gate fire under saturation
	// even though some opens still succeed — the failure mode the per-database
	// circuit breaker misses, because every success resets its counter.
	doltSaturationTripFraction = 0.5

	// doltSaturationCooldown is how long the gate rejects opens after tripping
	// before admitting a single probe. Mirrors the beads per-database breaker
	// cooldown; long enough for the server to shed its queued connections.
	doltSaturationCooldown = 5 * time.Second
)

// doltSaturationProbeTimeout bounds how long a probe may be considered "in
// flight" before the gate re-arms a fresh one, so a probe whose outcome is never
// recorded (e.g. a panic between Admit and the open) cannot wedge the gate open
// forever. Set beyond the native open timeout (bdCommandTimeout) so a slow probe
// normally reports back before the re-arm.
var doltSaturationProbeTimeout = 2 * bdCommandTimeout

// doltAdmissionOutcome is one recorded native-open result within the window.
type doltAdmissionOutcome struct {
	at        time.Time
	saturated bool
}

// admissionToken identifies a single native-store open admitted through the
// gate. Admit hands one back and RecordOutcome matches it, so that only the
// outcome of the open admitted as the recovery probe can resolve the gate. The
// zero value is never issued for a probe and means "no probe in flight"; opens
// admitted while the gate is closed also carry the zero token because they are
// not probes and must never resolve a later probe's slot.
type admissionToken uint64

// doltAdmissionGate is a process-wide saturation breaker for a single Dolt
// server address (host:port), shared across every database on that server.
//
// It distinguishes server *saturation* (connection attempts timing out at the
// handshake while some still succeed) from a server that is simply *down*
// (connection refused) — the latter is owned by the beads per-database circuit
// breaker. When saturated, Admit returns false for all callers except a single
// spaced probe, so the whole process stops dialing the overloaded server until
// it recovers.
type doltAdmissionGate struct {
	addr string

	mu     sync.Mutex
	now    func() time.Time
	recent []doltAdmissionOutcome

	open       bool           // true while backing off
	openedAt   time.Time      // when backoff began / the last probe was issued
	nextToken  admissionToken // monotonic source of probe tokens
	probeToken admissionToken // token of the in-flight probe (0 = none)
	probeAt    time.Time      // when the in-flight probe was admitted
}

// newDoltAdmissionGate creates a closed (admitting) gate for the given server
// address.
func newDoltAdmissionGate(addr string) *doltAdmissionGate {
	return &doltAdmissionGate{addr: addr, now: time.Now}
}

// Admit reports whether a native-store open against this server should proceed
// and returns a token identifying the admission. While closed it always admits
// with the zero token (the open is an ordinary open, not a probe). While
// backing off it rejects every caller except one probe per cooldown, which it
// admits with a fresh non-zero token so RecordOutcome can tell the probe's
// outcome apart from a straggler open admitted before the trip.
func (g *doltAdmissionGate) Admit() (bool, admissionToken) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.open {
		return true, 0
	}

	now := g.now()
	// A probe is already out; keep rejecting until it resolves or times out.
	if g.probeToken != 0 {
		if now.Sub(g.probeAt) < doltSaturationProbeTimeout {
			return false, 0
		}
		// The previous probe never reported back; fall through and re-arm. Its
		// token is now stale, so if it ever returns it cannot match the new
		// probe and cannot resolve the re-armed slot.
	}

	if now.Sub(g.openedAt) >= doltSaturationCooldown {
		g.nextToken++
		g.probeToken = g.nextToken
		g.probeAt = now
		g.openedAt = now
		return true, g.probeToken
	}
	return false, 0
}

// RecordOutcome reports the result of a native-store open so the gate can trip
// or recover. token is the value Admit returned for this open; err is the error
// returned by the open (nil on success).
func (g *doltAdmissionGate) RecordOutcome(token admissionToken, err error) {
	saturated := doltServerSaturationOutcome(err)

	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()

	if g.open {
		// Only the admitted probe's own outcome resolves the gate. An outcome
		// whose token does not match the live probe is a straggler — an open
		// admitted before the trip (token 0) or a superseded probe — that just
		// happened to finish inside this probe window. Ignoring it keeps a stale
		// success from canceling an active backoff and a stale timeout from
		// re-arming the cooldown and discarding the real probe's pending result.
		if g.probeToken == 0 || token != g.probeToken {
			return
		}
		g.probeToken = 0
		if saturated {
			g.openedAt = now // re-arm: another cooldown before the next probe
			return
		}
		// The probe reached a responsive server (success, or a non-saturation
		// error such as a hard-down refusal the per-database breaker owns):
		// stop backing off.
		g.open = false
		g.recent = nil
		log.Printf("[dolt-admission] %s: recovered, resuming native opens", g.addr)
		return
	}

	g.recent = append(g.recent, doltAdmissionOutcome{at: now, saturated: saturated})
	g.pruneLocked(now)

	satCount := 0
	for _, o := range g.recent {
		if o.saturated {
			satCount++
		}
	}
	if satCount >= doltSaturationMinSamples &&
		float64(satCount) >= doltSaturationTripFraction*float64(len(g.recent)) {
		g.open = true
		g.openedAt = now
		g.probeToken = 0
		log.Printf("[dolt-admission] %s: saturated, backing off collectively (%d/%d recent opens saturating; cooldown %s)",
			g.addr, satCount, len(g.recent), doltSaturationCooldown)
		g.recent = nil
	}
}

// pruneLocked drops outcomes older than the window. recent is append-ordered by
// time, so the stale entries form a prefix.
func (g *doltAdmissionGate) pruneLocked(now time.Time) {
	cutoff := now.Add(-doltSaturationWindow)
	i := 0
	for ; i < len(g.recent); i++ {
		if g.recent[i].at.After(cutoff) {
			break
		}
	}
	if i > 0 {
		g.recent = append(g.recent[:0], g.recent[i:]...)
	}
}

// doltServerSaturationOutcome classifies an open error as a saturation signal.
// Only signals that indicate the server is overloaded (rather than down or
// returning a query-level error) count toward collective backoff.
func doltServerSaturationOutcome(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "i/o timeout"):
		return true
	case strings.Contains(s, "too many connections"): // MySQL ER_CON_COUNT_ERROR (1040)
		return true
	case strings.Contains(s, "context deadline exceeded"): // open timed out under load
		return true
	case strings.Contains(s, "connection timed out"):
		return true
	}
	return false
}

// doltAdmissionGates holds one gate per Dolt server address for the process.
var doltAdmissionGates sync.Map // addr string -> *doltAdmissionGate

// doltAdmissionGateFor returns the shared gate for a server address, or nil when
// admission control does not apply: no resolvable server address, or test mode
// (where tests manage their own server lifecycle and a real-saturation gate
// would produce cascading false trips, mirroring the beads breaker's
// BEADS_TEST_MODE bypass).
func doltAdmissionGateFor(addr string) *doltAdmissionGate {
	if addr == "" {
		return nil
	}
	if os.Getenv("BEADS_TEST_MODE") == "1" {
		return nil
	}
	if g, ok := doltAdmissionGates.Load(addr); ok {
		return g.(*doltAdmissionGate)
	}
	g, _ := doltAdmissionGates.LoadOrStore(addr, newDoltAdmissionGate(addr))
	return g.(*doltAdmissionGate)
}

// doltServerAddrFromEnv resolves the shared Dolt server address (host:port) from
// a scoped open environment, falling back to the ambient process environment.
// It returns "" when either coordinate is absent — embedded/standalone opens
// have no shared-server ceiling to protect, so they are not gated.
func doltServerAddrFromEnv(env map[string]string) string {
	host := envOrAmbient(env, "BEADS_DOLT_SERVER_HOST")
	if host == "" {
		return ""
	}
	port := envOrAmbient(env, "BEADS_DOLT_SERVER_PORT")
	if port == "" {
		port = envOrAmbient(env, "BEADS_DOLT_PORT")
	}
	if port == "" {
		return ""
	}
	return host + ":" + port
}

// envOrAmbient reads key from the scoped env map when present and non-empty,
// otherwise from the process environment.
func envOrAmbient(env map[string]string, key string) string {
	if env != nil {
		if v, ok := env[key]; ok {
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				return trimmed
			}
		}
	}
	return strings.TrimSpace(os.Getenv(key))
}

// resetDoltAdmissionGatesForTest clears the process-wide gate registry. Tests
// use it to isolate gate state between cases.
func resetDoltAdmissionGatesForTest() {
	doltAdmissionGates.Range(func(k, _ any) bool {
		doltAdmissionGates.Delete(k)
		return true
	})
}
