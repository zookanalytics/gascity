package beads

import (
	"fmt"
	"testing"
	"time"
)

// fakeAdmissionClock is a deterministic clock for admission-gate tests.
type fakeAdmissionClock struct {
	t time.Time
}

func (c *fakeAdmissionClock) now() time.Time          { return c.t }
func (c *fakeAdmissionClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestAdmissionGate() (*doltAdmissionGate, *fakeAdmissionClock) {
	clk := &fakeAdmissionClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	g := newDoltAdmissionGate("dolt.example:3307")
	g.now = clk.now
	return g, clk
}

// admits reports whether the gate admits, discarding the token. Used by tests
// that only care about the admit/reject decision, not probe identity.
func admits(g *doltAdmissionGate) bool {
	admitted, _ := g.Admit()
	return admitted
}

// recordSaturated injects n saturation outcomes from ordinary (non-probe) opens
// admitted while the gate was closed, so they carry the zero token.
func recordSaturated(g *doltAdmissionGate, n int) {
	for i := 0; i < n; i++ {
		g.RecordOutcome(0, fmt.Errorf("dial tcp 127.0.0.1:3307: i/o timeout"))
	}
}

func recordOK(g *doltAdmissionGate, n int) {
	for i := 0; i < n; i++ {
		g.RecordOutcome(0, nil)
	}
}

func TestDoltAdmissionGateAdmitsByDefault(t *testing.T) {
	g, _ := newTestAdmissionGate()
	if !admits(g) {
		t.Fatal("fresh gate should admit")
	}
}

func TestDoltAdmissionGateAdmitsWithZeroTokenWhileClosed(t *testing.T) {
	g, _ := newTestAdmissionGate()
	// Ordinary opens admitted while the gate is closed are not probes, so they
	// must carry the zero token; otherwise a closed-gate open could later be
	// mistaken for a probe and resolve the gate.
	admitted, token := g.Admit()
	if !admitted {
		t.Fatal("fresh gate should admit")
	}
	if token != 0 {
		t.Fatalf("closed-gate admit token = %d, want 0", token)
	}
}

func TestDoltAdmissionGateTripsOnSaturationCluster(t *testing.T) {
	g, _ := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	if admits(g) {
		t.Fatalf("gate should back off after %d saturation outcomes", doltSaturationMinSamples)
	}
}

func TestDoltAdmissionGateIgnoresNonSaturationErrors(t *testing.T) {
	g, _ := newTestAdmissionGate()
	// Hard-down refusals are owned by the per-database breaker, not the
	// saturation gate. They must not trip collective backoff.
	for i := 0; i < doltSaturationMinSamples*3; i++ {
		g.RecordOutcome(0, fmt.Errorf("dial tcp 127.0.0.1:3307: connect: connection refused"))
	}
	if !admits(g) {
		t.Fatal("connection-refused outcomes must not trip the saturation gate")
	}
}

// TestDoltAdmissionGateTripsDespiteInterleavedSuccesses is the headline
// behavior: unlike the per-database breaker (which resets its counter on every
// success), the saturation gate evaluates a failure *ratio* over a window, so
// interleaved successes under saturation do not prevent backoff.
func TestDoltAdmissionGateTripsDespiteInterleavedSuccesses(t *testing.T) {
	g, _ := newTestAdmissionGate()
	// 6 saturated + 3 succeeded = 9 samples, ratio 0.67 >= trip fraction.
	g.RecordOutcome(0, nil)
	recordSaturated(g, 2)
	g.RecordOutcome(0, nil)
	recordSaturated(g, 2)
	g.RecordOutcome(0, nil)
	recordSaturated(g, 2)
	if admits(g) {
		t.Fatal("gate should back off when a majority of recent opens saturate, even with interleaved successes")
	}
}

func TestDoltAdmissionGateStaysOpenBelowTripFraction(t *testing.T) {
	g, _ := newTestAdmissionGate()
	// A healthy server with the occasional timeout: enough saturation samples
	// to clear the minimum, but interleaved with successes so the failure ratio
	// stays below the trip fraction at every step. 5 failures among 20 opens
	// (ratio 0.25) must not back the fleet off.
	for i := 0; i < doltSaturationMinSamples; i++ {
		recordOK(g, 3)
		recordSaturated(g, 1)
	}
	if !admits(g) {
		t.Fatal("a low saturation ratio must not trip the gate")
	}
}

func TestDoltAdmissionGateWindowExpiry(t *testing.T) {
	g, clk := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples-1)
	clk.advance(doltSaturationWindow + time.Second)
	// The earlier failures have aged out of the window; one fresh failure is
	// far below the minimum sample count.
	recordSaturated(g, 1)
	if !admits(g) {
		t.Fatal("stale failures outside the window must not count toward tripping")
	}
}

func TestDoltAdmissionGateProbeRecoversOnSuccess(t *testing.T) {
	g, clk := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	if admits(g) {
		t.Fatal("gate should be open immediately after tripping")
	}
	// Before cooldown elapses, no probe is allowed.
	clk.advance(doltSaturationCooldown - time.Second)
	if admits(g) {
		t.Fatal("gate should reject before cooldown elapses")
	}
	// After cooldown, exactly one probe is admitted with a non-zero token.
	clk.advance(2 * time.Second)
	admitted, token := g.Admit()
	if !admitted {
		t.Fatal("gate should admit a single probe after cooldown")
	}
	if token == 0 {
		t.Fatal("an admitted probe must carry a non-zero token")
	}
	if admits(g) {
		t.Fatal("gate should reject additional callers while a probe is in flight")
	}
	// The probe succeeds: the gate closes and admits normally again.
	g.RecordOutcome(token, nil)
	if !admits(g) {
		t.Fatal("gate should close after a successful probe")
	}
}

func TestDoltAdmissionGateProbeFailureReArmsCooldown(t *testing.T) {
	g, clk := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	clk.advance(doltSaturationCooldown + time.Second)
	admitted, token := g.Admit()
	if !admitted {
		t.Fatal("gate should admit a probe after cooldown")
	}
	// The probe still saturates: stay open and re-arm the cooldown.
	g.RecordOutcome(token, fmt.Errorf("dial tcp 127.0.0.1:3307: i/o timeout"))
	if admits(g) {
		t.Fatal("gate should stay open after a failed probe")
	}
	// Another cooldown later, a fresh probe is allowed again.
	clk.advance(doltSaturationCooldown + time.Second)
	if !admits(g) {
		t.Fatal("gate should admit a new probe after re-armed cooldown")
	}
}

func TestDoltAdmissionGateStragglerSuccessDoesNotCancelBackoff(t *testing.T) {
	g, _ := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	// A success from an open admitted just before the trip arrives late. It
	// must not cancel the active backoff (no probe is in flight yet).
	g.RecordOutcome(0, nil)
	if admits(g) {
		t.Fatal("a straggler success must not cancel collective backoff")
	}
}

// TestDoltAdmissionGateStragglerSuccessDuringProbeIsIgnored asserts that a
// success from a pre-trip straggler open, landing during a later probe window,
// does not close the gate — only the live probe's own outcome may.
func TestDoltAdmissionGateStragglerSuccessDuringProbeIsIgnored(t *testing.T) {
	g, clk := newTestAdmissionGate()

	// An ordinary open is admitted while the gate is still closed; it will take
	// a long time to return.
	admitted, stragglerToken := g.Admit()
	if !admitted {
		t.Fatal("closed gate should admit the straggler open")
	}
	if stragglerToken != 0 {
		t.Fatalf("closed-gate straggler token = %d, want 0", stragglerToken)
	}

	// The server saturates and the gate trips while the straggler is in flight.
	recordSaturated(g, doltSaturationMinSamples)

	// After cooldown a real probe is admitted, with its own non-zero token.
	clk.advance(doltSaturationCooldown + time.Second)
	probeAdmitted, probeToken := g.Admit()
	if !probeAdmitted {
		t.Fatal("gate should admit a probe after cooldown")
	}
	if probeToken == stragglerToken {
		t.Fatal("probe and straggler must have distinct tokens")
	}

	// The straggler finally returns successfully, inside the probe window. Its
	// stale success must NOT close the gate.
	g.RecordOutcome(stragglerToken, nil)
	if admits(g) {
		t.Fatal("a straggler success during a probe window must not close the gate")
	}

	// The real probe then succeeds and the gate recovers as normal.
	g.RecordOutcome(probeToken, nil)
	if !admits(g) {
		t.Fatal("gate should close after the real probe succeeds")
	}
}

// TestDoltAdmissionGateStragglerTimeoutDuringProbeDoesNotDiscardRealProbe is the
// mirror case: a straggler timeout during a probe window must not consume the
// probe slot, so the real probe's later success still closes the gate.
func TestDoltAdmissionGateStragglerTimeoutDuringProbeDoesNotDiscardRealProbe(t *testing.T) {
	g, clk := newTestAdmissionGate()

	admitted, stragglerToken := g.Admit()
	if !admitted {
		t.Fatal("closed gate should admit the straggler open")
	}

	recordSaturated(g, doltSaturationMinSamples)

	clk.advance(doltSaturationCooldown + time.Second)
	probeAdmitted, probeToken := g.Admit()
	if !probeAdmitted {
		t.Fatal("gate should admit a probe after cooldown")
	}

	// The straggler times out inside the probe window. It must be ignored, not
	// re-arm the cooldown.
	g.RecordOutcome(stragglerToken, fmt.Errorf("dial tcp 127.0.0.1:3307: i/o timeout"))

	// The real probe SUCCEEDS. Because the straggler didn't hijack the probe
	// slot, this success still resolves recovery and closes the gate.
	g.RecordOutcome(probeToken, nil)
	if !admits(g) {
		t.Fatal("the real probe's success must still close the gate despite a straggler timeout")
	}
}

func TestDoltAdmissionGateLostProbeReArms(t *testing.T) {
	g, clk := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	clk.advance(doltSaturationCooldown + time.Second)
	if !admits(g) {
		t.Fatal("gate should admit a probe after cooldown")
	}
	// The probe's outcome is never recorded (e.g. a panic between admit and
	// open). After the probe timeout, the gate re-arms a fresh probe rather
	// than wedging open forever.
	clk.advance(doltSaturationProbeTimeout + time.Second)
	if !admits(g) {
		t.Fatal("gate should re-arm a probe whose outcome was never recorded")
	}
}

// TestDoltAdmissionGateLostProbeStragglerCannotResolveReArmedProbe pins the
// re-arm path: once a timed-out probe is superseded, its late outcome carries a
// stale token and must not resolve the freshly re-armed probe.
func TestDoltAdmissionGateLostProbeStragglerCannotResolveReArmedProbe(t *testing.T) {
	g, clk := newTestAdmissionGate()
	recordSaturated(g, doltSaturationMinSamples)
	clk.advance(doltSaturationCooldown + time.Second)
	_, lostProbeToken := g.Admit()

	// The first probe never reports back; after the probe timeout a fresh probe
	// is admitted with a new token.
	clk.advance(doltSaturationProbeTimeout + time.Second)
	admitted, reArmedToken := g.Admit()
	if !admitted {
		t.Fatal("gate should re-arm a probe whose outcome was never recorded")
	}
	if lostProbeToken == reArmedToken {
		t.Fatal("re-armed probe must have a distinct token from the lost probe")
	}

	// The lost probe finally returns successfully — but it is stale and must not
	// resolve the re-armed probe's slot.
	g.RecordOutcome(lostProbeToken, nil)
	if admits(g) {
		t.Fatal("a superseded probe's late success must not resolve the re-armed probe")
	}

	// Only the re-armed probe's own outcome closes the gate.
	g.RecordOutcome(reArmedToken, nil)
	if !admits(g) {
		t.Fatal("gate should close after the re-armed probe succeeds")
	}
}

func TestDoltServerSaturationOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"io timeout", fmt.Errorf("read tcp: i/o timeout"), true},
		{"too many connections", fmt.Errorf("Error 1040: Too many connections"), true},
		{"context deadline", fmt.Errorf("connecting: context deadline exceeded"), true},
		{"connection timed out", fmt.Errorf("dial tcp: connection timed out"), true},
		{"connection refused", fmt.Errorf("dial tcp: connect: connection refused"), false},
		{"unrelated", fmt.Errorf("table not found"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := doltServerSaturationOutcome(tc.err); got != tc.want {
				t.Fatalf("doltServerSaturationOutcome(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestDoltServerAddrFromEnv(t *testing.T) {
	t.Run("server host and port from map", func(t *testing.T) {
		addr := doltServerAddrFromEnv(map[string]string{
			"BEADS_DOLT_SERVER_HOST": "dolt.example",
			"BEADS_DOLT_SERVER_PORT": "3307",
		})
		if addr != "dolt.example:3307" {
			t.Fatalf("addr = %q, want dolt.example:3307", addr)
		}
	})
	t.Run("falls back to BEADS_DOLT_PORT", func(t *testing.T) {
		addr := doltServerAddrFromEnv(map[string]string{
			"BEADS_DOLT_SERVER_HOST": "dolt.example",
			"BEADS_DOLT_PORT":        "3307",
		})
		if addr != "dolt.example:3307" {
			t.Fatalf("addr = %q, want dolt.example:3307", addr)
		}
	})
	t.Run("missing host yields empty", func(t *testing.T) {
		// Clear ambient host so the map miss is decisive on hosts that export it.
		t.Setenv("BEADS_DOLT_SERVER_HOST", "")
		if addr := doltServerAddrFromEnv(map[string]string{"BEADS_DOLT_SERVER_PORT": "3307"}); addr != "" {
			t.Fatalf("addr = %q, want empty when host is unset", addr)
		}
	})
	t.Run("missing port yields empty", func(t *testing.T) {
		t.Setenv("BEADS_DOLT_SERVER_PORT", "")
		t.Setenv("BEADS_DOLT_PORT", "")
		if addr := doltServerAddrFromEnv(map[string]string{"BEADS_DOLT_SERVER_HOST": "dolt.example"}); addr != "" {
			t.Fatalf("addr = %q, want empty when port is unset", addr)
		}
	})
	t.Run("ambient fallback", func(t *testing.T) {
		t.Setenv("BEADS_DOLT_SERVER_HOST", "ambient.example")
		t.Setenv("BEADS_DOLT_SERVER_PORT", "4407")
		if addr := doltServerAddrFromEnv(nil); addr != "ambient.example:4407" {
			t.Fatalf("addr = %q, want ambient.example:4407", addr)
		}
	})
	t.Run("non-nil env authoritative: absent host ignores ambient", func(t *testing.T) {
		// withNativeDoltOpenEnv unsets BEADS_DOLT_SERVER_HOST for the open when a
		// scoped env omits it, so the gate must resolve "" rather than key on an
		// unrelated ambient server. Ambient is populated to prove the scoped map —
		// not os.Getenv — decides the address.
		t.Setenv("BEADS_DOLT_SERVER_HOST", "ambient.example")
		t.Setenv("BEADS_DOLT_SERVER_PORT", "4407")
		if addr := doltServerAddrFromEnv(map[string]string{"BEADS_DOLT_SERVER_PORT": "3307"}); addr != "" {
			t.Fatalf("addr = %q, want empty: a non-nil scoped env without a host must not fall back to ambient", addr)
		}
	})
	t.Run("non-nil env authoritative: empty port ignores ambient", func(t *testing.T) {
		// mirrorBeadsDoltEnv writes BEADS_DOLT_SERVER_PORT="" so child opens cannot
		// inherit stale ambient Dolt settings; projection unsets it. The gate must
		// treat the explicit empty as "no port", matching the open, not fall back
		// to ambient and reject against the wrong host:port.
		t.Setenv("BEADS_DOLT_SERVER_PORT", "4407")
		t.Setenv("BEADS_DOLT_PORT", "4407")
		addr := doltServerAddrFromEnv(map[string]string{
			"BEADS_DOLT_SERVER_HOST": "dolt.example",
			"BEADS_DOLT_SERVER_PORT": "",
		})
		if addr != "" {
			t.Fatalf("addr = %q, want empty: an explicit empty scoped port must not fall back to ambient", addr)
		}
	})
}

func TestDoltAdmissionGateForSharesPerAddr(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	t.Setenv("BEADS_TEST_MODE", "")

	a1 := doltAdmissionGateFor("host-a:3307")
	a2 := doltAdmissionGateFor("host-a:3307")
	b := doltAdmissionGateFor("host-b:3307")
	if a1 == nil || b == nil {
		t.Fatal("gate lookup returned nil for valid addrs")
	}
	if a1 != a2 {
		t.Fatal("same addr must share one gate (collective backoff)")
	}
	if a1 == b {
		t.Fatal("different addrs must have independent gates")
	}
}

func TestDoltAdmissionGateForSkipsWhenUnconfigured(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)

	t.Run("empty addr", func(t *testing.T) {
		t.Setenv("BEADS_TEST_MODE", "")
		if doltAdmissionGateFor("") != nil {
			t.Fatal("empty addr should yield no gate")
		}
	})
	t.Run("test mode", func(t *testing.T) {
		t.Setenv("BEADS_TEST_MODE", "1")
		if doltAdmissionGateFor("host-a:3307") != nil {
			t.Fatal("test mode should bypass the gate")
		}
	})
}
