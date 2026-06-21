package beads

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	beadslib "github.com/steveyegge/beads"
)

// clearAmbientDoltServerEnv removes any ambient shared-server coordinates so a
// test's gate address comes solely from the scoped env map it passes.
func clearAmbientDoltServerEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BEADS_DOLT_SERVER_HOST", "")
	t.Setenv("BEADS_DOLT_SERVER_PORT", "")
	t.Setenv("BEADS_DOLT_PORT", "")
}

// stubNativeDoltOpen replaces the native open hook for the duration of a test.
func stubNativeDoltOpen(t *testing.T, open func(context.Context, string) (beadslib.Storage, error)) {
	t.Helper()
	prev := nativeDoltOpenBestAvailable
	t.Cleanup(func() { nativeDoltOpenBestAvailable = prev })
	nativeDoltOpenBestAvailable = open
}

// TestNewNativeDoltStoreAtFailsFastWhileProbeHoldsEnvMutex pins finding 1: a
// rejected open must fail fast with ErrDoltServerSaturated instead of serializing
// behind the open-env mutex that the in-flight probe holds for the whole native
// open. The gate is admitted before withNativeDoltOpenEnv, so holding the mutex
// here must not delay the rejection.
func TestNewNativeDoltStoreAtFailsFastWhileProbeHoldsEnvMutex(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	t.Setenv("BEADS_TEST_MODE", "")
	clearAmbientDoltServerEnv(t)

	// Keep the (buggy) blocked path off the network; the fixed path rejects at
	// Admit and never reaches the open.
	stubNativeDoltOpen(t, func(context.Context, string) (beadslib.Storage, error) {
		return &nativeDoltStorageSpy{
			getConfig: func(context.Context, string) (string, error) { return "gc", nil },
		}, nil
	})

	env := map[string]string{
		"BEADS_DOLT_SERVER_HOST": "sat.example",
		"BEADS_DOLT_SERVER_PORT": "3307",
	}
	gate := doltAdmissionGateFor(doltServerAddrFromEnv(env))
	if gate == nil {
		t.Fatal("precondition: expected a gate for the scoped server address")
	}
	recordSaturated(gate, doltSaturationMinSamples) // trip: backing off, within cooldown
	if admits(gate) {
		t.Fatal("precondition: gate should be backing off after a saturation cluster")
	}

	// Simulate the in-flight probe: hold the open-env mutex for the whole open.
	nativeDoltOpenEnvMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), env)
		done <- err
	}()

	select {
	case err := <-done:
		nativeDoltOpenEnvMu.Unlock()
		if !errors.Is(err, ErrDoltServerSaturated) {
			t.Fatalf("err = %v, want ErrDoltServerSaturated", err)
		}
	case <-time.After(2 * time.Second):
		nativeDoltOpenEnvMu.Unlock()
		t.Fatal("newNativeDoltStoreAt blocked on the open-env mutex instead of failing fast with ErrDoltServerSaturated")
	}
}

// TestNewNativeDoltStoreAtRecordsProbeOutcomeWhenEnvProjectionFails pins the
// second half of finding 1: an admitted probe must report an outcome even when
// env projection fails, or the gate wedges with a phantom in-flight probe until
// the probe timeout. A NUL byte in a projected value forces os.Setenv to fail.
func TestNewNativeDoltStoreAtRecordsProbeOutcomeWhenEnvProjectionFails(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	t.Setenv("BEADS_TEST_MODE", "")
	clearAmbientDoltServerEnv(t)

	addr := "sat.example:3307"
	clk := &fakeAdmissionClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	g := newDoltAdmissionGate(addr)
	g.now = clk.now
	doltAdmissionGates.Store(addr, g)
	recordSaturated(g, doltSaturationMinSamples)
	clk.advance(doltSaturationCooldown + time.Second) // the next Admit issues a probe

	env := map[string]string{
		"BEADS_DOLT_SERVER_HOST": "sat.example",
		"BEADS_DOLT_SERVER_PORT": "3307",
		"BEADS_DOLT_PASSWORD":    "bad\x00value", // NUL → projection fails
	}
	_, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), env)
	if err == nil {
		t.Fatal("expected env projection failure")
	}
	if errors.Is(err, ErrDoltServerSaturated) {
		t.Fatalf("env projection failure must not be reported as saturation: %v", err)
	}
	if !strings.Contains(err.Error(), "projecting native Dolt open env") {
		t.Fatalf("err = %v, want an env projection error", err)
	}
	// The probe slot must be released; otherwise the gate cannot admit again until
	// the probe timeout elapses.
	if !admits(g) {
		t.Fatal("env projection failure leaked the probe slot: the gate did not record an outcome")
	}
}

// TestNewNativeDoltStoreAtTranslatesPostOpenSaturationToSentinel pins finding 2:
// a successful handshake followed by a saturating config read must surface as
// ErrDoltServerSaturated (so the factory does not fall back to bd), and the
// half-open storage handle must be closed.
func TestNewNativeDoltStoreAtTranslatesPostOpenSaturationToSentinel(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	clearAmbientDoltServerEnv(t) // no gate: isolate the error translation

	var closed bool
	stubNativeDoltOpen(t, func(context.Context, string) (beadslib.Storage, error) {
		return &nativeDoltStorageSpy{
			getConfig: func(context.Context, string) (string, error) {
				return "", fmt.Errorf("read tcp 10.0.0.1:3307->10.0.0.2:5000: i/o timeout")
			},
			close: func() error { closed = true; return nil },
		}, nil
	})

	_, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), nil)
	if !errors.Is(err, ErrDoltServerSaturated) {
		t.Fatalf("err = %v, want wrapped ErrDoltServerSaturated", err)
	}
	if !closed {
		t.Fatal("storage was not closed after a failed config read")
	}
	if !strings.Contains(err.Error(), "issue prefix") {
		t.Fatalf("err = %v, want the issue-prefix context retained in the chain", err)
	}
}

// TestNewNativeDoltStoreAtPostOpenSaturationReArmsGateInsteadOfRecovering pins the
// "record the final open-boundary result" half of finding 2: the gate must
// observe the config-read timeout, not the successful dial, so a probe whose
// config read saturates re-arms the cooldown rather than falsely recovering.
func TestNewNativeDoltStoreAtPostOpenSaturationReArmsGateInsteadOfRecovering(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	t.Setenv("BEADS_TEST_MODE", "")
	clearAmbientDoltServerEnv(t)

	addr := "sat.example:3307"
	clk := &fakeAdmissionClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	g := newDoltAdmissionGate(addr)
	g.now = clk.now
	doltAdmissionGates.Store(addr, g)
	recordSaturated(g, doltSaturationMinSamples)
	clk.advance(doltSaturationCooldown + time.Second) // the next Admit issues a probe

	stubNativeDoltOpen(t, func(context.Context, string) (beadslib.Storage, error) {
		// The handshake succeeds, but the config read times out under load.
		return &nativeDoltStorageSpy{
			getConfig: func(context.Context, string) (string, error) {
				return "", fmt.Errorf("reading issue prefix: i/o timeout")
			},
			close: func() error { return nil },
		}, nil
	})

	env := map[string]string{
		"BEADS_DOLT_SERVER_HOST": "sat.example",
		"BEADS_DOLT_SERVER_PORT": "3307",
	}
	if _, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), env); !errors.Is(err, ErrDoltServerSaturated) {
		t.Fatalf("err = %v, want ErrDoltServerSaturated", err)
	}
	// The probe saturated at the config-read boundary, so the gate must stay
	// backed off. A fresh Admit before the re-armed cooldown elapses is rejected;
	// a falsely-recovered gate would admit.
	if admits(g) {
		t.Fatal("post-open saturation falsely recovered the gate; it should re-arm the cooldown")
	}
}

// TestNewNativeDoltStoreAtTranslatesDialSaturationToSentinel keeps the dial and
// config-read boundaries consistent: a saturating handshake is also surfaced as
// ErrDoltServerSaturated so the factory does not fall back into the same storm.
func TestNewNativeDoltStoreAtTranslatesDialSaturationToSentinel(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	clearAmbientDoltServerEnv(t)

	stubNativeDoltOpen(t, func(context.Context, string) (beadslib.Storage, error) {
		return nil, fmt.Errorf("dial tcp 10.0.0.1:3307: i/o timeout")
	})

	if _, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), nil); !errors.Is(err, ErrDoltServerSaturated) {
		t.Fatalf("err = %v, want ErrDoltServerSaturated for a saturating dial", err)
	}
}

// TestNewNativeDoltStoreAtPreservesNonSaturationDialError guards the translation
// scope: a hard-down error (owned by the per-database breaker) must pass through
// unchanged so the factory still falls back to the bd CLI.
func TestNewNativeDoltStoreAtPreservesNonSaturationDialError(t *testing.T) {
	resetDoltAdmissionGatesForTest()
	t.Cleanup(resetDoltAdmissionGatesForTest)
	clearAmbientDoltServerEnv(t)

	sentinel := errors.New("dial native: connect: connection refused")
	stubNativeDoltOpen(t, func(context.Context, string) (beadslib.Storage, error) {
		return nil, sentinel
	})

	_, err := newNativeDoltStoreAt(context.Background(), t.TempDir(), nil)
	if errors.Is(err, ErrDoltServerSaturated) {
		t.Fatalf("non-saturation dial error must not be translated to ErrDoltServerSaturated: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the original dial error preserved for bd fallback", err)
	}
}
