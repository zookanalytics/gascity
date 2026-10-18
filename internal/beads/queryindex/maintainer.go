package queryindex

import (
	"context"
	"errors"
	"time"
)

// Store is one bead store a Maintainer keeps indexed. SQLStore builds one over
// a Dolt connection.
type Store struct {
	// Label names the store in logs, such as "city" or "rig/gascity".
	Label string
	// Inspect reads the store's Catalog for want.
	Inspect func(ctx context.Context, want []Index) (Catalog, error)
	// TableHash reads a table's working-set hash.
	TableHash func(ctx context.Context, table string) (string, error)
	// Build creates an index and records it in the store's history.
	Build func(ctx context.Context, ix Index) error
	// DoltVersion reads the version of the store's Dolt server.
	DoltVersion func(ctx context.Context) (string, error)
	// SelfTest runs SelfTest against the store's Dolt server.
	SelfTest func(ctx context.Context) (SelfTestResult, error)
}

// Maintainer builds the indexes bead stores are missing. A pass builds one
// index at a time and starts each build only once its table's hash has held
// for Quiet.QuietFor, so the build's commit has no concurrent write to merge.
// A build that fails is retried after BaseBackoff, doubling on each further
// failure up to MaxBackoff. A table that never goes quiet is retried on the
// next pass with no backoff.
//
// Metadata indexes are built only on a Dolt server that passes SelfTest. The
// verdict is kept per Dolt version, so a server upgraded to a release that
// passes gets its metadata indexes on the next pass. A self-test that cannot
// run holds the metadata indexes for that pass and runs again on the next.
type Maintainer struct {
	// Quiet bounds the wait for a table to stop changing.
	Quiet QuietOptions
	// Spacing is the pause after each build, before the next one starts.
	Spacing time.Duration
	// BaseBackoff is the wait before retrying a failed build.
	BaseBackoff time.Duration
	// MaxBackoff caps the doubled wait.
	MaxBackoff time.Duration
	// Clock is the time source; nil uses the system clock.
	Clock Clock
	// Logf receives one line per build outcome; nil discards them.
	Logf func(format string, args ...any)

	failures map[string]buildFailure
	verdicts Verdicts
}

type buildFailure struct {
	next  time.Time
	delay time.Duration
}

// Pass visits every store in order and builds each index of want it is
// missing. It returns only ctx's error; every other failure is logged and
// either retried after its backoff or, for a store it could not read, on the
// next pass.
func (m *Maintainer) Pass(ctx context.Context, stores []Store, want []Index) error {
	for _, store := range stores {
		cat, err := store.Inspect(ctx, want)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			m.logf("%s: reading indexes: %v", store.Label, err)
			continue
		}
		missing := cat.Missing(want)
		if hasMetadataIndex(missing) {
			safe, err := m.metadataIndexesSafe(ctx, store)
			if err != nil {
				return err
			}
			if !safe {
				missing = withoutMetadataIndexes(missing)
			}
		}
		for _, ix := range missing {
			if err := m.ensure(ctx, store, ix); err != nil {
				return err
			}
		}
	}
	return nil
}

// metadataIndexesSafe reports whether store's Dolt server has passed
// SelfTest, running the test the first time a pass meets a server version. It
// returns only ctx's error.
func (m *Maintainer) metadataIndexesSafe(ctx context.Context, store Store) (bool, error) {
	version, verdict, fresh, err := m.verdicts.forStore(ctx, store)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if err != nil {
		m.logf("%s: metadata indexes held: %v", store.Label, err)
		return false, nil
	}
	if fresh && !verdict.OK {
		m.logf("metadata indexes held while the Dolt server runs %s: %s", version, verdict.Failure)
	}
	return verdict.OK, nil
}

func hasMetadataIndex(indexes []Index) bool {
	for _, ix := range indexes {
		if ix.MetadataKey != "" {
			return true
		}
	}
	return false
}

func withoutMetadataIndexes(indexes []Index) []Index {
	var out []Index
	for _, ix := range indexes {
		if ix.MetadataKey == "" {
			out = append(out, ix)
		}
	}
	return out
}

// ensure builds ix on store unless a recent failure is backing it off. It
// returns only ctx's error.
func (m *Maintainer) ensure(ctx context.Context, store Store, ix Index) error {
	clock := m.clock()
	key := store.Label + "\x00" + ix.Table + "\x00" + ix.Name
	if f, ok := m.failures[key]; ok && clock.Now().Before(f.next) {
		return nil
	}
	quiet, err := WaitQuiet(ctx, clock, func(ctx context.Context) (string, error) {
		return store.TableHash(ctx, ix.Table)
	}, m.Quiet)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		m.fail(key, store, ix, err)
		return nil
	}
	if !quiet {
		m.logf("%s: %s waits for a quiet table: it changed within every %s stretch for %s; the next pass retries",
			store.Label, ix, m.Quiet.QuietFor, m.Quiet.MaxWait)
		return nil
	}

	started := clock.Now()
	err = store.Build(ctx, ix)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err == nil {
		err = m.verify(ctx, store, ix)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if err != nil {
		m.fail(key, store, ix, err)
	} else {
		delete(m.failures, key)
		m.logf("%s: built %s as %s in %s", store.Label, ix, ix.Name, clock.Now().Sub(started).Round(time.Millisecond))
	}
	if err := clock.Sleep(ctx, m.Spacing); err != nil {
		return err
	}
	return nil
}

// errStillMissing reports a build that returned without error but left no
// index covering the key, as when an index of the same name with another
// definition already exists.
var errStillMissing = errors.New("still missing after the build")

func (m *Maintainer) verify(ctx context.Context, store Store, ix Index) error {
	cat, err := store.Inspect(ctx, []Index{ix})
	if err != nil {
		return err
	}
	if len(cat.Missing([]Index{ix})) > 0 {
		return errStillMissing
	}
	return nil
}

func (m *Maintainer) fail(key string, store Store, ix Index, err error) {
	if m.failures == nil {
		m.failures = map[string]buildFailure{}
	}
	delay := m.BaseBackoff
	if f, ok := m.failures[key]; ok {
		delay = min(2*f.delay, m.MaxBackoff)
	}
	m.failures[key] = buildFailure{next: m.clock().Now().Add(delay), delay: delay}
	m.logf("%s: building %s failed; next attempt in %s: %v", store.Label, ix, delay, err)
}

func (m *Maintainer) clock() Clock {
	if m.Clock == nil {
		return realClock{}
	}
	return m.Clock
}

func (m *Maintainer) logf(format string, args ...any) {
	if m.Logf != nil {
		m.Logf(format, args...)
	}
}
