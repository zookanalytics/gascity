package queryindex

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Verdicts caches SelfTest results by Dolt version, so stores served by one
// server version share one run. A self-test that cannot run is not cached.
// The zero value is ready to use, and a Verdicts is safe for concurrent use.
type Verdicts struct {
	mu        sync.Mutex
	byVersion map[string]SelfTestResult
}

// For returns store's Dolt version and its server's SelfTest verdict.
func (v *Verdicts) For(ctx context.Context, store Store) (string, SelfTestResult, error) {
	version, verdict, _, err := v.forStore(ctx, store)
	return version, verdict, err
}

// forStore is For, also reporting whether this call ran the self-test.
func (v *Verdicts) forStore(ctx context.Context, store Store) (version string, verdict SelfTestResult, fresh bool, err error) {
	version, err = store.DoltVersion(ctx)
	if err != nil {
		return "", SelfTestResult{}, false, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cached, ok := v.byVersion[version]; ok {
		return version, cached, false, nil
	}
	verdict, err = store.SelfTest(ctx)
	if err != nil {
		return version, SelfTestResult{}, false, fmt.Errorf("the Dolt %s index self-test did not run: %w", version, err)
	}
	if v.byVersion == nil {
		v.byVersion = map[string]SelfTestResult{}
	}
	v.byVersion[version] = verdict
	return version, verdict, true, nil
}

// Status is what one bead store holds of the indexes it should carry.
type Status struct {
	// Missing are the indexes the store lacks and a Maintainer builds.
	Missing []Index
	// Held are the metadata indexes the store lacks because its Dolt server
	// fails SelfTest.
	Held []Index
	// Exposed are the metadata indexes the store carries although its Dolt
	// server fails SelfTest: writes to their tables are at risk.
	Exposed []Index
	// Version is the store's Dolt version, and Verdict its self-test
	// result. Both are empty when want names no metadata index on a table
	// the store has.
	Version string
	Verdict SelfTestResult
}

// CheckStore reads store's Status for want. It consults the self-test only
// when want names a metadata index on a table the store has.
func CheckStore(ctx context.Context, store Store, want []Index, verdicts *Verdicts) (Status, error) {
	cat, err := store.Inspect(ctx, want)
	if err != nil {
		return Status{}, fmt.Errorf("reading indexes: %w", err)
	}
	var status Status
	missing := map[string]bool{}
	for _, ix := range cat.Missing(want) {
		missing[ix.Table+"\x00"+ix.Name] = true
		if ix.MetadataKey == "" {
			status.Missing = append(status.Missing, ix)
		}
	}
	var absent, present []Index
	for _, ix := range want {
		if ix.MetadataKey == "" || !cat.tables[strings.ToLower(ix.Table)] {
			continue
		}
		if missing[ix.Table+"\x00"+ix.Name] {
			absent = append(absent, ix)
		} else {
			present = append(present, ix)
		}
	}
	if len(absent)+len(present) == 0 {
		return status, nil
	}
	status.Version, status.Verdict, err = verdicts.For(ctx, store)
	if err != nil {
		return status, err
	}
	if status.Verdict.OK {
		status.Missing = append(status.Missing, absent...)
	} else {
		status.Held, status.Exposed = absent, present
	}
	return status, nil
}
