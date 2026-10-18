package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/beads/queryindex"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doltpool"
)

// Bead-store query index lane timing. A pass reads each store's catalog, a few
// information_schema rows, and builds what is missing. A build first waits
// for its table to hold still, then takes tens of seconds on a store of tens
// of thousands of beads.
const (
	beadStoreIndexFirstPass    = time.Minute
	beadStoreIndexPassInterval = 10 * time.Minute
	// beadStoreIndexBuildWait bounds the wait for a CREATE INDEX reply.
	// The managed server cancels a statement after its own read timeout
	// first, so this only has to outlast that.
	beadStoreIndexBuildWait = 10 * time.Minute
)

// newBeadStoreIndexMaintainer returns the maintainer the controller runs. A
// table must hold still for 30s, read every 5s for up to 5 minutes, before a
// build starts on it; builds are a minute apart; a failed build is retried
// after an hour, doubling to a day.
func newBeadStoreIndexMaintainer(stderr io.Writer, logPrefix string) *queryindex.Maintainer {
	return &queryindex.Maintainer{
		Quiet:       queryindex.QuietOptions{QuietFor: 30 * time.Second, Poll: 5 * time.Second, MaxWait: 5 * time.Minute},
		Spacing:     time.Minute,
		BaseBackoff: time.Hour,
		MaxBackoff:  24 * time.Hour,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(stderr, "%s: bead-store-index: %s\n", logPrefix, fmt.Sprintf(format, args...)) //nolint:errcheck // best-effort stderr
		},
	}
}

// startBeadStoreIndexLane runs the bead-store query index maintainer on its
// own goroutine until ctx ends: a first pass a minute after start, then one
// every beadStoreIndexPassInterval. Each pass reads the current config and
// re-inspects every store, so a new rig, a newly declared metadata key, and a
// table a beads migration rebuilt without its indexes are all picked up. The
// returned channel closes when the lane has stopped.
func (cr *CityRuntime) startBeadStoreIndexLane(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		m := newBeadStoreIndexMaintainer(cr.stderr, cr.logPrefix)
		timer := time.NewTimer(beadStoreIndexFirstPass)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			cr.safeTick(func() { cr.beadStoreIndexPass(ctx, m) }, "bead-store-index")
			timer.Reset(beadStoreIndexPassInterval)
		}
	}()
	return done
}

// beadStoreIndexPass runs one maintainer pass over the city's bead stores. A
// suspended city is left alone, like every other store-reading lane.
func (cr *CityRuntime) beadStoreIndexPass(ctx context.Context, m *queryindex.Maintainer) {
	if gcDoltSkip() {
		return
	}
	cfg := cr.serviceConfigSnapshot()
	if cfg == nil || effectiveCitySuspended(cfg, loadSuspensionStateBestEffort(cr.cityPath)) {
		return
	}
	want, err := queryindex.Expected(cfg.MetadataIndexKeys())
	if err != nil {
		m.Logf("%v", err)
		return
	}
	// Pass returns only ctx's error, and the lane loop stops on ctx itself.
	if err := m.Pass(ctx, beadStoreIndexStores(cr.cityPath, cfg, m.Logf), want); err != nil {
		return
	}
}

// beadStoreIndexStores returns a queryindex.Store for every bead store whose
// Dolt server gc owns and reaches directly: the scopes the blocked-flag repair
// writes to (blockedRepairScopes), minus suspended ones.
func beadStoreIndexStores(cityPath string, cfg *config.City, logf func(string, ...any)) []queryindex.Store {
	var stores []queryindex.Store
	for _, scope := range withoutSuspendedRepairScopes(blockedRepairScopes(cityPath, cfg), suspendedBeadsScopes(cityPath, cfg)) {
		store, ok, err := beadStoreIndexStore(cityPath, scope.root, scope.id)
		if err != nil {
			logf("%s: %v", scope.id, err)
			continue
		}
		if ok {
			stores = append(stores, store)
		}
	}
	return stores
}

// beadStoreIndexStore returns the queryindex.Store for the bead store at
// scopeRoot, labeled label. It reports false, with no error, for a scope with
// no canonical Dolt target and for a proxied-server scope, which gc reaches
// only by starting its proxy.
func beadStoreIndexStore(cityPath, scopeRoot, label string) (queryindex.Store, bool, error) {
	target, ok, err := canonicalScopeDoltTarget(cityPath, scopeRoot)
	if err != nil {
		return queryindex.Store{}, false, fmt.Errorf("resolving the Dolt endpoint: %w", err)
	}
	if !ok || strings.EqualFold(strings.TrimSpace(target.DoltMode), "proxied-server") {
		return queryindex.Store{}, false, nil
	}
	db, err := openScopeDoltDatabase(target)
	if err != nil {
		return queryindex.Store{}, false, fmt.Errorf("opening the Dolt database: %w", err)
	}
	return queryindex.SQLStore(label, db, func(context.Context) (*sql.DB, error) {
		return openScopeDoltDedicated(target, beadStoreIndexBuildWait)
	}), true, nil
}

// openScopeDoltDatabase returns the pooled handle for a scope's Dolt
// database. The handle is owned by internal/doltpool; do not Close it.
func openScopeDoltDatabase(target contract.DoltConnectionTarget) (*sql.DB, error) {
	if socket := strings.TrimSpace(target.Socket); socket != "" {
		return managedDoltOpenDatabaseSocket(socket, target.User, target.Database)
	}
	return managedDoltOpenDatabase(target.Host, target.Port, target.User, target.Database)
}

// openScopeDoltDedicated opens an unpooled handle to a scope's Dolt database
// whose replies may take up to wait. The caller closes it.
func openScopeDoltDedicated(target contract.DoltConnectionTarget, wait time.Duration) (*sql.DB, error) {
	user := strings.TrimSpace(target.User)
	if user == "" {
		user = "root"
	}
	database := strings.TrimSpace(target.Database)
	if database == "" {
		return nil, fmt.Errorf("missing database")
	}
	if socket := strings.TrimSpace(target.Socket); socket != "" {
		return doltpool.OpenDedicatedSocket(socket, user, managedDoltPassword(), database, wait)
	}
	port := strings.TrimSpace(target.Port)
	if port == "" {
		return nil, fmt.Errorf("missing port")
	}
	return doltpool.OpenDedicated(managedDoltConnectHost(target.Host), port, user, managedDoltPassword(), database, wait)
}
