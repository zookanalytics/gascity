package main

import (
	"io"
	"maps"
	"slices"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// demandMigrationsCheckFor builds the check over fixed stores: the city store
// at cityPath and each configured rig's store at its path. The session rows
// go into the city store, where the check's session census reads them.
func demandMigrationsCheckFor(t *testing.T, cfg *config.City, cityPath string, city beads.Store, rigs map[string]beads.Store, sessions ...beads.Bead) *v2DemandMigrationsCheck {
	t.Helper()
	for _, s := range sessions {
		if _, err := city.Create(s); err != nil {
			t.Fatalf("Create(%s): %v", s.ID, err)
		}
	}
	byPath := map[string]beads.Store{cityPath: city}
	for _, rig := range cfg.Rigs {
		byPath[rig.Path] = rigs[rig.Name]
	}
	return newV2DemandMigrationsCheck(cfg, cityPath, func(path string) (beads.Store, error) {
		return byPath[path], nil
	})
}

// goldenDemandMigrationsCheck is the check over the backstop lane's repair
// golden fixture, whose stores log every write.
func goldenDemandMigrationsCheck(t *testing.T) (*v2DemandMigrationsCheck, repairGoldenFixture) {
	t.Helper()
	f := newRepairGoldenFixture(t)
	c := demandMigrationsCheckFor(t, f.env.Cfg, f.env.CityPath, f.env.CityStore, f.env.RigStores,
		stampTestSession(clobberSessionName, clobberLiveWorkDir),
		stampTestSession(goldenStepSession, goldenStepWorkDir))
	return c, f
}

// Kills: a dry run that hands the repairs the real stores, or a recording
// wrapper that forwards a write.
func TestDoctorV2DemandMigrationsDryRunWritesNothing(t *testing.T) {
	c, f := goldenDemandMigrationsCheck(t)
	f.env.Cfg.Daemon.SessionReconciler = config.SessionReconcilerV2

	r := c.Run(&doctor.CheckContext{CityPath: f.env.CityPath})

	if len(f.log.ops) != 0 {
		t.Fatalf("dry run wrote %q, want no writes", f.log.ops)
	}
	if r.Status != doctor.StatusWarning {
		t.Fatalf("status = %v (%s), want warning for rows needing repair under v2", r.Status, r.Message)
	}
}

// Kills: a repair dropped from the sweep, a count attributed to the wrong
// repair or leg, a collection read from the wrong leg set, and a clean store
// reported as needing repair. Under legacy a non-zero count is OK (the legacy
// demand pass applies these every tick); under v2 it is a warning.
func TestDoctorV2DemandMigrationsCountsEachRepair(t *testing.T) {
	clobbered := func(b beads.Bead) beads.Bead {
		maps.Copy(b.Metadata, clobberedWorkDir())
		return b
	}
	cases := []struct {
		name string
		city []beads.Bead
		rig  []beads.Bead // in rig "fixture"
		want map[demandMigrationKey]int
	}{
		{
			name: "clean",
			city: []beads.Bead{
				workBead("ga-ok", goldenCanonicalPlanner, "", "open", 5),
				workBead("ga-run", goldenCanonicalPlanner, goldenCanonicalPlanner, "in_progress", 5),
			},
		},
		{
			name: "work dir clobber on a rig leg",
			rig:  []beads.Bead{clobbered(workBead("fx-run", "", clobberSessionName, "in_progress", 5))},
			want: map[demandMigrationKey]int{{"repairPoolSlotWorkDirClobber", "rig:fixture"}: 1},
		},
		{
			name: "legacy bound assigned",
			city: []beads.Bead{workBead("ga-asg", goldenLegacyPlanner, goldenLegacyPlanner, "in_progress", 5)},
			want: map[demandMigrationKey]int{{"canonicalizeLegacyBoundAssignedWork", "city"}: 1},
		},
		{
			name: "legacy bound unassigned routed",
			city: []beads.Bead{workBead("ga-rlegacy", goldenLegacyPlanner, "", "open", 5)},
			want: map[demandMigrationKey]int{{"canonicalizeLegacyBoundUnassignedRoutedWork", "city"}: 1},
		},
		{
			name: "slot suffixed route",
			city: []beads.Bead{workBead("ga-slot", goldenCanonicalPlanner+"-2", "", "open", 5)},
			want: map[demandMigrationKey]int{{"collapseSlotSuffixedRoutedWork", "city"}: 1},
		},
	}
	for _, tc := range cases {
		for _, mode := range []string{config.SessionReconcilerLegacy, config.SessionReconcilerV2} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				f := newRepairGoldenFixture(t)
				cfg := f.env.Cfg
				cfg.Daemon.SessionReconciler = mode
				log := &writeLog{}
				city := writeLogStore{Store: beads.NewMemStoreFrom(0, tc.city, nil), log: log}
				rigs := map[string]beads.Store{
					"fixture": writeLogStore{Store: beads.NewMemStoreFrom(0, tc.rig, nil), log: log},
					"nodisp":  writeLogStore{Store: beads.NewMemStore(), log: log},
				}
				c := demandMigrationsCheckFor(t, cfg, f.env.CityPath, city, rigs, stampTestSession(clobberSessionName, clobberLiveWorkDir))

				sweep := c.sweep(nil, false, io.Discard)
				if len(sweep.problems) != 0 {
					t.Fatalf("problems = %q, want none", sweep.problems)
				}
				if !maps.Equal(sweep.counts, tc.want) {
					t.Errorf("counts = %v, want %v", sweep.counts, tc.want)
				}
				wantStatus := doctor.StatusOK
				if mode == config.SessionReconcilerV2 && len(tc.want) > 0 {
					wantStatus = doctor.StatusWarning
				}
				if r := c.Run(&doctor.CheckContext{CityPath: f.env.CityPath}); r.Status != wantStatus {
					t.Errorf("status = %v (%s), want %v", r.Status, r.Message, wantStatus)
				}
				if len(log.ops) != 0 {
					t.Errorf("dry run wrote %q", log.ops)
				}
			})
		}
	}
}

// Kills: --fix running a repair the dry run did not count, skipping one, or
// running them out of legacy order. The golden is the backstop lane's
// (TestBackstopDemandRepairsWriteLogGoldenInLegacyOrder) without the two
// writers that stay lane steps (the session stamp and the control-dispatcher
// route repair), plus one row that difference exposes: without the stamp,
// which legacy runs first and which would have stamped ga-root from its step,
// ga-root keeps its clobbered pool-slot work dir and the work-dir repair
// restores it. After the fix, a dry run counts nothing.
func TestDoctorV2DemandMigrationsFixAppliesLegacyRepairs(t *testing.T) {
	c, f := goldenDemandMigrationsCheck(t)
	f.env.Cfg.Daemon.SessionReconciler = config.SessionReconcilerV2
	ctx := &doctor.CheckContext{CityPath: f.env.CityPath}

	if err := c.Fix(ctx); err != nil {
		t.Fatalf("Fix: %v", err)
	}

	want := []string{
		"SetMetadataBatch ga-run " + beadmeta.WorkDirMetadataKey + "=" + clobberStaleLegacy,
		"Update ga-asg assignee=" + goldenCanonicalPlanner + " " + beadmeta.RoutedToMetadataKey + "=" + goldenCanonicalPlanner,
		"SetMetadataBatch ga-root " + beadmeta.WorkDirMetadataKey + "=" + clobberStaleLegacy,
		"SetMetadataBatch ga-rclob " + beadmeta.WorkDirMetadataKey + "=" + clobberStaleLegacy,
		"Update ga-rlegacy assignee=- " + beadmeta.RoutedToMetadataKey + "=" + goldenCanonicalPlanner,
		"Update ga-slot assignee=- " + beadmeta.RoutedToMetadataKey + "=" + goldenCanonicalPlanner,
	}
	if !slices.Equal(f.log.ops, want) {
		t.Errorf("fix writes\n got  %q\n want %q", f.log.ops, want)
	}
	if r := c.Run(ctx); r.Status != doctor.StatusOK {
		t.Errorf("after fix: status = %v (%s), want OK", r.Status, r.Message)
	}
}

// TestDoctorV2DemandMigrationsStopsOnceAbandoned: abandoned while it loads the
// session snapshot, the sweep finishes that load and starts neither work
// collection.
func TestDoctorV2DemandMigrationsStopsOnceAbandoned(t *testing.T) {
	run := func(probe *doctorAbandonProbe, ctx *doctor.CheckContext) {
		f := newRepairGoldenFixture(t)
		rigs := map[string]beads.Store{}
		for name, store := range f.env.RigStores {
			rigs[name] = probe.wrap(store)
		}
		demandMigrationsCheckFor(t, f.env.Cfg, f.env.CityPath, probe.wrap(f.env.CityStore), rigs).Run(ctx)
	}

	snapshot := newDoctorAbandonProbe()
	f := newRepairGoldenFixture(t)
	if _, err := loadSessionBeadSnapshot(cliSessionStore(snapshot.wrap(f.env.CityStore), f.env.Cfg, f.env.CityPath)); err != nil {
		t.Fatalf("loadSessionBeadSnapshot: %v", err)
	}
	snapshotReads := snapshot.reads.Load()

	full := newDoctorAbandonProbe()
	run(full, &doctor.CheckContext{})
	if full.reads.Load() <= snapshotReads {
		t.Fatalf("unabandoned run made %d reads, want more than the snapshot's %d", full.reads.Load(), snapshotReads)
	}

	probe := newDoctorAbandonProbe()
	run(probe, probe.ctx())
	if got := probe.reads.Load(); got != snapshotReads {
		t.Fatalf("abandoned run made %d reads, want only the snapshot's %d", got, snapshotReads)
	}
}

// TestDoctorV2DemandMigrationsStopsBetweenCollectionReads: abandoned as a rig
// leg's first assigned-work read returns, the sweep issues no further read on
// that leg, in the assigned collection or after it.
func TestDoctorV2DemandMigrationsStopsBetweenCollectionReads(t *testing.T) {
	// Only the fixture rig's store is probed, so the probe's first read is that
	// leg's first assigned-work read: the session snapshot reads the city store.
	run := func(probe *doctorAbandonProbe, ctx *doctor.CheckContext) {
		f := newRepairGoldenFixture(t)
		rigs := maps.Clone(f.env.RigStores)
		rigs["fixture"] = probe.wrap(rigs["fixture"])
		demandMigrationsCheckFor(t, f.env.Cfg, f.env.CityPath, f.env.CityStore, rigs).Run(ctx)
	}

	full := newDoctorAbandonProbe()
	run(full, &doctor.CheckContext{})
	if full.reads.Load() < 2 {
		t.Fatalf("unabandoned run made %d reads on the rig leg, want its several assigned-work reads", full.reads.Load())
	}

	probe := newDoctorAbandonProbe()
	run(probe, probe.ctx())
	if got := probe.reads.Load(); got != 1 {
		t.Fatalf("abandoned run made %d reads on the rig leg, want only the one in flight at the abandon", got)
	}
}
