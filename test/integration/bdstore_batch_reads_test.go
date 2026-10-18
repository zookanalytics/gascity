//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/doctor"
)

// TestBdStoreBatchedDispatchReadsMatchPerCallReads pins the two BdStore
// capabilities the control dispatcher uses to save bd calls (ga-vnycm2.18)
// against a real bd: ChildrenOfAny answers exactly the union of the per-parent
// Children reads (closed children included, ephemeral children excluded as
// `bd list --parent` excludes them), and UpdateReporting reports the row the
// update wrote, which a follow-up Get confirms.
func TestBdStoreBatchedDispatchReadsMatchPerCallReads(t *testing.T) {
	requireDoltIntegration(t)
	store := newRealBdStoreWorkspace(t, "bt")

	mk := func(b beads.Bead) beads.Bead {
		t.Helper()
		created, err := store.Create(b)
		if err != nil {
			t.Fatalf("create %s: %v", b.Title, err)
		}
		return created
	}
	p1 := mk(beads.Bead{Title: "parent one", Type: "task"})
	p2 := mk(beads.Bead{Title: "parent two", Type: "task"})
	c1 := mk(beads.Bead{Title: "child one", Type: "task", ParentID: p1.ID})
	mk(beads.Bead{Title: "child two", Type: "task", ParentID: p1.ID})
	mk(beads.Bead{Title: "child three", Type: "task", ParentID: p2.ID})
	mk(beads.Bead{Title: "ephemeral child", Type: "task", ParentID: p2.ID, Ephemeral: true})
	if err := store.Close(c1.ID); err != nil {
		t.Fatalf("close %s: %v", c1.ID, err)
	}

	var perParent []string
	for _, parent := range []string{p1.ID, p2.ID} {
		children, err := store.Children(parent, beads.IncludeClosed, beads.WithBothTiers)
		if err != nil {
			t.Fatalf("Children(%s): %v", parent, err)
		}
		for _, c := range children {
			perParent = append(perParent, c.ID+"<"+c.ParentID+":"+c.Status)
		}
	}
	batched, err := store.ChildrenOfAny([]string{p1.ID, p2.ID})
	if err != nil {
		t.Fatalf("ChildrenOfAny: %v", err)
	}
	var got []string
	for _, c := range batched {
		got = append(got, c.ID+"<"+c.ParentID+":"+c.Status)
	}
	slices.Sort(perParent)
	slices.Sort(got)
	if !slices.Equal(got, perParent) {
		t.Fatalf("ChildrenOfAny = %v, want the per-parent Children union %v", got, perParent)
	}
	if len(got) != 3 {
		t.Fatalf("children = %v, want the 3 non-ephemeral children", got)
	}

	leaf := mk(beads.Bead{Title: "leaf control", Type: "task", Metadata: map[string]string{"keep": "v"}})
	status := "closed"
	row, err := store.UpdateReporting(leaf.ID, beads.UpdateOpts{Status: &status, Metadata: map[string]string{"gc.outcome": "pass"}})
	if err != nil {
		t.Fatalf("UpdateReporting: %v", err)
	}
	after, err := store.Get(leaf.ID)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if row.Status != "closed" || row.Status != after.Status || row.Metadata["gc.outcome"] != "pass" || row.Metadata["keep"] != "v" || after.Metadata["gc.outcome"] != "pass" {
		t.Fatalf("UpdateReporting row = %+v, Get = status %q metadata %v; want both closed with gc.outcome=pass", row, after.Status, after.Metadata)
	}
}

// TestControlDispatchReadyAgreesBetweenBdAndNativeStores pins the ready set
// the control dispatcher's readiness scan reads, a FieldsCore Ready at
// TierBoth, as the same set whether its scope opens as a BdStore over the real
// bd or as the native store the linked beads library opens over the same
// database. The dispatcher filters only this set (by assignee, route, type,
// label and the instantiating mark), so a disagreement here offers a control
// bead on one transport that the other withholds.
//
// The corpus covers what the two reads compute differently. BdStore takes its
// candidates from `bd ready --brief` and vetoes one whose closed blocker
// recorded gc.work_outcome=blocked, reading the blockers by id with a
// closed-inclusive `bd list` that includes infra, gate and template rows. A
// `bd query` of closed wisps runs only when that list misses a blocker. The
// native store asks the library for the same core rows, its lite column set,
// and reads the candidates' edges and the blockers through the library's
// reader roles. So the blockers here close with and without the blocked
// outcome as a task, a step, a gate and a wisp, and the candidates carry the
// fields the dispatcher filters on afterwards.
func TestControlDispatchReadyAgreesBetweenBdAndNativeStores(t *testing.T) {
	requireDoltIntegration(t)
	bd, native := newRealBdAndNativeStores(t, "rp")

	mk := func(b beads.Bead) beads.Bead {
		t.Helper()
		created, err := bd.Create(b)
		if err != nil {
			t.Fatalf("create %q: %v", b.Title, err)
		}
		return created
	}
	closeWithOutcome := func(b beads.Bead, outcome string) {
		t.Helper()
		if outcome != "" {
			if err := bd.SetMetadata(b.ID, "gc.work_outcome", outcome); err != nil {
				t.Fatalf("set gc.work_outcome on %s: %v", b.ID, err)
			}
		}
		if err := bd.Close(b.ID); err != nil {
			t.Fatalf("close %s: %v", b.ID, err)
		}
	}
	behind := func(title string, blocker beads.Bead, ephemeral bool) beads.Bead {
		t.Helper()
		dependent := mk(beads.Bead{Title: title, Type: "task", Ephemeral: ephemeral})
		if err := bd.DepAdd(dependent.ID, blocker.ID, "blocks"); err != nil {
			t.Fatalf("DepAdd %s -> %s: %v", dependent.ID, blocker.ID, err)
		}
		return dependent
	}

	route := map[string]string{"gc.routed_to": "repo/core.control-dispatcher"}
	want := map[string]bool{}
	expect := func(b beads.Bead, ready bool) { want[b.ID] = ready }

	expect(mk(beads.Bead{Title: "routed control", Type: "task", Metadata: route}), true)
	expect(mk(beads.Bead{Title: "assigned control", Type: "task", Assignee: "repo--core.control-dispatcher"}), true)
	expect(mk(beads.Bead{Title: "routed wisp", Type: "task", Ephemeral: true, Metadata: route}), true)
	expect(mk(beads.Bead{Title: "epic", Type: "epic"}), true)
	expect(mk(beads.Bead{Title: "held control", Type: "task", Labels: []string{"hold:external"}, Metadata: route}), true)
	expect(mk(beads.Bead{Title: "instantiating control", Type: "task", Metadata: map[string]string{
		"gc.routed_to": "repo/core.control-dispatcher", "gc.instantiating": "1",
	}}), true)
	future := time.Now().Add(24 * time.Hour).UTC()
	expect(mk(beads.Bead{Title: "deferred control", Type: "task", DeferUntil: &future}), false)
	expect(mk(beads.Bead{Title: "step scaffolding", Type: "step"}), false)

	openBlocker := mk(beads.Bead{Title: "open blocker", Type: "task"})
	expect(openBlocker, true)
	expect(behind("behind an open blocker", openBlocker, false), false)

	for _, kind := range []struct {
		name      string
		typ       string
		ephemeral bool
	}{
		{name: "task", typ: "task"},
		{name: "step", typ: "step"},
		{name: "gate", typ: "gate"},
		{name: "wisp", typ: "task", ephemeral: true},
	} {
		passed := mk(beads.Bead{Title: kind.name + " blocker that passed", Type: kind.typ, Ephemeral: kind.ephemeral})
		closeWithOutcome(passed, "")
		expect(behind("behind a "+kind.name+" blocker that passed", passed, kind.ephemeral), true)

		blocked := mk(beads.Bead{Title: kind.name + " blocker closed blocked", Type: kind.typ, Ephemeral: kind.ephemeral})
		closeWithOutcome(blocked, "blocked")
		expect(behind("behind a "+kind.name+" blocker closed blocked", blocked, kind.ephemeral), false)
	}

	query := beads.ReadyQuery{TierMode: beads.TierBoth, Fields: beads.FieldsCore}
	viaBd, err := bd.Ready(query)
	if err != nil {
		t.Fatalf("BdStore.Ready(TierBoth, FieldsCore): %v", err)
	}
	viaNative, err := native.Ready(query)
	if err != nil {
		t.Fatalf("NativeDoltStore.Ready(TierBoth, FieldsCore): %v", err)
	}
	bdRows, nativeRows := controlReadyProjection(viaBd), controlReadyProjection(viaNative)
	if !slices.Equal(bdRows, nativeRows) {
		t.Fatalf("Ready(TierBoth, FieldsCore) differs between the stores:\n bd:     %s\n native: %s", strings.Join(bdRows, "\n         "), strings.Join(nativeRows, "\n         "))
	}
	got := map[string]bool{}
	for _, b := range viaNative {
		got[b.ID] = true
	}
	for id, ready := range want {
		if got[id] != ready {
			t.Errorf("bead %s ready = %v on both stores, want %v", id, got[id], ready)
		}
	}
}

// controlReadyProjection renders each ready bead as the fields the control
// dispatcher's routing reads, sorted by id, so two stores' answers compare as
// sets of the same rows.
func controlReadyProjection(ready []beads.Bead) []string {
	rows := make([]string, 0, len(ready))
	for _, b := range ready {
		labels := slices.Clone(b.Labels)
		slices.Sort(labels)
		rows = append(rows, strings.Join([]string{
			b.ID, b.Type, b.Status, b.Assignee,
			"ephemeral=" + boolString(b.Ephemeral),
			"routed_to=" + b.Metadata["gc.routed_to"],
			"instantiating=" + b.Metadata["gc.instantiating"],
			"labels=" + strings.Join(labels, ","),
		}, " "))
	}
	slices.Sort(rows)
	return rows
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// newRealBdAndNativeStores starts a Dolt server, initializes one bd workspace
// against it, and returns a BdStore running the isolated bd over the workspace
// and a NativeDoltStore the linked beads library opens over the same database.
func newRealBdAndNativeStores(t *testing.T, prefix string) (*beads.BdStore, *beads.NativeDoltStore) {
	t.Helper()
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, filepath.Join(rootDir, "dolt"))
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitInitWorkspace(t, wsDir)
	runBDInit(t, env, wsDir, prefix, serverPort)
	configureCustomTypes(t, env, wsDir, doctor.RequiredCustomTypes)

	native, err := beads.OpenNativeDoltStoreAt(context.Background(), wsDir, map[string]string{
		"BEADS_DOLT_AUTO_START":  "0",
		"BEADS_DOLT_SERVER_HOST": "127.0.0.1",
		"BEADS_DOLT_SERVER_PORT": serverPort,
	})
	if err != nil {
		t.Fatalf("opening the native store over %s: %v", wsDir, err)
	}
	t.Cleanup(func() {
		if err := native.CloseStore(); err != nil {
			t.Errorf("closing the native store: %v", err)
		}
	})
	return beads.NewBdStore(wsDir, isolatedBdStoreCommandRunner(env)), native
}
