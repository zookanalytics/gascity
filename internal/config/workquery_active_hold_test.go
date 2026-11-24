package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// Coverage for the generated queries' reading of an active hold
// (beadmeta.ActiveHoldValue): any hold:<value> with a nonempty value other than
// cleared is held, whatever the value names, and no reader flag can say so
// because bd's --exclude-label matches one label exactly. The tests EXECUTE the
// generated query against fakeFilteringReader, which honors the filter flags
// and the --limit a read carries, so a held row that takes a place in a read's
// window is observable as the unheld row behind it going unserved.

// activeHoldFixtures are hold labels the generated queries must treat as held:
// the two values the runtime always honored, pack-defined values, and a padded
// case variant.
func activeHoldFixtures() []string {
	return []string{"hold:mayor", "hold:external", "hold:po", "hold:release-train", " Hold:PO "}
}

// inactiveHoldFixtures are labels the generated queries must serve as ordinary
// work: the cleared state, malformed hold labels, and labels that only contain
// the word.
func inactiveHoldFixtures() []string {
	return []string{"hold:cleared", "Hold:Cleared", "hold:", "hold: ", "hold", "on-hold", "mpr-human-hold"}
}

// TestActiveHoldLabelJQAgreesWithBeadmeta runs the jq rendering of the active
// hold predicate over one label corpus beside beadmeta.ActiveHoldValue, so the
// shell tiers and the Go gates cannot disagree about what is held.
func TestActiveHoldLabelJQAgreesWithBeadmeta(t *testing.T) {
	requireJQ(t)
	corpus := append(append(activeHoldFixtures(), inactiveHoldFixtures()...),
		"hold::", "hold:\tpo\n", " hold: CLEARED ", "xhold:po", "hold-po", "HOLD:", "", "human")
	data, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	out := runJQFilter(t, "[.[] | "+activeHoldLabelJQ()+"]", string(data), "-c")
	var got []bool
	if err := json.Unmarshal([]byte(out), &got); err != nil || len(got) != len(corpus) {
		t.Fatalf("jq answered %q for %d labels: %v", out, len(corpus), err)
	}
	for i, label := range corpus {
		_, want := beadmeta.ActiveHoldValue(label)
		if got[i] != want {
			t.Errorf("label %q: jq says held=%v, beadmeta.ActiveHoldValue says %v", label, got[i], want)
		}
	}
}

// routedHeldRows returns n unassigned rows routed to the seat's alias, each
// carrying label.
func routedHeldRows(n int, label string) []fakeReaderRow {
	rows := make([]fakeReaderRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, fakeReaderRow{
			ID: fmt.Sprintf("held-%02d", i), Status: "open", Type: "task",
			Labels:   []string{label},
			Metadata: map[string]string{beadmeta.RoutedToMetadataKey: assignedHoldTestAlias},
		})
	}
	return rows
}

// TestRoutedTierServesPastAFullWindowOfHeldRows: a routed window filled with
// held rows must not hide the unheld routed row behind it, for every hold value
// and reader shape. The read is repeated unbounded only because the window was
// full and held a held row.
func TestRoutedTierServesPastAFullWindowOfHeldRows(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		for _, hold := range activeHoldFixtures() {
			t.Run(topoName+"/"+hold, func(t *testing.T) {
				rows := append(routedHeldRows(RoutedReadyClaimWindow, hold), routedOpenRow())
				got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
				assertServed(t, got, "routed")
				for _, row := range got {
					if strings.HasPrefix(row.ID, "held-") {
						t.Fatalf("routed tier served held row %s: %+v", row.ID, got)
					}
				}
				if !strings.Contains(log, "--limit=0") {
					t.Errorf("a full window of held rows was not read again unbounded; reads:\n%s", log)
				}
			})
		}
	}
}

// TestRoutedTierReadsOneWindowWhenNothingIsHeld: with no held row in the window
// the routed tier reads it once, as it always has.
func TestRoutedTierReadsOneWindowWhenNothingIsHeld(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append(routedHeldRows(RoutedReadyClaimWindow, "needs-review"), routedOpenRow())
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "held-00")
			if len(got) != RoutedReadyClaimWindow {
				t.Errorf("routed tier served %d rows, want its window of %d", len(got), RoutedReadyClaimWindow)
			}
			if reads := strings.Count(log, "--metadata-field "+beadmeta.RoutedToMetadataKey+"="); reads != 1 {
				t.Errorf("routed tier read %d times with nothing held, want once; reads:\n%s", reads, log)
			}
		})
	}
}

// TestRoutedTierServesInactiveHoldLabelsAsWork: hold:cleared, a hold: label
// with no value and labels that only contain the word are ordinary work.
func TestRoutedTierServesInactiveHoldLabelsAsWork(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		for _, label := range inactiveHoldFixtures() {
			t.Run(topoName+"/"+label, func(t *testing.T) {
				rows := routedHeldRows(1, label)
				got, _ := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
				assertServed(t, got, "held-00")
			})
		}
	}
}

// TestRoutedTierHoldsARowWhoseClearedLabelSitsBesideAnActiveHold: bd swaps one
// label per dimension, so hold:cleared can sit beside a hold set since; the
// active one still holds.
func TestRoutedTierHoldsARowWhoseClearedLabelSitsBesideAnActiveHold(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			held := routedHeldRows(1, "hold:cleared")
			held[0].Labels = append(held[0].Labels, "hold:po")
			got, _ := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), append(held, routedOpenRow()))
			assertServed(t, got, "routed")
			if len(got) != 1 {
				t.Fatalf("routed tier served %+v, want only the unheld row", got)
			}
		})
	}
}

// TestAssignedReadyServesPastAHeldHead: the assigned-ready tier serves one row,
// so a held assignment ahead of an unheld one must not take its place.
func TestAssignedReadyServesPastAHeldHead(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		for _, hold := range activeHoldFixtures() {
			t.Run(topoName+"/"+hold, func(t *testing.T) {
				got, _ := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), []fakeReaderRow{
					assignedOpenRow("held", assignedHoldTestSessionID, hold),
					assignedOpenRow("mine", assignedHoldTestSessionID),
					routedOpenRow(),
				})
				assertServed(t, got, "mine")
			})
		}
	}
}

// TestInProgressTierServesPastAHeldHead: the crash-recovery tier resumes one
// claim, so a held in_progress claim ahead of an unheld one must not take its
// place and send the session to fresh work while it owns a resumable claim.
func TestInProgressTierServesPastAHeldHead(t *testing.T) {
	for topoName, topo := range map[string]QueryTopology{"bd104": {}, "bd105": singleStoreTopology()} {
		for _, hold := range activeHoldFixtures() {
			t.Run(topoName+"/"+hold, func(t *testing.T) {
				held := assignedOpenRow("held", assignedHoldTestSessionID, hold)
				held.Status = "in_progress"
				mine := assignedOpenRow("mine", assignedHoldTestSessionID)
				mine.Status = "in_progress"
				got, _ := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), []fakeReaderRow{
					held, mine, assignedOpenRow("fresh", assignedHoldTestSessionID), routedOpenRow(),
				})
				assertServed(t, got, "mine")
			})
		}
	}
}

// TestPoolDemandCountExcludesActiveHolds: the reconciler's count form counts
// the rows a worker would be served, so held rows are not demand and inactive
// hold labels are.
func TestPoolDemandCountExcludesActiveHolds(t *testing.T) {
	requireJQ(t)
	var rows []fakeReaderRow
	for i, label := range append(activeHoldFixtures(), inactiveHoldFixtures()...) {
		rows = append(rows, fakeReaderRow{
			ID: fmt.Sprintf("row-%02d", i), Status: "open", Type: "task",
			Labels:   []string{label},
			Metadata: map[string]string{beadmeta.RoutedToMetadataKey: assignedHoldTestAlias},
		})
	}
	for topoName, topo := range map[string]QueryTopology{"bd104": {}, "bd105": singleStoreTopology()} {
		t.Run(topoName, func(t *testing.T) {
			command := poolDemandCountShell(assignedHoldTestAlias, topo)
			got := runFilteredCount(t, command, rows)
			if want := len(inactiveHoldFixtures()); got != want {
				t.Fatalf("pool demand count = %d, want %d (the rows carrying no active hold)", got, want)
			}
		})
	}
}

// runFilteredCount runs a count-form command over rows and returns the count it
// prints.
func runFilteredCount(t *testing.T, command string, rows []fakeReaderRow) int {
	t.Helper()
	tmp := t.TempDir()
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	rowsPath := tmp + "/rows.json"
	wispsPath := tmp + "/wisps.json"
	writeFakeBin(t, rowsPath, string(data))
	writeFakeBin(t, wispsPath, "[]")
	res := runGeneratedQueryWithBD(t, command, map[string]string{
		"FAKE_ROWS": rowsPath, "FAKE_WISPS": wispsPath, "FAKE_READ_LOG": tmp + "/reads.log",
	}, fakeFilteringReader, fakeFilteringReader)
	if res.exit != 0 {
		t.Fatalf("count form exited %d; stderr=%s", res.exit, res.stderr)
	}
	var n int
	if _, err := fmt.Sscan(strings.TrimSpace(res.stdout), &n); err != nil {
		t.Fatalf("count form printed %q: %v", res.stdout, err)
	}
	return n
}

// TestEphemeralProbesSkipActiveHolds: the bd-1.0.4 wisp probes drop held wisps
// before they cut to one row, for every hold value.
func TestEphemeralProbesSkipActiveHolds(t *testing.T) {
	for _, hold := range activeHoldFixtures() {
		t.Run(hold, func(t *testing.T) {
			got, _ := runFilteredQueryWithWisps(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(QueryTopology{}), assignedHoldSeatEnv(), nil,
				inCreationOrder(
					assignedOpenWisp("held-wisp", assignedHoldTestSessionID, 0, hold),
					assignedOpenWisp("mine-wisp", assignedHoldTestSessionID, 0),
				))
			assertServed(t, got, "mine-wisp")
		})
	}
}

// TestRouteScopedTiersCarryNoHoldLabelFlags: a hold is dropped by the jq
// predicate, never by an --exclude-label that names one value, so no generated
// read spells a hold label.
func TestRouteScopedTiersCarryNoHoldLabelFlags(t *testing.T) {
	agent := assignedHoldPoolAgent()
	for topoName, topo := range assignedHoldTopologies() {
		for name, query := range map[string]string{
			"work":        agent.EffectiveWorkQueryFor(topo),
			"pool demand": poolDemandCountShell(assignedHoldTestAlias, topo),
			"in progress": agent.EffectiveAssignedInProgressQueryFor(topo),
		} {
			if strings.Contains(query, `--exclude-label "hold:`) {
				t.Errorf("%s/%s query names a hold label in an --exclude-label flag: %s", topoName, name, query)
			}
		}
	}
}
