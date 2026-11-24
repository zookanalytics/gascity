package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// Coverage for the claim tiers' one pass over their rows: each drops held rows
// and rows whose gc.exclude_claimants names this claimant before it cuts its
// answer, and a windowed read (claimWindowScript) reads again unbounded when a
// full window lost a row to either filter. Each test puts held rows and
// excluded rows, interleaved, ahead of the one row the tier may serve, runs
// the generated query against fakeFilteringReader as the seat
// (assignedHoldSeatEnv), and expects that row served.

// claimWindowExcludedValue is a gc.exclude_claimants value that names the
// seat's alias beside another identity.
const claimWindowExcludedValue = "example/pool.other, " + assignedHoldTestAlias

// heldAndExcludedRows returns n rows built by row from their index, the even
// ones on an active hold and the odd ones excluding the seat.
func heldAndExcludedRows(n int, row func(i int) fakeReaderRow) []fakeReaderRow {
	rows := make([]fakeReaderRow, 0, n)
	for i := 0; i < n; i++ {
		r := row(i)
		if i%2 == 0 {
			r.Labels = append(r.Labels, "hold:po")
		} else {
			md := map[string]string{beadmeta.ExcludeClaimantsMetadataKey: claimWindowExcludedValue}
			for k, v := range r.Metadata {
				md[k] = v
			}
			r.Metadata = md
		}
		rows = append(rows, r)
	}
	return rows
}

// assertReadUnbounded fails unless the reader log holds an unbounded read
// (--limit=0) carrying every marker: the tier's window came back full with a
// dropped row in it and was read again.
func assertReadUnbounded(t *testing.T, log string, markers ...string) {
	t.Helper()
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "--limit=0") {
			continue
		}
		all := true
		for _, m := range markers {
			all = all && strings.Contains(line, m)
		}
		if all {
			return
		}
	}
	t.Errorf("no read carrying %q was repeated unbounded; reads:\n%s", markers, log)
}

// assertNothingDroppedServed fails if the query served a row the test built to
// be dropped.
func assertNothingDroppedServed(t *testing.T, got []fakeReaderRow) {
	t.Helper()
	for _, row := range got {
		if strings.HasPrefix(row.ID, "dropped-") {
			t.Fatalf("query served %s, a held or excluded row: %+v", row.ID, got)
		}
	}
}

func TestRoutedTierServesPastAFullWindowOfHeldAndExcludedRows(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append(heldAndExcludedRows(2*RoutedReadyClaimWindow, func(i int) fakeReaderRow {
				return fakeReaderRow{
					ID: fmt.Sprintf("dropped-%02d", i), Status: "open", Type: "task",
					Metadata: map[string]string{beadmeta.RoutedToMetadataKey: assignedHoldTestAlias},
				}
			}), routedOpenRow())
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "routed")
			assertNothingDroppedServed(t, got)
			assertReadUnbounded(t, log, beadmeta.RoutedToMetadataKey+"="+assignedHoldTestAlias+" --unassigned")
		})
	}
}

func TestMigrationTierServesPastAFullWindowOfHeldAndExcludedRows(t *testing.T) {
	legacyRoot := func(id string) fakeReaderRow {
		return fakeReaderRow{
			ID: id, Status: "open", Type: "task",
			Metadata: map[string]string{
				beadmeta.RunTargetMetadataKey: assignedHoldTestAlias,
				beadmeta.KindMetadataKey:      beadmeta.KindWorkflow,
			},
		}
	}
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append(heldAndExcludedRows(2*poolDemandMigrationWindow, func(i int) fakeReaderRow {
				return legacyRoot(fmt.Sprintf("dropped-%02d", i))
			}), legacyRoot("legacy-root"))
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "legacy-root")
			assertNothingDroppedServed(t, got)
			assertReadUnbounded(t, log, beadmeta.RunTargetMetadataKey+"="+assignedHoldTestAlias)
		})
	}
}

// graphAnchorClaim is the seat's in_progress graph.v2 workflow root, the
// anchor whose routed steps the anchor probe reads.
func graphAnchorClaim() fakeReaderRow {
	return fakeReaderRow{
		ID: "anchor", Status: "in_progress", Type: "task", Assignee: assignedHoldTestSessionID,
		Metadata: map[string]string{
			beadmeta.KindMetadataKey:            beadmeta.KindWorkflow,
			beadmeta.FormulaContractMetadataKey: "graph.v2",
		},
	}
}

func TestGraphAnchorProbeServesPastAFullWindowOfHeldAndExcludedSteps(t *testing.T) {
	anchorStep := func(id string) fakeReaderRow {
		return fakeReaderRow{
			ID: id, Status: "open", Type: "task",
			Metadata: map[string]string{
				beadmeta.RootBeadIDMetadataKey: "anchor",
				beadmeta.RoutedToMetadataKey:   assignedHoldTestAlias,
			},
		}
	}
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append([]fakeReaderRow{graphAnchorClaim()}, heldAndExcludedRows(2*graphAnchorReadyWindow, func(i int) fakeReaderRow {
				return anchorStep(fmt.Sprintf("dropped-%02d", i))
			})...)
			rows = append(rows, anchorStep("step"))
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "step")
			assertNothingDroppedServed(t, got)
			assertReadUnbounded(t, log, beadmeta.RootBeadIDMetadataKey+"=anchor")
		})
	}
}

func TestAssignedReadyServesPastAWindowOfHeldAndExcludedAssignments(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append(heldAndExcludedRows(2, func(i int) fakeReaderRow {
				return assignedOpenRow(fmt.Sprintf("dropped-%02d", i), assignedHoldTestSessionID)
			}), assignedOpenRow("mine", assignedHoldTestSessionID), routedOpenRow())
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "mine")
			assertReadUnbounded(t, log, "ready", "--assignee="+assignedHoldTestSessionID+" --json")
		})
	}
}

// inProgressClaim is an in_progress claim of the seat's session.
func inProgressClaim(id string) fakeReaderRow {
	row := assignedOpenRow(id, assignedHoldTestSessionID)
	row.Status = "in_progress"
	return row
}

// TestInProgressTierServesPastAWindowOfHeldAndExcludedClaims covers the
// single-store crash-recovery read, whose window is one row. The federated
// read is cut by `gc ready --until-unblocked`, which leaves held rows out
// itself (internal/readywork).
func TestInProgressTierServesPastAWindowOfHeldAndExcludedClaims(t *testing.T) {
	for topoName, topo := range map[string]QueryTopology{"bd104": {}, "bd105": singleStoreTopology()} {
		t.Run(topoName, func(t *testing.T) {
			rows := append(heldAndExcludedRows(2, func(i int) fakeReaderRow {
				return inProgressClaim(fmt.Sprintf("dropped-%02d", i))
			}), inProgressClaim("mine"), assignedOpenRow("fresh", assignedHoldTestSessionID), routedOpenRow())
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "mine")
			assertReadUnbounded(t, log, "list --status in_progress --assignee="+assignedHoldTestSessionID)
		})
	}
}

func TestInProgressCandidatesServePastAFullWindowOfHeldAndExcludedClaims(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		t.Run(topoName, func(t *testing.T) {
			rows := append([]fakeReaderRow{graphAnchorClaim()}, heldAndExcludedRows(2*inProgressCandidatesLimit, func(i int) fakeReaderRow {
				return inProgressClaim(fmt.Sprintf("dropped-%02d", i))
			})...)
			rows = append(rows, inProgressClaim("mine"), routedOpenRow())
			got, log := runFilteredQuery(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), rows)
			assertServed(t, got, "mine")
			assertReadUnbounded(t, log, "--status in_progress --assignee="+assignedHoldTestSessionID)
		})
	}
}

// TestLegacyEphemeralPoolDemandServesPastHeldAndExcludedWisps covers the
// bd-1.0.4 routed wisp candidates, which the worker cuts to 20 from the whole
// open-ephemeral snapshot: both filters run before that cut, so a cut full of
// excluded wisps cannot hide the routed wisp behind them.
func TestLegacyEphemeralPoolDemandServesPastHeldAndExcludedWisps(t *testing.T) {
	for topoName, topo := range bd104WispTopologies() {
		t.Run(topoName, func(t *testing.T) {
			wisps := heldAndExcludedRows(40, func(i int) fakeReaderRow {
				return fakeReaderRow{
					ID: fmt.Sprintf("dropped-%02d", i), Status: "open", Type: "task",
					Metadata: map[string]string{beadmeta.RoutedToMetadataKey: assignedHoldTestAlias},
				}
			})
			wisps = inCreationOrder(append(wisps, fakeReaderRow{
				ID: "wisp-step", Status: "open", Type: "task",
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: assignedHoldTestAlias},
			})...)
			got, _ := runFilteredQueryWithWisps(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(topo), assignedHoldSeatEnv(), nil, wisps)
			assertServed(t, got, "wisp-step")
		})
	}
}

// TestWispProbesSkipHeldAndExcludedWisps covers the bd-1.0.4 assigned wisp
// probes, which cut the snapshot to one row after their selections.
func TestWispProbesSkipHeldAndExcludedWisps(t *testing.T) {
	for _, status := range []string{"open", "in_progress"} {
		t.Run(status, func(t *testing.T) {
			wisps := heldAndExcludedRows(2, func(i int) fakeReaderRow {
				row := assignedOpenWisp(fmt.Sprintf("dropped-%02d", i), assignedHoldTestSessionID, 0)
				row.Status = status
				return row
			})
			mine := assignedOpenWisp("mine-wisp", assignedHoldTestSessionID, 0)
			mine.Status = status
			got, _ := runFilteredQueryWithWisps(t, assignedHoldPoolAgent().EffectiveWorkQueryFor(QueryTopology{}), assignedHoldSeatEnv(), nil,
				inCreationOrder(append(wisps, mine)...))
			assertServed(t, got, "mine-wisp")
		})
	}
}
