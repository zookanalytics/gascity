---
name: Selective bead reads for the controller's hot paths, measured (gc-7eevlv)
description: Records the per-read field projection (beads.FieldSet) and the measurements behind it. On a store with a 10 MB notes row, a control-dispatcher drain on bd 1.3.1 moves 1,114 bytes instead of 11,535,629, the native store allocates about 0.1 MB per ready read instead of 23 MB, and a scope driven by bd 1.0.4 pays one refused --brief per process. Lists the reads converted, the reads left whole on purpose, and the beads filed for them.
---

# gc-7eevlv: selective reads for the controller's hot paths

## Verdict

A control-dispatcher drain now asks every backing for core rows: every field
except the free-form text. On a store holding a 10 MB notes row, one drain's
`bd ready` moves 1,114 bytes where it moved 11,535,629, the dolt server spends
about half the CPU on it, and the native store allocates about 0.5% of what a
whole-row read allocates. The payload is the `--brief` size, inside the 2x the
bead asked for.

## What changed

- `beads.ListQuery.Fields` and `beads.ReadyQuery.Fields` take a
  `beads.FieldSet`. The zero value, `FieldsFull`, reads whole rows, so no read
  changed until its caller opted in. `FieldsCore` returns every row with an
  empty `Description` and every other field intact, on every backing. The
  `internal/beads` package doc states how each backing honors it and why the
  projection is a per-read declaration rather than a core-row view, an
  event-fed ready set, or a longer idle backoff.
- The control-dispatcher readiness scan (`controlReadyLegReady` and the shell
  fallback `controlReadyScopeShellReady` in `cmd/gc/dispatch_control_ready.go`)
  reads core rows. So do the blocker lookups BdStore and NativeDoltStore make
  inside every `Ready` to apply the work-outcome veto, which read only a
  blocker's status and metadata.
- The closed named-session index (`BuildClosedNamedSessionBeadIndex`) reads
  core rows and keeps only the winning bead's id, so no row leaves it.
- `TestProjectedReadFlowsReadNoOmittedField` in cmd/gc registers every
  projected read outside `internal/beads` with the boundary its rows stay
  inside, and fails when a function those rows reach selects `Description`.

## Measurements

The fixture is a Homebrew dolt 2.4.2 `sql-server` on 127.0.0.1 with a bd
workspace in server mode. Of its four beads, three are ready and one of those
carries 10,485,760 bytes of notes and 1,048,576 bytes of design, written by
SQL. Every bd run went through `env -i`, so no live store was touched. The host
is the 18-core macOS fleet host at load 45 to 75, so wall times are noisy; the
CPU figure is the dolt server's CPU time across ten drains, divided by ten.

| Read (bd 1.3.1) | Bytes per drain | Dolt CPU per drain | Wall p50 |
|---|---|---|---|
| `bd ready --json --include-ephemeral --limit 0` | 11,535,629 | 37 to 44 ms | 1.37 to 1.49 s |
| the same with `--brief` | 1,114 | 18 to 21 ms | 0.85 to 0.91 s |

Through gascity's own code, with a BdStore over the exec runner pointed at the
same workspace:

- A `FieldsFull` `Ready(TierBoth)` ran one `bd ready` that returned 11,535,629
  bytes. A `FieldsCore` one ran `bd ready ... --brief` and returned 1,114
  bytes. The three rows matched field for field once the full rows'
  descriptions were cleared.
- A closed-inclusive `List` moved 11,536,217 bytes whole and 1,669 bytes core,
  again with matching rows.
- Against bd 1.0.4 (the supported floor, which has no `--brief`), the first
  core read ran `bd ready ... --brief`, which exited 1 with
  `Error: unknown flag: --brief`, then re-read whole rows and answered
  correctly. A second store built over the same scope ran one `bd ready`
  without the flag: the refusal is spent once per scope per process.

Through NativeDoltStore (`OpenNativeDoltStoreAt` over the same workspace and
server), measured as the Go allocation of one read:

| Read | Whole rows | Core rows |
|---|---|---|
| `Ready(TierBoth)` | 23,195,072 bytes | 109,240 to 109,808 bytes |
| `List(AllowScan, IncludeClosed, TierBoth)` | 23,152,952 to 23,176,768 bytes | 70,520 to 78,728 bytes |

The rows matched field for field once the full rows' descriptions were
cleared, so the library's lite select drops only the text.

## Left whole on purpose

- The controller-demand ready and open snapshots, including the v2
  external-reads lane's recordings, keep their rows across a pass and hand
  them to several collectors, so no single function bounds them and the flow
  check cannot register them. Filed as gc-ve3wtf.
- The seat-work live lists keep their rows in the seat-work index and hand
  them out as hits, for the same reason. Filed as gc-8egjlt.
- The cache's prime and reconcile scans fill the cache, which serves full
  reads, so they read whole rows. Filling it from core rows needs a per-row
  text-known mark. Filed as gc-zjd8ye.
- `bd query`, which BdStore runs for the wisp leg of a both-tier list, has no
  `--brief`. Those rows arrive whole and the store clears their text.
- A projected Live or ParentID list through a CachingStore reads the backing
  and installs nothing, so it no longer refreshes the cache as a whole-row one
  does.
