---
name: bd 1.2.1's legacy-workspace gate vs. a fresh gc-managed city (gc-sc8a8 / gc-1kqz0)
description: bd 1.2.1 classifies a workspace's era from its on-disk shape before init runs, and a fresh gc-managed city presents exactly the shape it refuses — the managed Dolt server's data root IS .beads/dolt and it exists before the first bd init. Records the guard's full decision table, the two distinct shapes gc produced, why --force/--reinit-local cannot bypass it, and what to re-check on the next bd bump.
---

# gc-sc8a8: a scratch city is not a legacy workspace

## Verdict

**Not a migration. Two shared-setup defects in gc's init path**, both caused by
the same bd 1.2.1 gate, both fixed by making gc present bd the workspace shape
that is actually true.

gc-1kqz0's fix-direction note offered (a) MIGRATION or (b) FIXTURE and called
(b) "the more likely culprit and the cheaper check". (b) is correct — with the
refinement that it is not the *fixtures* that were wrong. Production `gc init`
built the same shape; the fixtures were the first callers to meet the new gate.

## The gate

`bd/cmd/bd/legacy_upgrade_guard.go:guardLegacyUpgradeWorkspace(beadsDir)`,
new in the 1.1.0 → 1.2.1 bump (`4ad99760b`). It runs from `bd init` *before*
the existing-workspace checks, so **neither `--force` nor `--reinit-local`
bypasses it** — those flags only lift the local data-safety guard downstream.
It also never consults the `--server` flag: classification reads
`.beads/metadata.json` (`configfile.ConfigFileName`) off disk.

Its decision, in order, for `serverMode = (metadata.dolt_mode == "server")`
and `localDoltRoot = .beads/dolt is a real, non-symlink directory`:

| On-disk shape | Verdict |
|---|---|
| historical SQLite (`.db` file, no embeddeddolt) | refuse `historical SQLite workspace` |
| embeddeddolt repo present, not server mode | **pass** |
| server mode + witness in 0.55–0.62 | refuse `legacy Dolt server workspace from bd <v>` |
| no `localDoltRoot` | **pass** |
| server mode + witness major ≥ 1 | **pass** |
| server mode + no/invalid witness | refuse `legacy Dolt server workspace` |
| embedded/unset mode + `localDoltRoot` + not shared-server | refuse `legacy Dolt workspace` |

"Witness" is `.beads/.local_version`, the file bd writes itself on every
version change (`cmd/bd/version_tracking.go`). On a workspace bd has never run
in, it does not exist — so gc is the only party that can answer.

## The two shapes gc produced

Both start from the same fact: **the managed Dolt server's data root is
`<scope>/.beads/dolt`, and the server is started before the first `bd init`.**
So `localDoltRoot` is always true for a gc-managed scope, and gc lands in the
bottom two rows of the table above.

1. **gc-driven init** (`initAndHookDir` → `normalizeCanonicalBdScopeFilesForInit`
   → `initBeadsForDir`). Metadata already says `dolt_mode: server`, but no
   witness exists → row 6 → `legacy Dolt server workspace detected`.
   Fixed by seeding the witness: `contract.EnsureLocalVersionWitness`, wired in
   at the end of `normalizeCanonicalBdScopeFilesForInit`.

2. **direct `gc-beads-bd init <dir> <prefix> [db]`** (a documented entry point;
   `managedBdWaitTestTemplate` uses it). No metadata at all → `cfg == nil` →
   row 7 → `legacy Dolt workspace detected`. The script's own comment already
   asserted "gc's normalizeCanonicalBdScopeFilesForInit writes metadata.json
   BEFORE invoking us" — an invariant it depended on but never established.
   Fixed by having `op_init` call `normalize_scope_canonical_files` before
   `bd init` when `dolt_mode` is not already `server`.

Shape 2 is why the witness alone was not enough: row 7 has no witness escape.
Its only escape is shared-server mode, which a managed city is not.

## Why gc may write a file bd owns

The witness records which era created the workspace. For a scope gc is creating
right now against a bd it just resolved, gc knows that answer and bd cannot.
The write is deliberately narrow:

- only for the exact shape bd refuses (server mode + real `.beads/dolt`);
- only when **no** witness exists — a pre-1.0 value there is a real
  migration signal and must keep reaching the operator;
- only the version of the bd binary about to run (`beads.ProbeBDVersion`,
  the same PATH lookup `run_bd_pinned` makes), so bd's own upgrade-detection
  stays correct: recorded == binary means nothing to reconcile, which is the
  truth for a workspace being created.

Writing a floor value like `1.0.0` instead would clear the gate but fake an
upgrade on every fresh city. Writing a pre-1.0 or malformed value is rejected
outright rather than written.

## On the next bd bump

The gate is version-shaped, so re-check it whenever the bd pin moves:

- `grep -rn "legacy_upgrade_guard" $(go list -m -f '{{.Dir}}' github.com/steveyegge/beads)`
  and re-derive the table above; the branch gc relies on is the
  `currentVersionWitness(version)` escape.
- Confirm `--reinit-local` is still the non-deprecated spelling
  (`bd help init-safety`).
- The cheapest end-to-end check is
  `GC_FAST_UNIT=0 go test ./cmd/gc/ -run TestFreshManagedBdCityInitSeedsPinnedHQDatabaseAndKeepsGCPrefix`
  (~9s), which exercises shape 1. Shape 2 needs
  `-tags integration ... -run TestManagedBdRigProviderStoreRecoversAfterHardKillPortRebind` (~4min).

Sibling from the same bump, different mechanism: **gc-zfh0w**
(`internal/bdflags` manifest stale against 1.2.1). Not fixed here.

## Reproducing the classification without a server

The guard is pure filesystem inspection, so it can be A/B'd in seconds. Run bd
with a **sanitized environment** — an inherited `BEADS_DIR` sends it at the real
rig store:

```bash
mk() { mkdir -p "$1/.beads/dolt/hq"
  printf 'issue_prefix: gc\ndolt:\n  mode: server\n' > "$1/.beads/config.yaml"
  printf '{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"hq"}\n' \
    > "$1/.beads/metadata.json"; }
mk /var/tmp/no-witness
mk /var/tmp/with-witness && printf '1.2.1\n' > /var/tmp/with-witness/.beads/.local_version
for c in no-witness with-witness; do
  ( cd /var/tmp/$c && env -i PATH="$PATH" HOME="$HOME" BD_NON_INTERACTIVE=1 \
      bd init --quiet --server -p gc --database hq --skip-hooks --skip-agents \
        --server-host 127.0.0.1 --server-port 39999 /var/tmp/$c 2>&1 | head -3 )
done
```

`no-witness` refuses; `with-witness` gets past the gate and fails later on the
(deliberately absent) server. That difference is the whole defect.
