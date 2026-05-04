---
title: gc doctor --json schema
description: Wire contract for the machine-readable output of `gc doctor --json`.
---

`gc doctor --json` emits a single JSON document on stdout for automated
consumers (deacon-patrol, watchdogs, monitoring scrapers). The default
human-readable output is a separate, also-stable contract documented in
[engdocs/design/beads-dolt-contract-redesign.md](../design/beads-dolt-contract-redesign.md);
this page covers the JSON envelope only.

## When to use it

Use `--json` whenever the consumer is code, not a human:

- **Agents** parsing findings to act or escalate (`mol-deacon-patrol`'s
  `system-health` step).
- **Scripts** filtering findings by status (`gc doctor --json | jq
  '.checks[] | select(.status != "ok")'`).
- **Monitoring** that tracks the count of warning/error checks over time.

Do not parse the human output — it is the human contract, optimized for
operator triage, and may add prefixes, change ordering of detail lines,
or use Unicode glyphs without notice. Anything an agent needs to read
must come through `--json`.

## Top-level shape

```json
{
  "checks": [ ... ],
  "summary": { "passed": 0, "warned": 0, "failed": 0, "fixed": 0 }
}
```

`checks` is always an array (possibly empty). `summary` is always
present. Both keys are required — consumers may rely on their presence
without defensive `if` checks.

## Per-check entry

```json
{
  "name": "session-model",
  "status": "warning",
  "message": "1 session model finding(s)",
  "details": [
    "lx-wisp-pxdv assigned to missing session bead gc-toolkit/gastown.witness"
  ],
  "fix_hint": "claim or close the wisp",
  "fix_error": "",
  "fix_attempted": false,
  "fixed": false
}
```

| Field | Type | Always present | Notes |
|-------|------|----------------|-------|
| `name` | string | yes | Stable check identifier (e.g. `city-config`, `session-model`). Never empty. |
| `status` | string | yes | One of `"ok"`, `"warning"`, `"error"`. (Future statuses, if introduced, will be additive — consumers should treat unknown values as non-OK.) |
| `message` | string | yes | One-line human-readable summary. Counts and headlines live here; per-finding text lives in `details`. |
| `details` | string array | omitempty | Per-finding lines (the actionable subset). Always emitted in JSON regardless of `--verbose` — the `--verbose` flag only affects human output. |
| `fix_hint` | string | omitempty | A suggested command or action when the check failed and cannot auto-fix. |
| `fix_error` | string | omitempty | Populated when `--fix` was requested and the auto-fix function returned an error. |
| `fix_attempted` | bool | yes | True if `--fix` ran the fixer but the check is still non-OK afterwards. |
| `fixed` | bool | yes | True if `--fix` ran and the re-check passed. A `fixed` entry will have `status: "ok"`. |

`omitempty` fields are omitted from the JSON when their value is the
zero value (empty string, empty slice). Consumers should treat absence
identically to the zero value.

## Summary

```json
{
  "passed": 12,
  "warned": 1,
  "failed": 0,
  "fixed": 1
}
```

| Field | Notes |
|-------|-------|
| `passed` | Count of checks with `status: "ok"`. Includes checks that were `fixed` (they are counted both as `passed` and `fixed`, matching the human summary contract). |
| `warned` | Count of `status: "warning"`. |
| `failed` | Count of `status: "error"`. |
| `fixed` | Count of checks remediated by `--fix`. |

## Exit codes

The exit code is identical to the human-output mode:

- `0` — no failures (warnings allowed).
- `1` — at least one check returned `status: "error"`.

Exit codes are not emitted in the JSON document; consumers that care
about pass/fail can inspect `summary.failed > 0` instead of relying on
the process exit status.

## Example consumer (jq)

```bash
# Emit only checks that need attention, with their actionable details.
gc doctor --json | jq '.checks[] | select(.status != "ok")'

# Count failures for monitoring.
gc doctor --json | jq '.summary.failed'

# Get the fix hint for a specific check.
gc doctor --json | jq -r '.checks[] | select(.name == "session-model") | .fix_hint // "no hint"'
```

## Stability promise

This schema is the wire contract. Compatible changes:

- Adding new checks (more entries in `checks[]`).
- Adding new `omitempty` fields on entries (existing consumers ignore them).
- Adding new fields on `summary` (consumers should ignore unknown keys).

Breaking changes (require a versioned envelope or new flag):

- Renaming or removing fields documented above.
- Changing field types.
- Changing the meaning of `status` tokens.

When in doubt, add a new field rather than overloading an existing one.
