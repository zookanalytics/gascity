# Audit: bare-form `<rig>/<role>` identifiers in gastown pack source

**Bead:** `gc-iypi`
**Date:** 2026-04-30
**Auditor:** `gascity/gastown.polecat` (session `lx-uoxn`, alias
`gascity/gastown.nux`)
**Scope:** `examples/gastown/packs/gastown/` in this worktree
**Out of scope:** Applying fixes; opening upstream PRs; touching
local lx-m9ckw patches.

## TL;DR

Pack-binding-imported gastown rigs (`city.toml [rigs.imports.gastown]`)
expose pack agents under their qualified-form identity
`<rig>/gastown.<role>`. Any `assignee=`, `gc.routed_to=`,
`pool:`-label, mail address, or nudge target in the pack source that
emits the bare form `<rig>/<role>` (or, for city-scoped agents, bare
`<role>`) does not match the qualified identity. Routed beads sit
unclaimed; nudges fail to wake the addressed agent; spawn-detection
work-queries miss the bead.

This audit catalogs every such reference in the pack source and
classifies each one. It does not apply fixes (per `gc-iypi`).

## Inventory

### A. Already-known references (covered by lx-m9ckw / bead caveats)

| File | Line | Snippet | Classification | Coverage |
|---|---|---|---|---|
| `formulas/mol-polecat-work.toml` | 207 | `gc bd update {{issue}} --status=open --assignee=<rig>/refinery --set-metadata gc.routed_to=<rig>/refinery` | Pack-binding bug | lx-m9ckw (local patch) |
| `agents/polecat/prompt.template.md` | 202 | `gc bd update <work-bead> --status=open --assignee={{ .RigName }}/refinery --set-metadata gc.routed_to={{ .RigName }}/refinery` | Pack-binding bug | lx-m9ckw (local patch) |
| `formulas/mol-refinery-patrol.toml` | 170 | `--set-metadata gc.routed_to=<rig>/polecat` (rejection re-route) | Pack-binding bug | bead caveats |
| `template-fragments/approval-fallacy.template.md` | 44 | `gc bd update <work-bead> --status=open --assignee={{ .RigName }}/refinery --set-metadata gc.routed_to={{ .RigName }}/refinery` | Pack-binding bug | source of #2 (polecat done sequence is rendered from this fragment); lx-m9ckw must patch this too |

### B. Mayor / dispatch playbook (the symptom that triggered this audit)

| File | Line | Snippet | Classification | Suggested fix |
|---|---|---|---|---|
| `agents/mayor/prompt.template.md` | 24 | `gc bd update <bead-id> --set-metadata gc.routed_to=<rig>/polecat  # dispatch to polecat pool` | Pack-binding bug | Replace with qualified form, or document `gc sling` as the canonical filing path that stamps `<rig>/gastown.polecat` automatically |
| `agents/mayor/prompt.template.md` | 217 | `\| Dispatch work to polecat \| \`gc bd update <bead> --label=pool:<rig>/polecat\` \| ~~gc polecat spawn~~ / ~~--assignee=<rig>/polecat~~ \|` | Pack-binding bug | Same as above. The strikethrough only marks the `--assignee` form as wrong, not the bare `<rig>/polecat` portion — even the anti-pattern example perpetuates the bare form |

This is the mayor's filing-path table. It is the most load-bearing
operator-facing instruction in the pack: the mayor uses this exact
snippet when manually dispatching beads. `gc-1g53` (the Dolt MPTCP
fix that stayed in pool for ~11 hours) was filed against this
playbook entry. **#1486 P1 covers this family upstream;** locally
this is the highest-priority fix.

### C. Crew prompt (templated dispatch table & nudge examples)

| File | Line | Snippet | Classification | Notes |
|---|---|---|---|---|
| `assets/prompts/crew.template.md` | 103 | `\| Work should be done by target rig's workers \| \`{{ cmd }} convoy create\` + \`gc bd update --label=pool:<rig>/polecat\` \|` | Pack-binding bug | Same as Mayor #B |
| `assets/prompts/crew.template.md` | 402 | `\| Dispatch work to polecat \| \`gc bd update <bead> --label=pool:<rig>/polecat\` \| ~~gc polecat spawn~~ / ~~--assignee=<rig>/polecat~~ \|` | Pack-binding bug | Same as Mayor #B |
| `assets/prompts/crew.template.md` | 210 | `gc nudge {{ .RigName }}/crew/alice "..."` | Intentional bare | `crew/<name>` is the crew-member address pattern, separate from pack roles. Crew members aren't pack-bound. |
| `assets/prompts/crew.template.md` | 211 | `gc nudge {{ .RigName }}/<polecat-name> "..."` | Ambiguous | `<polecat-name>` is a session-name placeholder (e.g., `furiosa`, `nux`). Polecat sessions are addressable via the runtime's session-name registry, which may or may not honor pack binding. **Design check needed.** Same family as #1397. |
| `assets/prompts/crew.template.md` | 212 | `gc mail send {{ .RigName }}/alice -s "Urgent" -m "..." --notify` | Intentional bare | `<name>` is a crew alias, not a pack role |
| `assets/prompts/crew.template.md` | 217 | text: ``{{ .RigName }}/polecats/<name>`` address form (described as the form being used) | Stale doc | Refinery prompt explicitly says "There is no `{{ .RigName }}/polecats/<name>` address form" (line 154). This crew docs reference is stale. |
| `assets/prompts/crew.template.md` | 93 | `BD_ACTOR` is `{{ .RigName }}/crew/{{ basename .AgentName }}` | Intentional bare | crew-member path is its own namespace |

### D. Refinery prompt (mail/nudge/identity)

| File | Line | Snippet | Classification | Suggested fix |
|---|---|---|---|---|
| `agents/refinery/prompt.template.md` | 148 | `gc nudge {{ .RigName }}/<polecat-name> "Run gc hook; ..."` | Ambiguous | Same as Crew #211. Session-name resolution behavior under pack binding needs design clarification. |
| `agents/refinery/prompt.template.md` | 154 | text: "There is no `{{ .RigName }}/polecats/<name>` address form." | Documentation (correct) | This line is the canonical statement that the polecats-plural form was removed. Other references to `polecats/<name>` (Crew #217, Witness #190, Witness mol #145) should align with this. |
| `agents/refinery/prompt.template.md` | 190 | `Mail identity: {{ .RigName }}/refinery` | Pack-binding bug (informational) | The refinery's own identity label. Under `[rigs.imports.gastown]` the actual identity is `{{ .RigName }}/gastown.refinery`. |

### E. Witness prompt (mail/nudge/identity)

| File | Line | Snippet | Classification | Suggested fix |
|---|---|---|---|---|
| `agents/witness/prompt.template.md` | 183 | `gc mail send {{ .RigName }}/refinery -s "Subject" -m "..."` | Pack-binding bug | qualified form |
| `agents/witness/prompt.template.md` | 184 | `gc nudge {{ .RigName }}/<polecat-name> "..."` | Ambiguous | session-name; same as Crew #211 |
| `agents/witness/prompt.template.md` | 185 | `gc session peek {{ .RigName }}/<polecat-name> 50` | Ambiguous | `gc session peek` resolves runtime session names; binding behavior may differ from `nudge`/`mail` |
| `agents/witness/prompt.template.md` | 190 | text: `{{ .RigName }}/polecats/<name>` address form | Stale doc | Same as Crew #217 |
| `agents/witness/prompt.template.md` | 257 | `Your mail address: {{ .RigName }}/witness` | Pack-binding bug (informational) | Same as Refinery #190 |

### F. Polecat prompt (escalation, identity, command quick-ref)

| File | Line | Snippet | Classification | Suggested fix |
|---|---|---|---|---|
| `agents/polecat/prompt.template.md` | 103 | descriptive: "`{{ .RigName }}/polecat`" (the polecat's pool routing target) | Pack-binding bug (informational) | Should describe `<rig>/gastown.polecat` |
| `agents/polecat/prompt.template.md` | 156 | `gc mail send {{ .RigName }}/witness -s "ESCALATION: ..." -m "..."` | Pack-binding bug | qualified form |
| `agents/polecat/prompt.template.md` | 169 | `gc nudge {{ .RigName }}/witness "..."` | Pack-binding bug | qualified form |
| `agents/polecat/prompt.template.md` | 170 | `gc mail send {{ .RigName }}/witness -s "HELP: ..." -m "..."` | Pack-binding bug | qualified form |
| `agents/polecat/prompt.template.md` | 222 | `\| Escalate blocker \| \`gc mail send {{ .RigName }}/witness ...\` \|` | Pack-binding bug | qualified form |
| `agents/polecat/prompt.template.md` | 229 | `Mail identity: {{ .RigName }}/{{ basename .AgentName }}` | Ambiguous | The polecat's mail identity uses the basename (e.g., `furiosa`). Whether mail addresses are bound under pack namespace at the session-name layer is the same design question as #1397. **Already noted in lx-m9ckw caveats.** |

### G. Witness patrol formula (queue health & orphan detection)

| File | Line | Snippet | Classification | Suggested fix |
|---|---|---|---|---|
| `formulas/mol-witness-patrol.toml` | 145 | text: "Filter for beads assigned to polecat-pattern agents (e.g., `<rig>/polecats/<name>`)" | Stale doc | The plural-`polecats/` form was removed (cf. Refinery #154). The actual orphan-detection logic in code uses `AgentMatchesIdentity`, which the upstream `gastownhall/gascity#1486` PR is meant to fix. Description should match current SDK behavior. |
| `formulas/mol-witness-patrol.toml` | 309 | `gc bd list --assignee=<rig>/refinery --status=open --json` | Pack-binding bug | `--assignee=<rig>/gastown.refinery` |
| `formulas/mol-witness-patrol.toml` | 329 | `gc nudge <rig>/refinery "Work beads waiting for merge. Please check queue."` | Pack-binding bug | qualified form |

### H. Dog pool references (`pool="dog"`, `gc.routed_to=dog`, `--label=pool:dog`)

`dog` is defined upstream in the `maintenance` pack and imported into
gastown via `pack.toml [imports.maintenance]`. In a pack-binding rig
the dog template's qualified identity becomes
`<rig>/gastown.maintenance.dog` (transitive bindings apply). Bare
`dog` won't match.

The bead caveats explicitly flag this family ("dolt `mol-dog-*`
orders' `pool=\"dog\"`").

| File | Line | Snippet | Classification | Notes |
|---|---|---|---|---|
| `orders/digest-generate.toml` | 6 | `pool = "dog"` | Pack-binding bug | Order's pool reference |
| `formulas/mol-deacon-patrol.toml` | 165 | `--set-metadata gc.routed_to=dog` (warrant for stuck coordination agents) | Pack-binding bug | Stuck-agent warrant routing |
| `formulas/mol-deacon-patrol.toml` | 189 | `gc bd list --status=in_progress --metadata-field gc.routed_to=dog --json --limit=0` | Pack-binding bug | Find active dog work (read-side) |
| `formulas/mol-deacon-patrol.toml` | 205 | `--set-metadata gc.routed_to=dog` (warrant for stuck dogs) | Pack-binding bug | |
| `formulas/mol-witness-patrol.toml` | 381 | `--label=pool:dog` (stuck-polecat warrant) | Pack-binding bug | |
| `agents/witness/prompt.template.md` | 133 | `--label=pool:dog` | Pack-binding bug | Witness playbook |
| `agents/witness/prompt.template.md` | 253 | `\| File stuck-agent warrant \| \`gc bd create --type=warrant --label=pool:dog ...\` \|` | Pack-binding bug | Cmd quick-ref |
| `agents/boot/prompt.template.md` | 95 | `--label=pool:dog` | Pack-binding bug | Boot playbook |
| `agents/boot/prompt.template.md` | 127 | `\| File stuck warrant \| \`gc bd create --type=warrant --label=pool:dog ...\` \|` | Pack-binding bug | Cmd quick-ref |
| `agents/deacon/prompt.template.md` | 109 | `--label=pool:dog` | Pack-binding bug | Deacon playbook |
| `agents/deacon/prompt.template.md` | 166 | `\| File stuck-agent warrant \| \`gc bd create --type=warrant --label=pool:dog ...\` \|` | Pack-binding bug | Cmd quick-ref |

**Open question:** Does pool-label routing in code resolve through the
binding namespace, or by matching the raw label string? If the SDK
already canonicalizes `pool:dog` → `pool:<rig>/gastown.maintenance.dog`
internally on lookup, these are NOT bugs at runtime, just stale
documentation. **Design check needed before applying fixes.**

### I. Boot agent — bare `deacon` references (city-scoped)

Boot is `scope = "city"`; deacon is `scope = "city"`. Under
`[workspace.pack.gastown]`-style binding the deacon's qualified name
is `gastown.deacon`. Under bare `[workspace.pack]` the deacon's name
is just `deacon`. Today gastown is typically referenced via
`workspace.pack` without a binding alias — but the same family of
bug applies if a workspace ever uses a binding.

| File | Line | Snippet | Classification |
|---|---|---|---|
| `agents/boot/prompt.template.md` | 52 | `gc bd list --assignee=deacon --status=in_progress --json --limit=5` | Ambiguous (depends on workspace.pack binding) |
| `agents/boot/prompt.template.md` | 86 | `{{ cmd }} nudge deacon "Boot check: ..."` | Ambiguous |
| `agents/boot/prompt.template.md` | 125 | `\| Check deacon work \| \`gc bd list --assignee=deacon --status=in_progress --json\` \|` | Ambiguous |
| `agents/boot/prompt.template.md` | 49 | `{{ cmd }} agent peek deacon 30` | Ambiguous |

Boot only ever runs in a city that imports gastown via
`workspace.pack`. If `workspace.pack` accepts a binding alias,
these snippets all break. **Design check needed**: does
`workspace.pack` support binding aliases, and if so should boot
template these references through `{{ .Binding }}.deacon` or similar?

### J. Intentional-bare (correct under all binding regimes)

| File | Line | Snippet | Reason intentional |
|---|---|---|---|
| `formulas/mol-refinery-patrol.toml` | 293 | `--set-metadata gc.routed_to=human` | `human` is a special routing token for "blocked / needs human attention", not an agent role |
| `formulas/mol-deacon-patrol.toml` | 5; `mol-witness-patrol.toml` | 5; `mol-refinery-patrol.toml` | 5; `mol-polecat-work.toml` (multiple) | `--assignee=$GC_AGENT` / `--assignee="$GC_ALIAS"` / `--assignee="$GC_SESSION_NAME"` / `--assignee="$GC_SESSION_ID"` | Runtime-populated identity variables; correctly resolved by the controller |
| `assets/scripts/cycle.sh`, `tmux-theme.sh` | various | `*--polecat-*`, `*/witness`, `deacon`, etc. | Match against tmux session-name suffixes for theming/cycling; bare role-name suffix is the actual session-name pattern |

## Summary counts

- **Pack-binding bugs (high-confidence fixes):** 23
  - Already known / covered by lx-m9ckw: 4
  - New findings (mayor/crew/witness/polecat/refinery/molecules): 19
- **Ambiguous (design check needed before fix):** 8
  - Polecat session-name addressing under pack binding (#1397 family): 5
  - Boot's bare-`deacon` references (workspace.pack binding semantics): 3
  - **Open question:** Does pool-label routing canonicalize through binding? (~11 dog-pool sites depend on the answer)
- **Stale doc (`<rig>/polecats/<name>` form removed):** 3
- **Intentional bare:** all `$GC_*` identity vars; `gc.routed_to=human`; tmux session-suffix matching in scripts

## Coverage cross-reference

| Reference | Covers |
|---|---|
| `gastownhall/gascity#1486` (P1, pack-prefix routing mismatch) | A1, A2, A3, A4, B1, B2, C1, C2, D1, D3, E1, E5, F2-F5, G2, G3, all of H, all of I |
| `gastownhall/gascity#1397` (P2, prompt-template angle) | C4, D1, E2, E3, F6 (session-name addressing under binding) |
| `gastownhall/gascity` PR #1110 (CHANGES_REQUESTED) | Attempts a fix for #1486; status of fix scope vs this audit's findings is unclear from the bead description alone |
| `lx-m9ckw` (HQ) | A1, A2 only (refinery handoff) — does not cover A3, A4, or any of B-I |

## Recommended follow-up beads

The bead asks not to apply fixes here; the report is the deliverable.
For the overseer's planning, suggested follow-up beads:

1. **P1 — Mayor dispatch playbook (B1, B2):** Highest-priority. The
   exact symptom `gc-iypi` was filed for. Either replace bare form
   with qualified form in the mayor prompt, or rewrite the playbook
   to mandate `gc sling` (which stamps qualified form automatically).
2. **P1 — `template-fragments/approval-fallacy.template.md:44` (A4):**
   Source of the polecat done-sequence; lx-m9ckw should be extended
   to patch this fragment, otherwise A2 (the rendered polecat prompt)
   will keep regenerating with the bug.
3. **P2 — Witness/Refinery/Polecat prompts (D, E, F):** Bulk fix
   under #1486. Bind through `{{ .Binding }}` or precompute the
   qualified identity at template-render time.
4. **P2 — Witness patrol queue checks (G2, G3):** Same family as #3.
5. **Design check — Section H (dog pool):** Before bulk-fixing
   `pool:dog` / `gc.routed_to=dog`, confirm whether pool-label
   routing in the SDK canonicalizes through binding. The fix differs
   substantially based on the answer.
6. **Design check — Section I (boot/deacon):** Confirm whether
   `workspace.pack` supports binding aliases today. If so, bind
   `deacon` references through the workspace binding name.
7. **Cleanup — Stale `<rig>/polecats/<name>` references (C6, E4, G1):**
   Replace with the current canonical form documented at
   `agents/refinery/prompt.template.md:154`.

## Notes for the overseer

- This audit is read-only. No pack source files were modified.
- No upstream PRs were opened.
- Local lx-m9ckw patches were not touched.
- The underlying SDK behavior (specifically `AgentMatchesIdentity`,
  `RoutingTarget`, and pool-label routing) was not audited — that is
  upstream PR #1110's territory and out of scope for this bead.
- The two "Ambiguous" sections (H, I, plus the polecat session-name
  family) are blocking design questions — applying naive fixes
  without resolving them risks introducing new mismatches.
