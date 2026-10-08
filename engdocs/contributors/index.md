---
title: Contributors
description: The shortest path for new contributors to get productive in Gas City.
---

## Read These First

- [Codebase Map](codebase-map.md)
- [Docs Organization](docs-organization.md) when adding or moving pages in
  the published `docs/` tree
- [Architecture Overview](../architecture/index.md)
- [Primitive Test](primitive-test.md)
- [PR Review Handoff Notes](pr-review-handoff.md)
- [Reconciler Debugging](reconciler-debugging.md)
- [Infra-Class Store Routing Audit](infra-class-store-routing-audit.md) when
  opening a store on a bead id — which class a bead belongs to decides which
  store answers, and the audit inventories the sites that got it wrong
- [Bazel Test Hermeticity Audit](bazel-test-hermeticity-audit.md) when a
  test reads the network, the calendar, host tools, or shared paths — what
  a cached `bazel test` PASS can and cannot vouch for, and how the
  `test/bazel-hermeticity.toml` ledger tags such tests
- [Huma Usage Notes](huma-usage.md) when touching `internal/api/`,
  OpenAPI generation, or SSE registration
- [Excalidraw Setup](excalidraw-setup.md) when authoring diagrams for the docs
- [Hold and Blocked Label Conventions](hold-label-conventions.md) when a bead
  needs to pause on a specific actor or condition — only `hold:mayor` and
  `hold:external` are canonical
- [Release Gate Criteria Conventions](release-gate-criteria-conventions.md)
  when signing off the "Tests pass" criterion on a `release-gates/*.md`
  deploy gate — it must cite the CI jobs `ci-required` actually gates on
- [Contributor Response and Attribution Conventions](contributor-response-conventions.md)
  when replying to, superseding, adopting, or closing someone else's issue or
  PR — what the contributor is owed and how credit is recorded
- [Maintainer Environment](maintainer-environment.md) for maintainers on the
  shared build hosts or the internal bd ledger — contributors can skip it
- [Beads Version Bump Anchors](beads-version-bump-anchors.md) when moving the
  `github.com/steveyegge/beads` pin — the version lives in a dozen files, and
  four of them fail in places that never mention beads
- [Upstream sync 2026-10-07](upstream-sync-2026-10-07.md) — the carried-commit
  ledger for the fork: every commit kept, dropped, reworked or regenerated when
  rebasing onto upstream, with the upstream evidence behind each drop
- [Upstream sync 2026-10-13](upstream-sync-2026-10-13.md) — the next ledger:
  the 398-commit upstream catch-up (Actor verbs, SeatWork, issueops read
  roles, ChildrenOfAny), the five drops with their verification, and the
  reworks that followed upstream's direction
- [Upstream sync 2026-10-15](upstream-sync-2026-10-15.md) — the 65-commit
  catch-up: one drop (a supervisor test flake upstream fixed), the
  `gc nudge show` and pre-commit lint reworks onto upstream's typed nudge
  outcomes and hook fallback, and the census v3 regeneration
- [Upstream sync 2026-10-24](upstream-sync-2026-10-24.md) — the 134-commit
  catch-up (opjournal lanes, work ladder, reconciler v2 default): no whole
  drop, four absorbed hunks, the BdStore and order-timeout reworks, and the
  consolidation of patch commits into the commits they patched
- [`CONTRIBUTING.md`](https://github.com/gastownhall/gascity/blob/main/CONTRIBUTING.md)
- [`TESTING.md`](https://github.com/gastownhall/gascity/blob/main/TESTING.md)

## Expectations

- Keep current-state behavior in the architecture docs and future changes in
  the design docs.
- Treat the [Primitive Test](primitive-test.md) as the gate before adding new
  SDK surface area.
- Run `make check` before you open a PR.
- Run `make check-docs` when changing navigation, cross-links, or docs
  structure.

## Active Proposals

- [Testing Pyramid Audit and Hardening Plan](testing-pyramid-hardening-plan.md)
  for the proposed test-size, ownership, doubles, synchronization, and E2E
  direction

## When to Update Docs

- Update architecture docs when code behavior changes.
- Update design-doc status when a proposal is accepted, implemented, or
  superseded.
- Move exploratory notes, audits, and roadmaps into the archive instead of
  presenting them as current onboarding material.
