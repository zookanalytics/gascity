package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/suspensionstate"
	"github.com/gastownhall/gascity/internal/testpolicy/resourcecensus"
)

// censusOwnerLivenessCheck detects resource-census ledger rows
// (test/test-resources.toml) whose owner_bead no longer resolves in the
// scope's bead store. Detection only: it never repairs the ledger.
//
// Only owner_bead ids in a namespace this city owns are checked. A census
// ledger is a repo-committed file, so a downstream city that vendors a
// repo inherits that repo's own bead ids — ids minted in a bead store this
// city has never had. Those can never resolve here, by construction rather
// than by rot, so reporting them as dangling would make the check
// permanently red for every adopter and would invite the one repair that
// is actually destructive: re-pointing correct upstream-authored rows.
type censusOwnerLivenessCheck struct {
	cfg      *config.City
	cityPath string
	newStore func(string) (beads.Store, error)
}

// newCensusOwnerLivenessCheck constructs a censusOwnerLivenessCheck.
func newCensusOwnerLivenessCheck(cfg *config.City, cityPath string, newStore func(string) (beads.Store, error)) *censusOwnerLivenessCheck {
	return &censusOwnerLivenessCheck{cfg: cfg, cityPath: cityPath, newStore: newStore}
}

// Name returns the check's identifier.
func (c *censusOwnerLivenessCheck) Name() string { return "census-owner-liveness" }

// CanFix reports that this check is detection-only.
func (c *censusOwnerLivenessCheck) CanFix() bool { return false }

// Fix is a no-op; this check never auto-repairs findings.
func (c *censusOwnerLivenessCheck) Fix(_ *doctor.CheckContext) error { return nil }

// Run scans the city and each non-suspended, path-bearing rig's
// resource-census ledger for owner_bead references that no longer resolve
// in that scope's bead store.
func (c *censusOwnerLivenessCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	var findings []string
	var skipped []string

	live := c.liveBeadPrefixes()
	c.scanScope(&findings, &skipped, live, "city", c.cityPath)
	if c.cfg != nil {
		suspState, _ := loadSuspensionState(fsys.OSFS{}, c.cityPath)
		for _, rig := range c.cfg.Rigs {
			if suspensionstate.EffectiveRigSuspended(suspState, rig.Name, rig.EffectiveSuspendedOnStart()) || strings.TrimSpace(rig.Path) == "" {
				continue
			}
			c.scanScope(&findings, &skipped, live, "rig "+rig.Name, rig.Path)
		}
	}

	if len(findings) == 0 && len(skipped) == 0 {
		return okCheck(c.Name(), "no dangling owner_bead references found in resource-census ledgers")
	}

	details := append([]string{}, findings...)
	details = append(details, skipped...)
	sort.Strings(details)

	if len(findings) == 0 {
		return warnCheck(c.Name(),
			fmt.Sprintf("census-owner-liveness check skipped %d scope(s)", len(skipped)),
			"fix bead store access, then rerun gc doctor",
			details)
	}

	message := fmt.Sprintf("found %d dangling owner_bead reference(s) in resource-census ledgers", len(findings))
	if len(skipped) > 0 {
		message = fmt.Sprintf("%s (and skipped %d scope(s))", message, len(skipped))
	}
	fixHint := "re-point the ledger row's owner_bead through council review (see TESTING.md), or fix bead store access and rerun gc doctor"
	return warnCheck(c.Name(), message, fixHint, details)
}

// liveBeadPrefixes returns the set of bead ID prefixes this city owns: the
// HQ store's prefix plus every configured rig's effective prefix. Suspended
// and path-less rigs are included — this check does not scan their ledgers,
// but their bead stores still exist, so an id in their namespace is one the
// city could resolve.
//
// The set is empty when the city config is unavailable. Callers must then
// check every owner_bead: an unknown namespace list is no evidence that an
// id is foreign, and suppressing findings on a guess would hide the ledger
// rot this check exists to catch.
func (c *censusOwnerLivenessCheck) liveBeadPrefixes() map[string]struct{} {
	if c.cfg == nil {
		return nil
	}
	live := map[string]struct{}{}
	add := func(prefix string) {
		if prefix = normalizeCensusBeadPrefix(prefix); prefix != "" {
			live[prefix] = struct{}{}
		}
	}
	add(config.EffectiveHQPrefix(c.cfg))
	for i := range c.cfg.Rigs {
		add(c.cfg.Rigs[i].EffectivePrefix())
	}
	return live
}

// censusOwnerBeadIsLocal reports whether id names a bead in a namespace this
// city owns, and so can be meaningfully checked for liveness here. An id
// carrying no prefix segment is treated as local: it cannot be attributed to
// a foreign store, and reporting it beats silently dropping it.
func censusOwnerBeadIsLocal(id string, live map[string]struct{}) bool {
	if len(live) == 0 {
		return true
	}
	prefix, _, ok := strings.Cut(strings.TrimSpace(id), "-")
	if !ok {
		return true
	}
	if prefix = normalizeCensusBeadPrefix(prefix); prefix == "" {
		return true
	}
	_, local := live[prefix]
	return local
}

// normalizeCensusBeadPrefix canonicalizes a bead ID prefix for comparison,
// mirroring how the beads stores normalize their own ID prefixes.
func normalizeCensusBeadPrefix(prefix string) string {
	return strings.Trim(strings.ToLower(strings.TrimSpace(prefix)), "-")
}

// scanScope loads the resource-census ledger at path, if any, and checks
// each unique owner_bead it references against the scope's bead store.
// A missing ledger file is expected for almost every scope and is skipped
// silently; any other load error, store-open error, or non-not-found Get
// error is recorded as a skip with a reason rather than treated as a
// dangling finding.
//
// Rows whose owner_bead falls outside live — the set of bead ID prefixes
// this city owns — are dropped before the store is opened, so a ledger that
// references only foreign ids costs no store access and reports nothing.
func (c *censusOwnerLivenessCheck) scanScope(findings, skipped *[]string, live map[string]struct{}, label, path string) {
	if c.newStore == nil || strings.TrimSpace(path) == "" {
		return
	}

	ledgerPath := filepath.Join(path, "test", "test-resources.toml")
	ledger, err := resourcecensus.LoadLedger(ledgerPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		*skipped = append(*skipped, fmt.Sprintf("%s skipped: loading resource-census ledger: %v", label, err))
		return
	}

	rows := collectCensusOwnerBeadRows(ledger)
	if len(rows) == 0 {
		return
	}

	ids := make([]string, 0, len(rows))
	for id := range rows {
		if !censusOwnerBeadIsLocal(id, live) {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids)

	store, err := c.newStore(path)
	if err != nil {
		*skipped = append(*skipped, fmt.Sprintf("%s skipped: opening bead store: %v", label, err))
		return
	}

	for _, id := range ids {
		_, err := store.Get(id)
		switch {
		case err == nil:
			continue
		case errors.Is(err, beads.ErrNotFound):
			*findings = append(*findings, fmt.Sprintf("%s: dangling owner_bead=%s rows=[%s]", label, id, strings.Join(rows[id], "; ")))
		default:
			*skipped = append(*skipped, fmt.Sprintf("%s skipped: checking owner_bead %s: %v", label, id, err))
		}
	}
}

// collectCensusOwnerBeadRows collects, per unique owner_bead, a
// human-readable descriptor of every ledger row that references it across
// all four row categories.
func collectCensusOwnerBeadRows(ledger resourcecensus.Ledger) map[string][]string {
	rows := map[string][]string{}

	addBaseline := func(category string, list []resourcecensus.Baseline) {
		for _, row := range list {
			id := strings.TrimSpace(row.OwnerBead)
			if id == "" {
				continue
			}
			desc := fmt.Sprintf("%s: scope=%s resource=%s", category, row.Scope, row.Resource)
			rows[id] = append(rows[id], desc)
		}
	}
	addBaseline("audit_baseline", ledger.AuditBaseline)
	addBaseline("debt", ledger.Debt)
	addBaseline("small_debt", ledger.SmallDebt)

	for _, row := range ledger.Medium {
		id := strings.TrimSpace(row.OwnerBead)
		if id == "" {
			continue
		}
		desc := fmt.Sprintf("medium: package_dir=%s package_name=%s owner=%s", row.PackageDir, row.PackageName, row.Owner)
		rows[id] = append(rows[id], desc)
	}

	return rows
}
