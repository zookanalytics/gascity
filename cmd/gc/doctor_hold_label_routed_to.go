package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// holdLabelExternalValue is the one hold:<value> value that never implies a
// routing gap: it names a human/out-of-system dependency, not an agent.
const holdLabelExternalValue = "external"

// holdLabelRoutedToCheck detects beads carrying a hold:<value> label whose
// gc.routed_to metadata is missing or names some route other than <value>'s
// target. gc.routed_to is the sole persisted routing key (ga-eld2x); a
// hold:<value> label with no matching gc.routed_to has silently drifted from
// its intended route. A binding-qualified route counts as matching its short
// hold value (routeDenotesHoldValue), and --fix backfills gc.routed_to in that
// qualified form wherever the city binds one.
type holdLabelRoutedToCheck struct {
	cfg      *config.City
	cityPath string
	newStore func(string) (beads.Store, error)
}

func newHoldLabelRoutedToCheck(cfg *config.City, cityPath string, newStore func(string) (beads.Store, error)) *holdLabelRoutedToCheck {
	return &holdLabelRoutedToCheck{cfg: cfg, cityPath: cityPath, newStore: newStore}
}

func (c *holdLabelRoutedToCheck) Name() string { return "hold-label-routed-to" }

func (c *holdLabelRoutedToCheck) CanFix() bool { return true }

func (c *holdLabelRoutedToCheck) WarmupEligible() bool { return false }

// holdLabelValue returns the hold value carried by labels, if any
// hold:<value> label is present and <value> is not "external".
func holdLabelValue(labels []string) (string, bool) {
	for _, l := range labels {
		val, ok := strings.CutPrefix(l, "hold:")
		if !ok {
			continue
		}
		val = strings.TrimSpace(val)
		if val == "" || val == holdLabelExternalValue {
			continue
		}
		return val, true
	}
	return "", false
}

// holdRouteTarget is a single bead whose hold:<value> label and gc.routed_to
// metadata have drifted apart. holdValue is the label's value; want is the
// gc.routed_to value Fix writes for it, which is the binding-qualified form
// when the city binds exactly one route under that value.
type holdRouteTarget struct {
	label     string
	store     beads.Store
	beadID    string
	holdValue string
	want      string
	got       string
}

// routeDenotesHoldValue reports whether route already names the hold value's
// target. The short form matches literally; a binding-qualified form matches
// when the city binds it as an alias of that short form. That pairing —
// short-form hold label, qualified route — is the one both this check and
// v2-routed-to-namespace call canonical, since only "mayor" and "external" are
// sanctioned hold values (engdocs/contributors/hold-label-conventions.md)
// while bound routes must carry their binding prefix.
func routeDenotesHoldValue(route, holdValue string, aliases map[string][]string) bool {
	if route == holdValue {
		return true
	}
	for _, canonical := range aliases[holdValue] {
		if route == canonical {
			return true
		}
	}
	return false
}

// holdRouteWant returns the gc.routed_to value to write for a hold value: the
// binding-qualified form when the city binds exactly one route under it, else
// the hold value as written. Backfilling the short form where a single bound
// alias exists would leave the bead flagged by v2-routed-to-namespace, whose
// own Fix would rewrite it back to the qualified form — so each gc doctor
// --fix pass would undo the previous one and neither check would ever go
// green (gc-yzxra / gc-sci79). Ambiguous values (more than one bound alias)
// keep the short form: the sibling check reports those but leaves them for
// manual resolution rather than guessing a rewrite target.
func holdRouteWant(holdValue string, aliases map[string][]string) string {
	if canonicals := aliases[holdValue]; len(canonicals) == 1 {
		return canonicals[0]
	}
	return holdValue
}

func (c *holdLabelRoutedToCheck) collect() (targets []holdRouteTarget, skipped []string) {
	aliases := boundRoutedToAliases(c.cfg)
	scopes := []struct{ label, path string }{{"city", c.cityPath}}
	if c.cfg != nil {
		for _, rig := range c.cfg.Rigs {
			if rig.Suspended || strings.TrimSpace(rig.Path) == "" {
				continue
			}
			scopes = append(scopes, struct{ label, path string }{"rig " + rig.Name, rig.Path})
		}
	}
	for _, sc := range scopes {
		if c.newStore == nil || strings.TrimSpace(sc.path) == "" {
			continue
		}
		store, err := c.newStore(sc.path)
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("%s skipped: opening bead store: %v", sc.label, err))
			continue
		}
		// hold:<value> carries a dynamic value suffix, so no targeted
		// label/metadata query is possible; AllowScan is required for a
		// broad filter (internal/beads/query.go). Status is left unset so the
		// scan matches every non-closed bead (open, in_progress, blocked,
		// deferred, ...), not just "open" — an exact Status match would
		// silently hide hold:<value> drift on any other status (ga-fm2vgd.2).
		items, err := store.List(beads.ListQuery{AllowScan: true})
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("%s skipped: listing beads: %v", sc.label, err))
			continue
		}
		for _, b := range items {
			holdValue, ok := holdLabelValue(b.Labels)
			if !ok {
				continue
			}
			got := strings.TrimSpace(b.Metadata[beadmeta.RoutedToMetadataKey])
			if routeDenotesHoldValue(got, holdValue, aliases) {
				continue
			}
			targets = append(targets, holdRouteTarget{
				label:     sc.label,
				store:     store,
				beadID:    b.ID,
				holdValue: holdValue,
				want:      holdRouteWant(holdValue, aliases),
				got:       got,
			})
		}
	}
	return targets, skipped
}

func (c *holdLabelRoutedToCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	targets, skipped := c.collect()
	if len(targets) == 0 && len(skipped) == 0 {
		return okCheck(c.Name(), "no hold:<value> labels are missing a matching gc.routed_to")
	}
	details := make([]string, 0, len(targets)+len(skipped))
	for _, tgt := range targets {
		details = append(details, fmt.Sprintf("%s bead %s has hold:%s but gc.routed_to=%q; use %q", tgt.label, tgt.beadID, tgt.holdValue, tgt.got, tgt.want))
	}
	details = append(details, skipped...)
	sort.Strings(details)
	if len(targets) == 0 {
		return warnCheck(c.Name(),
			fmt.Sprintf("hold-label-routed-to check skipped %d scope(s)", len(skipped)),
			"fix bead store access, then rerun gc doctor",
			details)
	}
	return warnCheck(c.Name(),
		fmt.Sprintf("%d bead(s) carry a hold:<value> label without matching gc.routed_to", len(targets)),
		"run gc doctor --fix to backfill gc.routed_to from the hold:<value> label",
		details)
}

func (c *holdLabelRoutedToCheck) Fix(_ *doctor.CheckContext) error {
	targets, skipped := c.collect()
	for _, tgt := range targets {
		if err := tgt.store.SetMetadata(tgt.beadID, beadmeta.RoutedToMetadataKey, tgt.want); err != nil {
			return fmt.Errorf("%s bead %s: backfill gc.routed_to: %w", tgt.label, tgt.beadID, err)
		}
	}
	if len(skipped) > 0 {
		return fmt.Errorf("hold-label-routed-to skipped %d scope(s): %s", len(skipped), strings.Join(skipped, "; "))
	}
	return nil
}
