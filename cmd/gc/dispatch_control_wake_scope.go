package main

import (
	"io"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
)

// A city's event stream carries every store's bead events, and the --follow
// loop re-scans on each one it is woken by. A control dispatcher's readiness
// scan reads one scope's ledger, plus the city's graph binding on a split city.
// bd decides a bead's readiness from the rows of its own database, so a bead
// event in any other store cannot change the scan's answer, and waking on one
// only buys a scan that finds what the previous scan found. The idle sweep stays
// the backstop for writes that publish no event at all.

// workflowServeUnreadStoreFilter returns the predicate the --follow event pump
// drops events with: bead events for beads in a configured store this
// dispatcher's readiness scan never reads. It returns nil, which drops nothing,
// whenever the set of stores the scan reads is not known: a custom work_query,
// a city config that does not load, or a scope that is no configured store.
func workflowServeUnreadStoreFilter(agentCfg config.Agent, cityPath, storePath, workQuery string) func(events.Event) bool {
	if _, ok := parseControlReadyQuery(workflowServeWorkQuery(agentCfg, workQuery)); !ok {
		return nil
	}
	cfg, err := loadCityConfig(cityPath, io.Discard)
	if err != nil {
		workflowTracef("serve wake-scope off agent=%s: loading city config: %v", agentCfg.QualifiedName(), err)
		return nil
	}
	_, relocated := controlGraphBinding(cityPath, storePath)
	_, federated := controlGraphExtraLeg(cityPath, storePath)
	unread := workflowServeUnreadStorePrefixes(cfg, cityPath, resolveStoreScopeRoot(cityPath, storePath), relocated || federated)
	if len(unread) == 0 {
		workflowTracef("serve wake-scope off agent=%s: no configured store is outside the scan", agentCfg.QualifiedName())
		return nil
	}
	prefixes := make([]string, 0, len(unread))
	for prefix := range unread {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	workflowTracef("serve wake-scope agent=%s ignores bead events for prefixes=%s", agentCfg.QualifiedName(), strings.Join(prefixes, ","))
	return func(evt events.Event) bool {
		return workflowEventFromUnreadStore(cfg, unread, evt)
	}
}

// workflowServeUnreadStorePrefixes returns the bead-ID prefixes of the
// configured stores a control dispatcher scanning scopeRoot never reads, or nil
// when no store can be ruled out.
//
// The scan reads the scope's own ledger, and the city's graph binding when
// readsGraphBinding is set. Beads in a binding can carry the city's prefix, so a
// scan that reads one keeps the city prefix. A scope that is neither the city
// nor a configured rig yields nil: without knowing which store the scan reads,
// none can be ruled out.
func workflowServeUnreadStorePrefixes(cfg *config.City, cityPath, scopeRoot string, readsGraphBinding bool) map[string]struct{} {
	if cfg == nil {
		return nil
	}
	configured := make(map[string]struct{})
	read := make(map[string]struct{})
	scopeKnown := false
	if hq := strings.ToLower(strings.TrimSpace(config.EffectiveHQPrefix(cfg))); hq != "" {
		configured[hq] = struct{}{}
		if samePath(scopeRoot, cityPath) {
			scopeKnown = true
			read[hq] = struct{}{}
		}
		if readsGraphBinding {
			read[hq] = struct{}{}
		}
	}
	for i := range cfg.Rigs {
		prefix := strings.ToLower(strings.TrimSpace(cfg.Rigs[i].EffectivePrefix()))
		if prefix == "" {
			continue
		}
		configured[prefix] = struct{}{}
		if rigPath := resolvedRigPath(cityPath, cfg.Rigs[i].Path); rigPath != "" && samePath(rigPath, scopeRoot) {
			scopeKnown = true
			read[prefix] = struct{}{}
		}
	}
	if !scopeKnown {
		return nil
	}
	unread := make(map[string]struct{})
	for prefix := range configured {
		if _, ok := read[prefix]; !ok {
			unread[prefix] = struct{}{}
		}
	}
	return unread
}

// workflowEventFromUnreadStore reports whether evt is a bead event for a bead
// whose ID carries one of the unread prefixes. The prefix is the longest
// configured one the ID begins with, so a hyphenated rig prefix is not mistaken
// for a shorter one. An ID with no configured prefix is never ruled out.
func workflowEventFromUnreadStore(cfg *config.City, unread map[string]struct{}, evt events.Event) bool {
	if len(unread) == 0 || !workflowEventRelevant(evt) {
		return false
	}
	subject := strings.TrimSpace(evt.Subject)
	if subject == "" {
		return false
	}
	_, ok := unread[strings.ToLower(beadPrefix(cfg, subject))]
	return ok
}
