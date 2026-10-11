package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
)

// wakeScopeCity is a city with three configured stores: the city's own (lx)
// and two rigs (gc, tk), plus a rig declared without a path (ub).
func wakeScopeCity(t *testing.T) (cityPath string, cfg *config.City) {
	t.Helper()
	cityPath = t.TempDir()
	for _, rel := range []string{"rigs/gascity", "rigs/gc-toolkit", "elsewhere"} {
		if err := os.MkdirAll(filepath.Join(cityPath, rel), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}
	cfg = &config.City{
		Workspace: config.Workspace{Name: "test-city", Prefix: "lx"},
		Rigs: []config.Rig{
			{Name: "gascity", Path: filepath.Join(cityPath, "rigs", "gascity"), Prefix: "gc"},
			{Name: "gc-toolkit", Path: "rigs/gc-toolkit", Prefix: "tk"},
			{Name: "unbound", Prefix: "ub"},
		},
	}
	return cityPath, cfg
}

func prefixSet(prefixes map[string]struct{}) []string {
	if prefixes == nil {
		return nil
	}
	out := make([]string, 0, len(prefixes))
	for p := range prefixes {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

func TestWorkflowServeUnreadStorePrefixes(t *testing.T) {
	cityPath, cfg := wakeScopeCity(t)
	for _, tc := range []struct {
		name         string
		scope        string
		readsBinding bool
		want         []string
	}{
		{name: "rig scope reads only its rig", scope: filepath.Join(cityPath, "rigs", "gc-toolkit"), want: []string{"gc", "lx", "ub"}},
		{name: "rig scope reading the graph binding keeps the city prefix", scope: filepath.Join(cityPath, "rigs", "gc-toolkit"), readsBinding: true, want: []string{"gc", "ub"}},
		{name: "city scope reads only the city store", scope: cityPath, want: []string{"gc", "tk", "ub"}},
		{name: "a scope that is no configured store rules nothing out", scope: filepath.Join(cityPath, "elsewhere"), want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := prefixSet(workflowServeUnreadStorePrefixes(cfg, cityPath, tc.scope, tc.readsBinding))
			if !slices.Equal(got, tc.want) {
				t.Fatalf("unread store prefixes = %v, want %v", got, tc.want)
			}
		})
	}
	if got := workflowServeUnreadStorePrefixes(nil, cityPath, cityPath, false); got != nil {
		t.Fatalf("unread store prefixes with no config = %v, want nil", prefixSet(got))
	}
}

func TestWorkflowEventFromUnreadStore(t *testing.T) {
	_, cfg := wakeScopeCity(t)
	cfg.Rigs = append(cfg.Rigs, config.Rig{Name: "lx-ops", Path: "rigs/lx-ops", Prefix: "lx-ops"})
	unread := map[string]struct{}{"lx": {}, "gc": {}}

	for _, tc := range []struct {
		name string
		evt  events.Event
		want bool
	}{
		{name: "city wisp update", evt: events.Event{Type: events.BeadUpdated, Subject: "lx-wisp-v1xx5"}, want: true},
		{name: "other rig close", evt: events.Event{Type: events.BeadClosed, Subject: "gc-au2gxj"}, want: true},
		{name: "own rig create", evt: events.Event{Type: events.BeadCreated, Subject: "tk-brlyq3m"}, want: false},
		{name: "longest configured prefix wins", evt: events.Event{Type: events.BeadUpdated, Subject: "lx-ops-k2m9p"}, want: false},
		{name: "unconfigured prefix wakes", evt: events.Event{Type: events.BeadUpdated, Subject: "gcg-abc12"}, want: false},
		{name: "no subject wakes", evt: events.Event{Type: events.BeadUpdated}, want: false},
		{name: "not a bead event", evt: events.Event{Type: events.SessionStopped, Subject: "lx-wisp-v1xx5"}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := workflowEventFromUnreadStore(cfg, unread, tc.evt); got != tc.want {
				t.Fatalf("workflowEventFromUnreadStore(%s %s) = %t, want %t", tc.evt.Type, tc.evt.Subject, got, tc.want)
			}
		})
	}
	if workflowEventFromUnreadStore(cfg, nil, events.Event{Type: events.BeadUpdated, Subject: "lx-wisp-v1xx5"}) {
		t.Fatal("an empty unread set must not drop any event")
	}
}

func TestPumpWorkflowEventsDropsEventsTheSkipRejects(t *testing.T) {
	// scriptedWatcher (reconcile_wake_test.go) yields these, then an error.
	watcher := &scriptedWatcher{events: []events.Event{
		{Type: events.BeadUpdated, Subject: "lx-wisp-v1xx5"},
		{Type: events.BeadClosed, Subject: "tk-brlyq3m"},
		{Type: events.BeadUpdated, Subject: "lx-wisp-1gmhu"},
	}}
	done := make(chan struct{})
	defer close(done)
	eventCh := make(chan workflowWatchResult, 4)
	pumpWorkflowEvents(done, watcher, eventCh, func(evt events.Event) bool {
		return strings.HasPrefix(evt.Subject, "lx-")
	})

	first := <-eventCh
	if first.err != nil || first.evt.Subject != "tk-brlyq3m" {
		t.Fatalf("first delivery = %+v, want the tk-brlyq3m event", first)
	}
	second := <-eventCh
	if second.err == nil {
		t.Fatalf("second delivery = %+v, want the watcher's error", second)
	}
	if len(eventCh) != 0 {
		t.Fatalf("%d more deliveries, want none: a skipped event reached the serve loop", len(eventCh))
	}
}

func TestWorkflowServeUnreadStoreFilter(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "rigs", "gc-toolkit")
	if err := os.MkdirAll(rigPath, 0o755); err != nil {
		t.Fatalf("mkdir rig: %v", err)
	}
	cityToml := "[workspace]\nname = \"test-city\"\nprefix = \"lx\"\n\n" +
		"[[rigs]]\nname = \"gc-toolkit\"\npath = \"rigs/gc-toolkit\"\nprefix = \"tk\"\n"
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"), []byte(cityToml), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	controlAgent := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gc-toolkit"}
	cityEvent := events.Event{Type: events.BeadUpdated, Subject: "lx-wisp-v1xx5"}
	rigEvent := events.Event{Type: events.BeadUpdated, Subject: "tk-brlyq3m"}

	skip := workflowServeUnreadStoreFilter(controlAgent, cityPath, rigPath, workflowServeControlReadyQuery(controlAgent))
	if skip == nil {
		t.Fatal("filter = nil, want one for a control-ready scan of a rig store")
	}
	if !skip(cityEvent) {
		t.Fatalf("filter kept %s: the rig's scan never reads the city store", cityEvent.Subject)
	}
	if skip(rigEvent) {
		t.Fatalf("filter dropped %s: the rig's scan reads its own store", rigEvent.Subject)
	}

	custom := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gc-toolkit", WorkQuery: "bd ready --json --limit=1"}
	if skip := workflowServeUnreadStoreFilter(custom, cityPath, rigPath, custom.WorkQuery); skip != nil {
		t.Fatal("filter != nil for a custom work_query: what it reads is not known, so no event may be dropped")
	}
	if skip := workflowServeUnreadStoreFilter(controlAgent, t.TempDir(), rigPath, workflowServeControlReadyQuery(controlAgent)); skip != nil {
		t.Fatal("filter != nil for a city whose config does not load")
	}
}
