package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

// releaseNameTestCity stands up a city store and returns it plus a helper that
// creates session beads holding runtime identifiers.
func releaseNameTestCity(t *testing.T) (beads.Store, func(metadata map[string]string) beads.Bead) {
	t.Helper()
	clearGCEnv(t)
	clearInheritedCityRoutingEnv(t)
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")

	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", cityDir)
	writeNamedSessionCityTOML(t, cityDir)

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt(%q): %v", cityDir, err)
	}
	create := func(metadata map[string]string) beads.Bead {
		t.Helper()
		created, err := store.Create(beads.Bead{
			Title:    metadata["session_name"],
			Type:     session.BeadType,
			Labels:   []string{session.LabelSession},
			Metadata: metadata,
		})
		if err != nil {
			t.Fatalf("create session bead: %v", err)
		}
		return created
	}
	return store, create
}

// TestCmdSessionReleaseNameClearsClosedHolder is the end-to-end operator lever:
// a CLOSED session bead reserving an on_demand agent's runtime name is cleared
// by one command, with no metadata surgery and no bead ID to look up first.
func TestCmdSessionReleaseNameClearsClosedHolder(t *testing.T) {
	const (
		sessionName = "shutupandlisten--gc-toolkit__refinery"
		identity    = "shutupandlisten/gc-toolkit.refinery"
	)
	store, create := releaseNameTestCity(t)
	holder := create(map[string]string{
		"session_name":                        sessionName,
		"alias":                               identity,
		"agent_name":                          identity,
		session.CanonicalInstanceNameMetadata: identity,
		"template":                            "test",
	})
	if err := store.Close(holder.ID); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionReleaseName(sessionName, &stdout, &stderr, true); code != 0 {
		t.Fatalf("cmdSessionReleaseName = %d, want 0; stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	var got sessionActionResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &got); err != nil {
		t.Fatalf("stdout is not JSON: %v; stdout=%q", err, stdout.String())
	}
	if got.Action != "release-name" {
		t.Fatalf("action = %q, want release-name", got.Action)
	}
	if got.Identity != sessionName {
		t.Fatalf("identity = %q, want %q", got.Identity, sessionName)
	}
	if got.Count == nil || *got.Count != 1 {
		t.Fatalf("count = %v, want 1", got.Count)
	}
	if got.SessionID != holder.ID {
		t.Fatalf("session_id = %q, want %q", got.SessionID, holder.ID)
	}

	after, err := store.Get(holder.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for _, key := range []string{"session_name", "alias", session.CanonicalInstanceNameMetadata} {
		if v := after.Metadata[key]; v != "" {
			t.Fatalf("metadata[%q] = %q, want empty after release", key, v)
		}
	}
}

// TestCmdSessionReleaseNameRefusesLiveHolder keeps the lever from stealing a
// name out from under a running session.
func TestCmdSessionReleaseNameRefusesLiveHolder(t *testing.T) {
	const sessionName = "shutupandlisten--gc-toolkit__refinery"
	store, create := releaseNameTestCity(t)
	holder := create(map[string]string{
		"session_name": sessionName,
		"state":        string(session.StateActive),
		"template":     "test",
	})

	var stdout, stderr bytes.Buffer
	if code := cmdSessionReleaseName(sessionName, &stdout, &stderr, false); code == 0 {
		t.Fatalf("cmdSessionReleaseName(live holder) = 0, want non-zero; stdout=%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), holder.ID) {
		t.Fatalf("stderr = %q, want it to name the live holder %s", stderr.String(), holder.ID)
	}

	after, err := store.Get(holder.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Metadata["session_name"] != sessionName {
		t.Fatalf("session_name = %q, want %q left intact", after.Metadata["session_name"], sessionName)
	}
}

// TestCmdSessionReleaseNameNoHolder reports a clean no-op, so the command is
// safe to run while diagnosing.
func TestCmdSessionReleaseNameNoHolder(t *testing.T) {
	_, _ = releaseNameTestCity(t)

	var stdout, stderr bytes.Buffer
	if code := cmdSessionReleaseName("nobody-holds-this", &stdout, &stderr, true); code != 0 {
		t.Fatalf("cmdSessionReleaseName = %d, want 0; stderr=%s", code, stderr.String())
	}
	var got sessionActionResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &got); err != nil {
		t.Fatalf("stdout is not JSON: %v; stdout=%q", err, stdout.String())
	}
	if got.Count == nil || *got.Count != 0 {
		t.Fatalf("count = %v, want 0", got.Count)
	}
}
