package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// primeCaptureTestStore stands up a file-backed city store the same way
// bd_env_test.go does, so persistPrimeHookProviderSessionKey — which resolves
// the city from GC_CITY and opens its own store handle — reads and writes the
// same on-disk store the test inspects.
func primeCaptureTestStore(t *testing.T) (cityDir string, store beads.Store) {
	t.Helper()
	cityDir = t.TempDir()
	t.Setenv("GC_BEADS", "file")
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore: %v", err)
	}
	t.Setenv("GC_CITY", cityDir)
	s, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	return cityDir, s
}

// createCaptureSessionBead creates a session bead for the given provider family
// with an empty session_key and returns its id.
func createCaptureSessionBead(t *testing.T, store beads.Store, providerKind string) string {
	t.Helper()
	b, err := store.Create(beads.Bead{
		Title: "session " + providerKind,
		Type:  "session",
		Metadata: map[string]string{
			"provider_kind": providerKind,
			"session_key":   "",
		},
	})
	if err != nil {
		t.Fatalf("create session bead: %v", err)
	}
	return b.ID
}

// isolateProviderSessionEnv clears the ambient provider-session env so the test
// exercises the hook-stdin capture path deterministically (the live session this
// test may run inside can otherwise leak GC_PROVIDER_SESSION_ID).
func isolateProviderSessionEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GC_PROVIDER_SESSION_ID", "")
	t.Setenv("GEMINI_SESSION_ID", "")
	t.Setenv("GC_PROVIDER_SESSION_ID_REQUIRED", "1")
}

// TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinCaptured is the
// regression guard: a claude session must capture the resume id its
// SessionStart hook delivers on stdin. Without it session_key stays empty,
// wake_mode=resume has nothing to resume, and every recycle starts fresh.
func TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinCaptured(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	id := createCaptureSessionBead(t, store, "claude")
	t.Setenv("GC_SESSION_ID", id)
	isolateProviderSessionEnv(t)

	const claudeSessionID = "8273e9ca-ff09-4260-a03a-1f8534cc1ba5"
	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey(claudeSessionID, &stderr)

	got := reloadSessionKey(t, cityDir, id)
	if got != claudeSessionID {
		t.Fatalf("claude session_key = %q, want %q (hook stdin session id must be captured for claude; stderr=%q)", got, claudeSessionID, stderr.String())
	}
	if !strings.Contains(stderr.String(), "persisted resume session_key") {
		t.Errorf("successful capture must be observable, got stderr=%q", stderr.String())
	}
}

// TestPersistPrimeHookProviderSessionKey_CodexHookStdinStillCaptured pins the
// pre-existing codex behavior so the claude fix does not regress it.
func TestPersistPrimeHookProviderSessionKey_CodexHookStdinStillCaptured(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	id := createCaptureSessionBead(t, store, "codex")
	t.Setenv("GC_SESSION_ID", id)
	isolateProviderSessionEnv(t)

	const codexSessionID = "codex-abc-123"
	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey(codexSessionID, &stderr)

	if got := reloadSessionKey(t, cityDir, id); got != codexSessionID {
		t.Fatalf("codex session_key = %q, want %q", got, codexSessionID)
	}
}

// TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinReconcilesStaleKey is
// the regression guard for the model-usage emission gap: a long-lived claude
// session whose transcript forked mid-conversation (compaction, /clear, a resume
// the provider forks to a new file) reaches its SessionStart hook with the LIVE
// conversation id on stdin while session_key is still pinned to the abandoned
// transcript. gc prime --hook is authoritative for that trusted stdin, so it
// reconciles the stored key to the live id and clears the usage cursor; leaving
// the stale key records zero usage for the whole awake interval.
func TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinReconcilesStaleKey(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	b, err := store.Create(beads.Bead{
		Title: "session claude",
		Type:  "session",
		Metadata: map[string]string{
			"provider_kind":           "claude",
			"session_key":             "dead-transcript-uuid",
			"invocation_usage_cursor": "msg_from_dead_transcript",
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Setenv("GC_SESSION_ID", b.ID)
	isolateProviderSessionEnv(t)

	const liveSessionID = "live-conversation-uuid"
	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey(liveSessionID, &stderr)

	if got := reloadSessionKey(t, cityDir, b.ID); got != liveSessionID {
		t.Fatalf("session_key = %q, want %q (a stale key must reconcile to the live hook-stdin id; stderr=%q)", got, liveSessionID, stderr.String())
	}
	if got := reloadMarker(t, cityDir, b.ID, "invocation_usage_cursor"); got != "" {
		t.Fatalf("invocation_usage_cursor = %q, want empty (reconcile must reset the cursor so the new transcript sweeps from its head)", got)
	}
	if !strings.Contains(stderr.String(), "reconciled stale resume session_key") {
		t.Errorf("reconcile must be observable, got stderr=%q", stderr.String())
	}
}

// TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinSameIDNoReconcile is the
// control: when the hook-stdin id already equals the stored key nothing is
// rewritten. The usage cursor is preserved and no reconcile diagnostic fires, so
// the once-per-conversation-start hook is idempotent for a session that has not
// forked its transcript.
func TestPersistPrimeHookProviderSessionKey_ClaudeHookStdinSameIDNoReconcile(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	const key = "same-uuid"
	b, err := store.Create(beads.Bead{
		Title: "session claude",
		Type:  "session",
		Metadata: map[string]string{
			"provider_kind":           "claude",
			"session_key":             key,
			"invocation_usage_cursor": "msg_live",
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Setenv("GC_SESSION_ID", b.ID)
	isolateProviderSessionEnv(t)

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey(key, &stderr)

	if got := reloadSessionKey(t, cityDir, b.ID); got != key {
		t.Fatalf("session_key = %q, want unchanged %q", got, key)
	}
	if got := reloadMarker(t, cityDir, b.ID, "invocation_usage_cursor"); got != "msg_live" {
		t.Fatalf("invocation_usage_cursor = %q, want unchanged msg_live (an equal id must not reset the cursor)", got)
	}
	if strings.Contains(stderr.String(), "reconciled") {
		t.Errorf("no reconcile expected for an equal id, got stderr=%q", stderr.String())
	}
}

// TestProviderAcceptsHookStdinSessionID locks the allowlist boundary: only the
// families whose SessionStart hook delivers their authoritative resume id on
// stdin (codex, claude) are accepted; every other family is not.
func TestProviderAcceptsHookStdinSessionID(t *testing.T) {
	cases := map[string]bool{
		"codex":    true,
		"claude":   true,
		"gemini":   false,
		"pi":       false,
		"opencode": false,
		"unknown":  false,
		"":         false,
	}
	for family, want := range cases {
		if got := providerAcceptsHookStdinSessionID(family); got != want {
			t.Errorf("providerAcceptsHookStdinSessionID(%q) = %v, want %v", family, got, want)
		}
	}
}

// TestPersistPrimeHookProviderSessionKey_UnsupportedFamilyHookStdinRejected pins
// the safety boundary: a family outside the allowlist must NOT capture a
// hook-stdin session id. Such providers surface their id via env instead, which
// is handled before this gate.
func TestPersistPrimeHookProviderSessionKey_UnsupportedFamilyHookStdinRejected(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	id := createCaptureSessionBead(t, store, "gemini")
	t.Setenv("GC_SESSION_ID", id)
	isolateProviderSessionEnv(t)

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey("11111111-2222-3333-4444-555555555555", &stderr)

	if got := reloadSessionKey(t, cityDir, id); got != "" {
		t.Fatalf("gemini session_key = %q, want empty (hook stdin id must not be captured for non-allowlisted families)", got)
	}
}

// TestPersistPrimeHookProviderSessionKey_UnsupportedFamilyNonEmptyNotReconciled
// pins the safety boundary in the reconcile direction: a non-allowlisted family
// (gemini) must not have a non-empty key overwritten from hook stdin. Only
// codex/claude deliver their authoritative live conversation id there; every
// other family surfaces resume state by another route.
func TestPersistPrimeHookProviderSessionKey_UnsupportedFamilyNonEmptyNotReconciled(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	b, err := store.Create(beads.Bead{
		Title:    "session gemini",
		Type:     "session",
		Metadata: map[string]string{"provider_kind": "gemini", "session_key": "gemini-original"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Setenv("GC_SESSION_ID", b.ID)
	isolateProviderSessionEnv(t)

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey("gemini-different", &stderr)

	if got := reloadSessionKey(t, cityDir, b.ID); got != "gemini-original" {
		t.Fatalf("gemini session_key = %q, want unchanged gemini-original (untrusted family must not reconcile from hook stdin)", got)
	}
}

// TestPersistPrimeHookProviderSessionKey_EnvPathNonEmptyNotReconciled confirms
// the reconcile is surgical to the hook-stdin path: an id delivered via
// GC_PROVIDER_SESSION_ID (fromHookStdin=false) is a launch-time env value, not
// the id of a forked live conversation, so it must never overwrite a non-empty
// key. Only the SessionStart hook's stdin carries the live transcript id.
func TestPersistPrimeHookProviderSessionKey_EnvPathNonEmptyNotReconciled(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	b, err := store.Create(beads.Bead{
		Title:    "session claude",
		Type:     "session",
		Metadata: map[string]string{"provider_kind": "claude", "session_key": "env-original"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Setenv("GC_SESSION_ID", b.ID)
	t.Setenv("GEMINI_SESSION_ID", "")
	t.Setenv("GC_PROVIDER_SESSION_ID_REQUIRED", "1")
	t.Setenv("GC_PROVIDER_SESSION_ID", "env-different")

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey("", &stderr)

	if got := reloadSessionKey(t, cityDir, b.ID); got != "env-original" {
		t.Fatalf("claude env session_key = %q, want unchanged env-original (env path must not reconcile a non-empty key)", got)
	}
}

// TestPersistPrimeHookProviderSessionKey_ClaudeEnvSessionIDCaptured confirms the
// change is surgical — it touches only the hook-stdin branch. An id delivered
// via GC_PROVIDER_SESSION_ID (fromHookStdin=false) is captured for claude
// regardless of the gate, exactly as before.
func TestPersistPrimeHookProviderSessionKey_ClaudeEnvSessionIDCaptured(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	id := createCaptureSessionBead(t, store, "claude")
	t.Setenv("GC_SESSION_ID", id)
	t.Setenv("GEMINI_SESSION_ID", "")
	t.Setenv("GC_PROVIDER_SESSION_ID_REQUIRED", "1")
	const envSessionID = "env-1a2b3c4d"
	t.Setenv("GC_PROVIDER_SESSION_ID", envSessionID)

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey("", &stderr)

	if got := reloadSessionKey(t, cityDir, id); got != envSessionID {
		t.Fatalf("claude env session_key = %q, want %q (env path must be unaffected by the stdin gate)", got, envSessionID)
	}
}

// TestPersistPrimeHookProviderSessionKey_RejectsIDEqualToGCSessionID guards the
// pre-existing collision check for the claude path: a provider id equal to the
// gc session id is never stored as a resume key.
func TestPersistPrimeHookProviderSessionKey_RejectsIDEqualToGCSessionID(t *testing.T) {
	cityDir, store := primeCaptureTestStore(t)
	id := createCaptureSessionBead(t, store, "claude")
	t.Setenv("GC_SESSION_ID", id)
	isolateProviderSessionEnv(t)

	var stderr bytes.Buffer
	persistPrimeHookProviderSessionKey(id, &stderr) // hook id == gc session id

	if got := reloadSessionKey(t, cityDir, id); got != "" {
		t.Fatalf("session_key = %q, want empty (provider id equal to GC_SESSION_ID must be rejected)", got)
	}
}

func reloadSessionKey(t *testing.T, cityDir, id string) string {
	t.Helper()
	return reloadMarker(t, cityDir, id, "session_key")
}

// reloadMarker reopens the on-disk store and returns a session bead's metadata
// value for key, so a test can assert what gc prime --hook actually persisted
// (session_key, invocation_usage_cursor) rather than an in-memory copy.
func reloadMarker(t *testing.T, cityDir, id, key string) string {
	t.Helper()
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get session bead: %v", err)
	}
	return strings.TrimSpace(b.Metadata[key])
}
