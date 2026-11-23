package session

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

// TestPromptHash pins the empty-prompt gate, determinism, and distinctness.
func TestPromptHash(t *testing.T) {
	if got := PromptHash(""); got != "" {
		t.Fatalf("PromptHash(\"\") = %q, want empty (the P5 empty-prompt gate)", got)
	}
	a1 := PromptHash("hello world")
	a2 := PromptHash("hello world")
	if a1 == "" {
		t.Fatal("PromptHash of a non-empty prompt must be non-empty")
	}
	if a1 != a2 {
		t.Fatalf("PromptHash not deterministic: %q vs %q", a1, a2)
	}
	if b := PromptHash("hello world!"); b == a1 {
		t.Fatalf("distinct prompts hashed to the same value %q", a1)
	}
}

// TestCommitStartedPatchPriming proves the confirmation pair is both-or-neither
// and never stamps priming_attempted_at.
func TestCommitStartedPatchPriming(t *testing.T) {
	now := time.Date(2026, 7, 8, 1, 2, 3, 0, time.UTC)

	t.Run("zero PrimedAt stamps no priming keys", func(t *testing.T) {
		patch := CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", PromptHash: "h", Now: now})
		assertNoPrimingKeys(t, patch)
		if _, ok := patch["started_config_hash"]; !ok {
			t.Error("started_config_hash must still be written")
		}
	})

	t.Run("PrimedAt with empty hash stamps nothing (P5)", func(t *testing.T) {
		patch := CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", PrimedAt: now, PromptHash: "", Now: now})
		assertNoPrimingKeys(t, patch)
	})

	t.Run("both set stamps both, RFC3339 + verbatim", func(t *testing.T) {
		patch := CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", PrimedAt: now, PromptHash: "abc123", Now: now})
		if got, want := patch[PrimedAtMetadataKey], now.UTC().Format(time.RFC3339); got != want {
			t.Errorf("primed_at = %q, want %q", got, want)
		}
		if got := patch[PromptHashMetadataKey]; got != "abc123" {
			t.Errorf("prompt_hash = %q, want %q", got, "abc123")
		}
		if _, ok := patch[PrimingAttemptedAtMetadataKey]; ok {
			t.Error("CommitStartedPatch must never emit priming_attempted_at")
		}
	})

	t.Run("started_config_hash writer set unchanged", func(t *testing.T) {
		// The priming pair must not perturb started_config_hash's value.
		patch := CommitStartedPatch(CommitStartedPatchInput{CoreHash: "core-x", PrimedAt: now, PromptHash: "h", Now: now})
		if got := patch["started_config_hash"]; got != "core-x" {
			t.Errorf("started_config_hash = %q, want core-x", got)
		}
	})
}

// TestPrimingKeysClearedWhereverStartedConfigHashClears is the greppable form of
// the priming-key lifetime rule for the internal/session-owned clear sites
// (C-1..C-3): every one clears the three priming keys exactly when it clears
// started_config_hash.
func TestPrimingKeysClearedWhereverStartedConfigHashClears(t *testing.T) {
	t.Run("C-1 applyFreshWakeConversationReset", func(t *testing.T) {
		patch := MetadataPatch{}
		applyFreshWakeConversationReset(patch)
		assertClearsStartedHashAndPriming(t, patch)
	})

	t.Run("C-2 ConversationResetPatch clears when hash clears", func(t *testing.T) {
		cleared := ConversationResetPatch(true)
		assertClearsStartedHashAndPriming(t, cleared)

		// Churn arm keeps the hash — and therefore the markers.
		kept := ConversationResetPatch(false)
		if _, ok := kept["started_config_hash"]; ok {
			t.Fatal("churn arm must not clear started_config_hash")
		}
		for _, k := range primingResetKeys {
			if _, ok := kept[k]; ok {
				t.Errorf("churn arm must not clear priming key %s", k)
			}
		}
	})

	t.Run("C-7 RestartRequestPatch", func(t *testing.T) {
		patch := RestartRequestPatch("sess-key", time.Now())
		assertClearsStartedHashAndPriming(t, patch)
	})
}

// TestFreshWakeResetKeysAlignWithApply enforces the C-1 alignment note: the
// three priming keys appear in BOTH freshWakeConversationResetKeys and the
// applyFreshWakeConversationReset output.
func TestFreshWakeResetKeysAlignWithApply(t *testing.T) {
	listed := map[string]bool{}
	for _, k := range freshWakeConversationResetKeys {
		listed[k] = true
	}
	applied := MetadataPatch{}
	applyFreshWakeConversationReset(applied)
	for _, k := range primingResetKeys {
		if !listed[k] {
			t.Errorf("priming key %s missing from freshWakeConversationResetKeys", k)
		}
		if v, ok := applied[k]; !ok || v != "" {
			t.Errorf("priming key %s not cleared by applyFreshWakeConversationReset", k)
		}
	}
}

func assertNoPrimingKeys(t *testing.T, patch MetadataPatch) {
	t.Helper()
	for _, k := range primingResetKeys {
		if _, ok := patch[k]; ok {
			t.Errorf("unexpected priming key %s in patch", k)
		}
	}
}

func assertClearsStartedHashAndPriming(t *testing.T, patch MetadataPatch) {
	t.Helper()
	if got, ok := patch["started_config_hash"]; !ok || got != "" {
		t.Fatalf("started_config_hash not cleared (got %q, ok=%v)", got, ok)
	}
	for _, k := range primingResetKeys {
		if got, ok := patch[k]; !ok || got != "" {
			t.Errorf("priming key %s not cleared (got %q, ok=%v)", k, got, ok)
		}
	}
}

// The start commit writes the record of the conversation whose system prompt
// carries the rendered prompt as its caller decides: a session key sets it, an
// empty value clears it, and no value leaves it as it is.
func TestCommitStartedPatchRecordsTheSystemPromptConversation(t *testing.T) {
	now := time.Date(2026, 11, 22, 1, 2, 3, 0, time.UTC)
	key, none := "7f3c2a1e", ""

	patch := CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", Now: now, SystemPromptSessionKey: &key})
	if got := patch[SystemPromptSessionKeyMetadataKey]; got != key {
		t.Errorf("set: %s = %q, want %q", SystemPromptSessionKeyMetadataKey, got, key)
	}
	patch = CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", Now: now, SystemPromptSessionKey: &none})
	if got, ok := patch[SystemPromptSessionKeyMetadataKey]; !ok || got != "" {
		t.Errorf("clear: %s = %q (present %v), want an empty value that clears the record", SystemPromptSessionKeyMetadataKey, got, ok)
	}
	patch = CommitStartedPatch(CommitStartedPatchInput{CoreHash: "c", Now: now})
	if got, ok := patch[SystemPromptSessionKeyMetadataKey]; ok {
		t.Errorf("leave: %s = %q, want no write", SystemPromptSessionKeyMetadataKey, got)
	}
}

// A start that began a conversation records it when its launch passed the
// rendered prompt as the system prompt file, and clears the record when it
// passed none. A resume that passed the file leaves the record, whichever
// conversation it names, and one that passed none clears it. A value the row
// already holds writes nothing.
func TestSystemPromptSessionKeyAfterStart(t *testing.T) {
	show := func(p *string) string {
		if p == nil {
			return "nil"
		}
		return strconv.Quote(*p)
	}
	for _, tc := range []struct {
		name      string
		record    string
		began     bool
		passedKey string
		want      string // "nil" leaves the record; otherwise the quoted value written
	}{
		{name: "a new conversation with the file", record: "k1", began: true, passedKey: "k2", want: `"k2"`},
		{name: "a new conversation with the file, already recorded", record: "k1", began: true, passedKey: "k1", want: "nil"},
		{name: "a new conversation without the file", record: "k1", began: true, want: `""`},
		{name: "a new conversation without the file, no record", began: true, want: "nil"},
		{name: "a resume of the recorded conversation with the file", record: "k1", passedKey: "k1", want: "nil"},
		{name: "a resume of an older conversation with the file", record: "k1", passedKey: "k2", want: "nil"},
		{name: "a resume without the file", record: "k1", want: `""`},
		{name: "a resume without the file, no record", want: "nil"},
	} {
		got := SystemPromptSessionKeyAfterStart(Info{SystemPromptSessionKey: tc.record}, tc.began, tc.passedKey)
		if show(got) != tc.want {
			t.Errorf("%s: SystemPromptSessionKeyAfterStart = %s, want %s", tc.name, show(got), tc.want)
		}
		patch := SystemPromptSessionKeyPatch(got)
		if v, ok := patch[SystemPromptSessionKeyMetadataKey]; ok != (got != nil) || (got != nil && v != *got) {
			t.Errorf("%s: SystemPromptSessionKeyPatch = %v, want the record %s", tc.name, patch, show(got))
		}
	}
}

// A session Manager start records no conversation as begun with the rendered
// prompt in its system prompt: one that passes the system prompt file leaves
// the record, and one that passes none clears it, since a compaction in that
// life would leave the conversation's recorded system prompt without the
// prompt.
func TestManagerStartKeepsTheSystemPromptRecordOnlyWithTheFile(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	mgr := newTestManager(t, store, sp, WithStaleKeyDetectionWaiter(immediateStaleKeyDetectionWaiter))
	info, err := mgr.CreateSession(context.Background(), CreateOptions{Template: "helper", Command: "claude", WorkDir: "/tmp", Provider: "claude", ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	for k, v := range map[string]string{"session_key": "k1", SystemPromptSessionKeyMetadataKey: "k1"} {
		if err := store.SetMetadata(info.ID, k, v); err != nil {
			t.Fatalf("SetMetadata(%s): %v", k, err)
		}
	}
	record := func() string {
		t.Helper()
		b, err := store.Get(info.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return b.Metadata[SystemPromptSessionKeyMetadataKey]
	}
	resume := func(hints runtime.Config) {
		t.Helper()
		if err := sp.Stop(info.SessionName); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := mgr.Start(context.Background(), testActor(mgr, ActorOperator), info.ID, "claude --resume k1", hints); err != nil {
			t.Fatalf("Start(resume): %v", err)
		}
	}

	resume(runtime.Config{WorkDir: "/tmp", SystemPrompt: "# Role", SystemPromptFileFlag: "--append-system-prompt-file"})
	if got := record(); got != "k1" {
		t.Fatalf("record after a start that passed the system prompt file = %q, want k1 left as it was", got)
	}
	resume(runtime.Config{WorkDir: "/tmp"})
	if got := record(); got != "" {
		t.Fatalf("record after a start that passed no system prompt file = %q, want it cleared", got)
	}
}
