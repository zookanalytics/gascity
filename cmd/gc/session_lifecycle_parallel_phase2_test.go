package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/session/sessiontest"
	"github.com/gastownhall/gascity/internal/shellquote"
	workertest "github.com/gastownhall/gascity/internal/worker/workertest"
)

func TestPhase2InitialInputDelivery(t *testing.T) {
	reporter := newPhase2Reporter(t, "phase2-input-delivery")

	// The resume cases below model a legitimate resume (the keyed transcript is
	// present on disk). Stub the stale-resume probe to report "present" so the
	// pre-flight guard does not reclassify these resumes as fresh starts; the
	// guard's missing-transcript behavior is covered by TestStaleResumeKeyProbe
	// and the transcript layer's TestHasKeyedTranscript.
	prevProbe := staleResumeKeyProbe
	staleResumeKeyProbe = func([]string, string, string, string) (present, probeable bool) { return true, true }
	t.Cleanup(func() { staleResumeKeyProbe = prevProbe })

	for _, tc := range selectedPhase2ProviderCases(t) {
		tc := tc
		t.Run(string(tc.profileID), func(t *testing.T) {
			t.Run(string(workertest.RequirementInputInitialMessageFirstStart), func(t *testing.T) {
				prepared := preparePhase2Start(t, tc, "", map[string]string{
					"initial_message": "Do the first task.",
				})

				reporter.Require(t, initialMessageFirstStartResult(tc, prepared))
			})

			t.Run(string(workertest.RequirementInputInitialMessageResume), func(t *testing.T) {
				prepared := preparePhase2Start(t, tc, "already-started", map[string]string{
					"initial_message": "Do the first task.",
				})

				reporter.Require(t, initialMessageResumeResult(tc, prepared))
			})

			t.Run(string(workertest.RequirementInputOverrideDefaults), func(t *testing.T) {
				prepared := preparePhase2Start(t, tc, "", map[string]string{
					"initial_message":        "Ship it.",
					phase2OverrideOption(tc): tc.overrideValue,
				})

				reporter.Require(t, inputOverrideDefaultsResult(tc, prepared))
			})

			t.Run(string(workertest.RequirementInputInProgressResumeRestart), func(t *testing.T) {
				prepared := preparePhase2ResumeRestartStart(t, tc, map[string]string{
					"initial_message": "Do the first task.",
				}, true)

				reporter.Require(t, inProgressResumeRestartResult(tc, prepared))
			})

			t.Run(string(workertest.RequirementInputPreClaimResumeRestart), func(t *testing.T) {
				prepared := preparePhase2ResumeRestartStart(t, tc, map[string]string{
					"initial_message": "Do the first task.",
				}, false)

				reporter.Require(t, preClaimResumeRestartResult(tc, prepared))
			})
		})
	}
}

func TestPhase2HookEnabledClaudeFirstTurnStartupPayload(t *testing.T) {
	tc := phase2ProviderCaseForFamily(t, "claude")
	prepared := preparePhase2Start(t, tc, "", map[string]string{
		"initial_message": "Do the first task.",
	})

	if !prepared.candidate.tp.HookEnabled {
		t.Fatal("HookEnabled = false, want true for Claude phase2 profile")
	}
	if prepared.candidate.tp.ResolvedProvider == nil {
		t.Fatal("ResolvedProvider = nil, want Claude provider metadata")
	}
	if !prepared.candidate.tp.ResolvedProvider.SupportsHooks {
		t.Fatal("SupportsHooks = false, want true for Claude phase2 profile")
	}
	if prepared.cfg.PromptSuffix == "" {
		t.Fatal("PromptSuffix = empty, want first-turn startup payload to stay on launch for hook-enabled Claude")
	}
	// The rendered prompt rides the system prompt, and the first message is
	// the opener, which carries the worker nudge, so nothing is typed after.
	if got := prepared.cfg.SystemPrompt; got != "Base worker prompt" {
		t.Fatalf("SystemPrompt = %q, want the base startup prompt", got)
	}
	if got := prepared.cfg.SystemPromptFileFlag; got != "--append-system-prompt-file" {
		t.Fatalf("SystemPromptFileFlag = %q, want Claude's --append-system-prompt-file", got)
	}
	if got := prepared.cfg.Nudge; got != "" {
		t.Fatalf("Nudge = %q, want none after the opener", got)
	}

	payload, evidence, err := phase2PromptPayload(tc, prepared)
	if err != nil {
		t.Fatalf("phase2PromptPayload: %v (evidence=%v)", err, evidence)
	}
	if want := phase2Beacon(prepared) + "\n\nnudge-claude"; !strings.HasPrefix(payload, want+startupPromptNudgeSeparator) {
		t.Fatalf("payload = %q, want the opener %q first", payload, want)
	}
	if strings.Contains(payload, "Base worker prompt") {
		t.Fatalf("payload = %q, want the startup prompt out of the first message", payload)
	}
	if !strings.Contains(payload, "User message:\nDo the first task.") {
		t.Fatalf("payload = %q, want initial_message on first start", payload)
	}
	if strings.Count(payload, "Do the first task.") != 1 {
		t.Fatalf("payload = %q, want initial_message exactly once", payload)
	}

	// prompt_hash pins the rendered startup TEMPLATE prompt only. Even though the
	// delivered payload above carries the one-shot initial_message, the stored hash
	// must exclude it so a later Stage-4 re-derivation from the template still
	// matches (S19); hashing the delivered payload would re-prime the session
	// forever.
	if got, want := prepared.promptHash, sessionpkg.PromptHash("Base worker prompt"); got != want {
		t.Errorf("promptHash = %q, want base-template hash %q (initial_message must be excluded)", got, want)
	}
	if prepared.promptHash == sessionpkg.PromptHash(payload) {
		t.Errorf("promptHash must not hash the delivered payload %q (which includes initial_message)", payload)
	}
}

func TestPhase2InputResultFailureClassification(t *testing.T) {
	tc := selectedPhase2ProviderCases(t)[0]

	t.Run("prompt suffix parse failure stays requirement-scoped", func(t *testing.T) {
		prepared := preparePhase2Start(t, tc, "", map[string]string{
			"initial_message": "Do the first task.",
		})
		prepared.cfg.PromptSuffix = "'one' 'two'"

		result := initialMessageFirstStartResult(tc, prepared)
		if result.Status != workertest.ResultFail {
			t.Fatalf("result.Status = %q, want fail", result.Status)
		}
		if result.Requirement != workertest.RequirementInputInitialMessageFirstStart {
			t.Fatalf("result.Requirement = %q, want %q", result.Requirement, workertest.RequirementInputInitialMessageFirstStart)
		}
		if got := result.Evidence["prompt_suffix_parse_error"]; got == "" {
			t.Fatal("prompt_suffix_parse_error = empty, want parse failure evidence")
		}
	})

	t.Run("missing resolved provider fails without panic", func(t *testing.T) {
		prepared := preparePhase2Start(t, tc, "", map[string]string{
			"initial_message":        "Ship it.",
			phase2OverrideOption(tc): tc.overrideValue,
		})
		prepared.candidate.tp.ResolvedProvider = nil

		result := inputOverrideDefaultsResult(tc, prepared)
		if result.Status != workertest.ResultFail {
			t.Fatalf("result.Status = %q, want fail", result.Status)
		}
		if result.Requirement != workertest.RequirementInputOverrideDefaults {
			t.Fatalf("result.Requirement = %q, want %q", result.Requirement, workertest.RequirementInputOverrideDefaults)
		}
		if got := result.Evidence["resolved_provider"]; got != "" {
			t.Fatalf("resolved_provider = %q, want empty when provider is missing", got)
		}
	})
}

func preparePhase2Start(t *testing.T, tc phase2ProviderCase, startedConfigHash string, overrides map[string]string) *preparedStart {
	t.Helper()

	rawOverrides, err := json.Marshal(overrides)
	if err != nil {
		t.Fatalf("json.Marshal(overrides): %v", err)
	}

	store := beads.NewMemStore()
	metadata := map[string]string{
		"session_name":        "phase2-" + tc.family,
		"template":            "worker",
		"template_overrides":  string(rawOverrides),
		"started_config_hash": startedConfigHash,
	}
	if startedConfigHash != "" {
		metadata["session_key"] = "phase2-resume-key"
	}
	session, err := store.Create(beads.Bead{
		Title:    "phase2-" + tc.family,
		Type:     sessionBeadType,
		Labels:   []string{sessionBeadLabel},
		Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("Create session bead: %v", err)
	}

	prepared, err := prepareStartCandidate(startCandidate{
		info: sessiontest.SeedBead(t, session),
		tp:   phase2TemplateParams(t, tc, "Base worker prompt"),
	}, &config.City{}, store, &clock.Fake{Time: time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("prepareStartCandidate(%s): %v", tc.profileID, err)
	}
	return prepared
}

func preparePhase2ResumeRestartStart(t *testing.T, tc phase2ProviderCase, overrides map[string]string, assignedWork bool) *preparedStart {
	t.Helper()

	rawOverrides, err := json.Marshal(overrides)
	if err != nil {
		t.Fatalf("json.Marshal(overrides): %v", err)
	}

	store := beads.NewMemStore()
	session, err := store.Create(beads.Bead{
		Title:  "phase2-" + tc.family,
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"session_name":        "phase2-" + tc.family,
			"template":            "worker",
			"template_overrides":  string(rawOverrides),
			"started_config_hash": "already-started",
			"session_key":         "phase2-resume-key",
			// The conversation began with the prompt in its system prompt;
			// a profile without the flag ignores the record.
			sessionpkg.SystemPromptSessionKeyMetadataKey: "phase2-resume-key",
		},
	})
	if err != nil {
		t.Fatalf("Create session bead: %v", err)
	}

	if assignedWork {
		work, err := store.Create(beads.Bead{
			Title: "phase2 in-progress work",
			Type:  "task",
		})
		if err != nil {
			t.Fatalf("Create work bead: %v", err)
		}
		status := "in_progress"
		assignee := session.ID
		if err := store.Update(work.ID, beads.UpdateOpts{Status: &status, Assignee: &assignee}); err != nil {
			t.Fatalf("assign work bead: %v", err)
		}
	}

	tp := phase2TemplateParams(t, tc, "Base worker prompt")
	tp.Hints.Nudge = ""
	prepared, err := prepareStartCandidate(startCandidate{
		info: sessiontest.SeedBead(t, session),
		tp:   tp,
	}, &config.City{}, store, &clock.Fake{Time: time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("prepareStartCandidate(%s): %v", tc.profileID, err)
	}
	return prepared
}

// A Claude session's launches carry its rendered prompt in the system prompt,
// and the session row names the conversation that began that way. A fresh
// start's first message is the opener and its commit records the new
// conversation's key. A wake of that conversation types only the opener; a
// wake of a conversation that began before the record existed replays the
// prompt; both still pass the prompt as the system prompt file and write no
// record. A start that begins a conversation without the system prompt clears
// a record an earlier conversation left, and so does a wake that passes no
// system prompt file: a compaction in that life would leave the conversation's
// recorded system prompt without the prompt.
func TestReconcilerSystemPromptConversationRecordAndWakeOpener(t *testing.T) {
	prevProbe := staleResumeKeyProbe
	staleResumeKeyProbe = func([]string, string, string, string) (present, probeable bool) { return true, true }
	t.Cleanup(func() { staleResumeKeyProbe = prevProbe })

	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	prepareWith := func(t *testing.T, family string, metadata map[string]string, edit func(*TemplateParams)) *preparedStart {
		t.Helper()
		tc := phase2ProviderCaseForFamily(t, family)
		store := beads.NewMemStore()
		metadata["session_name"], metadata["template"] = "phase2-"+family, "worker"
		session, err := store.Create(beads.Bead{Title: "phase2-" + family, Type: sessionBeadType, Labels: []string{sessionBeadLabel}, Metadata: metadata})
		if err != nil {
			t.Fatalf("Create session bead: %v", err)
		}
		tp := phase2TemplateParams(t, tc, "Base worker prompt")
		if edit != nil {
			edit(&tp)
		}
		prepared, err := prepareStartCandidate(startCandidate{
			info: sessiontest.SeedBead(t, session),
			tp:   tp,
		}, &config.City{}, store, &clock.Fake{Time: now})
		if err != nil {
			t.Fatalf("prepareStartCandidate(%s): %v", family, err)
		}
		return prepared
	}
	prepare := func(t *testing.T, family string, metadata map[string]string) *preparedStart {
		t.Helper()
		return prepareWith(t, family, metadata, nil)
	}
	recorded := func(prepared *preparedStart) (string, bool) {
		v, ok := startCommitPatch(prepared, prepared.candidate.info, now)[sessionpkg.SystemPromptSessionKeyMetadataKey]
		return v, ok
	}

	t.Run("fresh start records its conversation", func(t *testing.T) {
		prepared := prepare(t, "claude", map[string]string{})
		key := prepared.candidate.info.SessionKey
		if key == "" {
			t.Fatal("fresh Claude start minted no session key")
		}
		if prepared.cfg.SystemPrompt != "Base worker prompt" || prepared.cfg.SystemPromptFileFlag != "--append-system-prompt-file" {
			t.Fatalf("system prompt = %q via %q, want the prompt via Claude's flag", prepared.cfg.SystemPrompt, prepared.cfg.SystemPromptFileFlag)
		}
		if want := shellquote.Quote(phase2Beacon(prepared) + "\n\nnudge-claude"); prepared.cfg.PromptSuffix != want || prepared.cfg.Nudge != "" {
			t.Fatalf("first message = %q, nudge = %q; want the opener %q and nothing typed after", prepared.cfg.PromptSuffix, prepared.cfg.Nudge, want)
		}
		if got, ok := recorded(prepared); !ok || got != key {
			t.Fatalf("commit records %q (written %v), want the new conversation's key %q", got, ok, key)
		}
	})

	t.Run("wake of the recorded conversation types only the opener", func(t *testing.T) {
		prepared := prepare(t, "claude", map[string]string{
			"started_config_hash": "already-started", "session_key": "k1", sessionpkg.SystemPromptSessionKeyMetadataKey: "k1",
		})
		if want := phase2Beacon(prepared) + "\n\nnudge-claude"; prepared.cfg.Nudge != want {
			t.Fatalf("wake nudge = %q, want only the opener %q", prepared.cfg.Nudge, want)
		}
		if prepared.cfg.PromptSuffix != "" || prepared.cfg.SystemPrompt != "Base worker prompt" {
			t.Fatalf("wake launch = suffix %q, system prompt %q; want no argv prompt and the system prompt file", prepared.cfg.PromptSuffix, prepared.cfg.SystemPrompt)
		}
		if got, ok := recorded(prepared); ok {
			t.Fatalf("wake commit writes the record %q, want it left alone", got)
		}
	})

	t.Run("wake of an older conversation replays the prompt", func(t *testing.T) {
		prepared := prepare(t, "claude", map[string]string{
			"started_config_hash": "already-started", "session_key": "k2", sessionpkg.SystemPromptSessionKeyMetadataKey: "k1",
		})
		if want := "Base worker prompt" + startupPromptNudgeSeparator + "nudge-claude"; prepared.cfg.Nudge != want {
			t.Fatalf("wake nudge = %q, want the replay %q", prepared.cfg.Nudge, want)
		}
		if prepared.cfg.SystemPrompt != "Base worker prompt" {
			t.Fatalf("SystemPrompt = %q, want the file to ride the wake", prepared.cfg.SystemPrompt)
		}
		if got, ok := recorded(prepared); ok {
			t.Fatalf("wake commit writes the record %q, want it left alone", got)
		}
	})

	t.Run("a wake that passes no system prompt file clears the record", func(t *testing.T) {
		prepared := prepareWith(t, "claude", map[string]string{
			"started_config_hash": "already-started", "session_key": "k1", sessionpkg.SystemPromptSessionKeyMetadataKey: "k1",
		}, func(tp *TemplateParams) {
			off := *tp.ResolvedProvider
			off.SystemPromptFileFlag = ""
			tp.ResolvedProvider = &off
		})
		if prepared.cfg.SystemPrompt != "" {
			t.Fatalf("SystemPrompt = %q, want none with the flag switched off", prepared.cfg.SystemPrompt)
		}
		if want := "Base worker prompt" + startupPromptNudgeSeparator + "nudge-claude"; prepared.cfg.Nudge != want {
			t.Fatalf("wake nudge = %q, want the replay %q", prepared.cfg.Nudge, want)
		}
		if got, ok := recorded(prepared); !ok || got != "" {
			t.Fatalf("commit records %q (written %v), want the record cleared", got, ok)
		}
	})

	t.Run("a start without the system prompt clears a stale record", func(t *testing.T) {
		prepared := prepare(t, "codex", map[string]string{sessionpkg.SystemPromptSessionKeyMetadataKey: "k1"})
		if prepared.cfg.SystemPrompt != "" {
			t.Fatalf("SystemPrompt = %q, want none for codex", prepared.cfg.SystemPrompt)
		}
		if got, ok := recorded(prepared); !ok || got != "" {
			t.Fatalf("commit records %q (written %v), want the stale record cleared", got, ok)
		}
	})
}
