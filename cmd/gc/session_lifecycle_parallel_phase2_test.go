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
	staleResumeKeyProbe = func(string, string, string) (present, probeable bool) { return true, true }
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
					"initial_message": "Ship it.",
					"model":           tc.wantModelOverride,
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
	if got := prepared.cfg.Nudge; got != "nudge-claude" {
		t.Fatalf("Nudge = %q, want existing worker nudge preserved separately", got)
	}

	payload, evidence, err := phase2PromptPayload(tc, prepared)
	if err != nil {
		t.Fatalf("phase2PromptPayload: %v (evidence=%v)", err, evidence)
	}
	if !strings.Contains(payload, "Base worker prompt") {
		t.Fatalf("payload = %q, want base startup prompt", payload)
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
			"initial_message": "Ship it.",
			"model":           tc.wantModelOverride,
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
		session: &session,
		tp:      phase2TemplateParams(t, tc, "Base worker prompt"),
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
		session: &session,
		tp:      tp,
	}, &config.City{}, store, &clock.Fake{Time: time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("prepareStartCandidate(%s): %v", tc.profileID, err)
	}
	return prepared
}

// TestPhase2ResumeRestartNudgePreservedWithoutPromptReplay is the regression
// guard for gc-7go2a, which narrows #2477. On a resume-mode restart an agent
// that already carries a non-empty startup nudge must receive the nudge ALONE:
// the provider's --resume path rehydrates the prior conversation (the rendered
// role prompt included), so prepending that prompt to the nudge only duplicates
// the already-restored role (the ~20k-token per-wake re-injection documented in
// gc-cbtfq). The nudge-LESS branch keeps #2477's behavior — the rendered prompt
// is the only payload that stops a resumed session from landing idle — and is
// covered by TestPhase2InitialInputDelivery's ResumeRestart requirements.
func TestPhase2ResumeRestartNudgePreservedWithoutPromptReplay(t *testing.T) {
	// Model a legitimate resume (keyed transcript present) so the pre-flight
	// guard does not reclassify these resumes as fresh starts; mirrors the stub
	// in TestPhase2InitialInputDelivery.
	prevProbe := staleResumeKeyProbe
	staleResumeKeyProbe = func(string, string, string) (present, probeable bool) { return true, true }
	t.Cleanup(func() { staleResumeKeyProbe = prevProbe })

	for _, tc := range selectedPhase2ProviderCases(t) {
		tc := tc
		t.Run(string(tc.profileID), func(t *testing.T) {
			prepared, prompt, nudge := preparePhase2ResumeRestartStartWithNudge(t, tc)

			switch {
			case strings.TrimSpace(nudge) == "":
				t.Fatalf("setup: resume restart nudge is empty; need a non-empty startup nudge to exercise the nudge-having branch")
			case strings.TrimSpace(prompt) == "":
				t.Fatalf("setup: rendered prompt is empty; need a non-empty prompt to detect re-injection")
			case prepared.cfg.Nudge != nudge:
				t.Fatalf("cfg.Nudge = %q, want the startup nudge alone (%q): the rendered prompt must not be prepended on a resume restart", prepared.cfg.Nudge, nudge)
			case strings.Contains(prepared.cfg.Nudge, prompt):
				t.Fatalf("cfg.Nudge = %q, want it to omit the rendered prompt %q (already rehydrated via --resume)", prepared.cfg.Nudge, prompt)
			case strings.TrimSpace(prepared.cfg.PromptSuffix) != "":
				t.Fatalf("cfg.PromptSuffix = %q, want empty on a resume restart", prepared.cfg.PromptSuffix)
			case strings.TrimSpace(prepared.cfg.PromptFlag) != "":
				t.Fatalf("cfg.PromptFlag = %q, want empty on a resume restart", prepared.cfg.PromptFlag)
			case prepared.cfg.Env[startupPromptDeliveredEnv] != "1":
				// Marker stays set so the SessionStart hook suppresses prompt
				// re-injection: the prompt is present via --resume, not via the
				// nudge, but either way the hook must not re-add it.
				t.Fatalf("%s = %q, want 1 so the SessionStart hook does not re-inject the resumed prompt", startupPromptDeliveredEnv, prepared.cfg.Env[startupPromptDeliveredEnv])
			}
		})
	}
}

// preparePhase2ResumeRestartStartWithNudge mirrors preparePhase2ResumeRestartStart
// but keeps the resolved template's non-empty startup nudge instead of clearing
// it, exercising the nudge-having resume-restart branch. It returns the prepared
// start along with the rendered prompt and the startup nudge so callers can
// assert the prompt is not folded into the nudge. The restart-nudge branch does
// not depend on assigned work, so this helper omits the work-bead machinery.
func preparePhase2ResumeRestartStartWithNudge(t *testing.T, tc phase2ProviderCase) (prepared *preparedStart, prompt, nudge string) {
	t.Helper()

	store := beads.NewMemStore()
	session, err := store.Create(beads.Bead{
		Title:  "phase2-" + tc.family,
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"session_name":        "phase2-" + tc.family,
			"template":            "worker",
			"started_config_hash": "already-started",
			"session_key":         "phase2-resume-key",
		},
	})
	if err != nil {
		t.Fatalf("Create session bead: %v", err)
	}

	tp := phase2TemplateParams(t, tc, "Base worker prompt")
	prepared, err = prepareStartCandidate(startCandidate{
		session: &session,
		tp:      tp,
	}, &config.City{}, store, &clock.Fake{Time: time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("prepareStartCandidate(%s): %v", tc.profileID, err)
	}
	return prepared, tp.Prompt, tp.Hints.Nudge
}
