//go:build acceptance_c

package tutorialgoldens

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTutorial04Communication(t *testing.T) {
	ws := newTutorialWorkspace(t)
	ws.attachDiagnostics(t, "tutorial-04")

	myCity := expandHome(ws.home(), "~/my-city")
	myProject := expandHome(ws.home(), "~/my-project")
	var tutorialMailID string
	mustMkdirAll(t, myProject)

	out, err := ws.runShell("gc init ~/my-city --provider claude --skip-provider-readiness", "")
	if err != nil {
		t.Fatalf("seed city init: %v\n%s", err, out)
	}
	ws.setCWD(myCity)

	for _, cmd := range []string{"gc rig add ~/my-project"} {
		if out, err := ws.runShell(cmd, ""); err != nil {
			t.Fatalf("seed rig add %q: %v\n%s", cmd, err, out)
		}
	}

	if out, err := ws.runShell("gc agent add --name reviewer --dir my-project", ""); err != nil {
		t.Fatalf("seed reviewer scaffold: %v\n%s", err, out)
	}
	writeFile(t, filepath.Join(myCity, "agents", "reviewer", "agent.toml"), "dir = \"my-project\"\nprovider = \""+tutorialReviewerProvider()+"\"\n", 0o644)
	writeFile(t, filepath.Join(myCity, "agents", "reviewer", "prompt.template.md"), "# Reviewer\nReview code.\n", 0o644)
	ws.noteWarning("TODO(issue #632): once bare agent names reliably resolve to the enclosing rig in acceptance-style paths, simplify tutorial 04's rig-local reviewer references from `my-project/reviewer` to bare `reviewer` where the shell is already in the rig")

	if _, err := ws.waitForSessionByTemplateOrTarget("mayor", "mayor", 30*time.Second, time.Second); err != nil {
		t.Fatalf("resolve mayor session bead: %v", err)
	}
	wakeMayor := func(context string) {
		t.Helper()
		out, err := ws.runShell("gc session wake mayor", "")
		if err != nil {
			t.Fatalf("%s: %v\n%s", context, err, out)
		}
	}
	mayorReady := func() bool {
		peekOut, peekErr := ws.runShell("gc session peek mayor --lines 1", "")
		return peekErr == nil && strings.TrimSpace(peekOut) != ""
	}
	waitForMayorReady := func(context string) {
		t.Helper()
		if _, err := ws.waitForSessionByTemplateOrTarget("mayor", "mayor", 30*time.Second, time.Second); err != nil {
			t.Fatalf("resolve mayor session bead %s: %v", context, err)
		}
		if !waitForCondition(t, 30*time.Second, 1*time.Second, mayorReady) {
			out, _ := ws.runShell("gc session list", "")
			t.Fatalf("mayor session did not become peekable %s:\n%s", context, out)
		}
	}
	killMayor := func(context string) {
		t.Helper()
		out, err := ws.runShell("gc session kill mayor", "")
		if err != nil {
			lowerOut := strings.ToLower(out)
			if strings.Contains(lowerOut, "session not found") ||
				strings.Contains(lowerOut, "no session found") ||
				strings.Contains(lowerOut, "is not active") {
				ws.noteWarning("tutorial 04 runtime workaround: mayor was already stopped while requesting a session recycle, so the page driver skips the fatal gc session kill error and waits for the named-session reconciler to bring it back")
				return
			}
			t.Fatalf("%s: %v\n%s", context, err, out)
		}
		if !strings.Contains(out, " killed.") {
			t.Fatalf("%s output mismatch:\n%s", context, out)
		}
	}
	restartCity := func(context string) {
		ws.noteWarning("tutorial 04 runtime workaround: %s, so the page driver performs a hidden gc stop/gc start cycle before retrying the visible communication flow", context)
		if out, err := ws.runShell("gc stop", ""); err != nil {
			t.Fatalf("hidden gc stop during tutorial 04 recovery: %v\n%s", err, out)
		}
		if out, err := ws.runShell("gc start", ""); err != nil {
			t.Fatalf("hidden gc start during tutorial 04 recovery: %v\n%s", err, out)
		}
		wakeMayor("wake mayor after tutorial 04 hidden restart")
	}
	if !waitForCondition(t, 30*time.Second, 1*time.Second, mayorReady) {
		ws.noteWarning("tutorial 04 runtime workaround: gc init can leave mayor mid-restart, so the page driver explicitly wakes it before bootstrapping a fresh headless submit")
		wakeMayor("wake mayor during tutorial 04 bootstrap")
		if out, err := ws.runShell(`gc session submit mayor "__tutorial04_bootstrap__"`, ""); err != nil {
			t.Fatalf("seed mayor submit bootstrap: %v\n%s", err, out)
		}
	}
	if !waitForCondition(t, 30*time.Second, 1*time.Second, mayorReady) {
		restartCity("gc init left mayor unpeekable during communication bootstrap")
		if out, err := ws.runShell(`gc session submit mayor "__tutorial04_bootstrap__"`, ""); err != nil {
			t.Fatalf("seed mayor submit bootstrap after hidden restart: %v\n%s", err, out)
		}
	}
	waitForMayorReady("during tutorial 04 seed bootstrap")

	t.Run(`gc mail send mayor -s "Review needed" -m "Please look at the auth module changes in my-project"`, func(t *testing.T) {
		out, err := ws.runShell(`gc mail send mayor -s "Review needed" -m "Please look at the auth module changes in my-project"`, "")
		if err != nil {
			t.Fatalf("gc mail send mayor: %v\n%s", err, out)
		}
		if !strings.Contains(out, "Sent message") {
			t.Fatalf("mail send output mismatch:\n%s", out)
		}
		tutorialMailID = firstBeadID(out)
		if tutorialMailID == "" {
			t.Fatalf("mail send output did not include a message ID:\n%s", out)
		}
	})

	t.Run("gc mail check mayor", func(t *testing.T) {
		out, err := ws.runShell("gc mail check mayor", "")
		if err != nil {
			t.Fatalf("gc mail check mayor: %v\n%s", err, out)
		}
		if !strings.Contains(strings.ToLower(out), "unread") {
			t.Fatalf("mail check output mismatch:\n%s", out)
		}
	})

	t.Run("gc mail inbox mayor", func(t *testing.T) {
		out, err := ws.runShell("gc mail inbox mayor", "")
		if err != nil {
			t.Fatalf("gc mail inbox mayor: %v\n%s", err, out)
		}
		for _, want := range []string{"Review needed", "auth module changes in my-project"} {
			if !strings.Contains(out, want) {
				t.Fatalf("mail inbox missing %q:\n%s", want, out)
			}
		}
	})

	communicationNudge := `Check mail and hook status, then act accordingly`
	communicationPeekTimeout := 90 * time.Second
	communicationRetryTimeout := 90 * time.Second
	communicationSettleTimeout := 10 * time.Second
	var reviewerWorkID string
	nudgeMayor := func(context string) {
		out, err := ws.runShell(`gc session nudge mayor "`+communicationNudge+`"`, "")
		if err != nil {
			t.Fatalf("%s: %v\n%s", context, err, out)
		}
		if !strings.Contains(out, "Nudged mayor") && !strings.Contains(out, "Queued nudge for mayor") {
			t.Fatalf("%s output mismatch:\n%s", context, out)
		}
	}
	submitMayorFollowUp := func(context, message string) {
		t.Helper()
		out, err := ws.runShell(`gc session submit mayor "`+message+`" --intent follow_up`, "")
		if err != nil {
			t.Fatalf("%s: %v\n%s", context, err, out)
		}
		if !strings.Contains(out, "Queued follow-up for mayor") &&
			!strings.Contains(out, "Submitted follow-up to mayor") {
			t.Fatalf("%s output mismatch:\n%s", context, out)
		}
	}
	reviewerHandoffExists := func() bool {
		out, err := ws.runShell(`bd list --json --all --limit=5 --metadata-field gc.routed_to=my-project/reviewer --title "Review the auth module changes"`, "")
		if err != nil {
			return false
		}
		var beads []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		}
		if err := json.Unmarshal([]byte(out), &beads); err != nil {
			return false
		}
		for _, bead := range beads {
			if bead.Title == "Review the auth module changes" {
				reviewerWorkID = bead.ID
				return true
			}
		}
		return false
	}
	waitForReviewerHandoff := func() bool {
		return waitForCondition(t, communicationSettleTimeout, 1*time.Second, reviewerHandoffExists)
	}

	t.Run(`gc session nudge mayor "Check mail and hook status, then act accordingly"`, func(t *testing.T) {
		nudgeMayor("gc session nudge mayor")
	})

	t.Run("gc session peek mayor --lines 6", func(t *testing.T) {
		var out string
		mayorCommunicationVisible := func() bool {
			var err error
			out, err = ws.runShell("gc session peek mayor --lines 6", "")
			if err != nil {
				return false
			}
			return strings.Contains(out, "Review needed") ||
				strings.Contains(out, "auth module changes in my-project") ||
				strings.Contains(out, "Review the auth module changes") ||
				(strings.Contains(out, "my-project/reviewer") && strings.Contains(out, "auth module"))
		}
		if waitForCondition(t, communicationPeekTimeout, 2*time.Second, mayorCommunicationVisible) {
			return
		}
		if waitForReviewerHandoff() {
			ws.noteWarning("tutorial 04 runtime workaround: mayor already created the reviewer handoff bead, so the page driver submits a visibility-only follow-up that surfaces that existing coordination in peek output without creating new work")
			submitMayorFollowUp(
				"submit follow-up after reviewer handoff proof",
				`The earlier auth-change review already produced reviewer work bead `+reviewerWorkID+`. Summarize that existing routing in the transcript without creating or routing any new work.`,
			)
		} else {
			ws.noteWarning("tutorial 04 runtime workaround: the visible nudge can leave mayor with injected mail but no proven reviewer handoff yet, so the page driver explicitly wakes mayor and requeues the same mail-driven nudge before retrying the visible peek step")
			wakeMayor("wake mayor before communication retry")
			nudgeMayor("re-nudge mayor before communication retry")
		}
		if waitForCondition(t, communicationRetryTimeout, 2*time.Second, mayorCommunicationVisible) {
			return
		}
		if waitForReviewerHandoff() {
			ws.noteWarning("tutorial 04 runtime workaround: after the wake retry the reviewer handoff bead exists, so the page driver submits one visibility-only follow-up instead of asking mayor to route the same work again")
			submitMayorFollowUp(
				"submit follow-up after wake retry handoff proof",
				`Reviewer work bead `+reviewerWorkID+` already covers the earlier auth-change review. Summarize that existing coordination in the transcript without creating new work.`,
			)
		} else {
			ws.noteWarning("tutorial 04 runtime workaround: wake-only recovery can still leave mayor runtime state wedged, so the page driver force-kills just the mayor session and lets the named-session reconciler recreate it without restarting the whole city")
			killMayor("kill mayor before final communication retry")
			waitForMayorReady("after tutorial 04 session recycle")
			if waitForReviewerHandoff() {
				ws.noteWarning("tutorial 04 runtime workaround: after recycling mayor the reviewer handoff bead already exists, so the page driver submits a visibility-only follow-up against that proven routing")
				submitMayorFollowUp(
					"submit follow-up after mayor recycle handoff proof",
					`Reviewer work bead `+reviewerWorkID+` already captures the earlier auth-change review. Summarize that prior routing in the transcript without creating or routing new work.`,
				)
			} else {
				ws.noteWarning("tutorial 04 runtime workaround: after recycling mayor there is still no proven reviewer handoff bead, so the page driver gives the fresh runtime one final mail-driven nudge and otherwise lets the tutorial fail closed")
				nudgeMayor("re-nudge mayor after final communication recycle")
			}
		}
		if !waitForCondition(t, communicationRetryTimeout, 2*time.Second, mayorCommunicationVisible) {
			t.Fatalf("peek mayor did not surface communication flow in time:\n%s", out)
		}
	})

	if mayorPeek, err := ws.runShell("gc session peek mayor --lines 12", ""); err == nil {
		ws.noteDiagnostic("final mayor peek:\n%s", mayorPeek)
	}
	if mayorLogs, err := ws.runShell("gc session logs mayor --tail 5", ""); err == nil {
		ws.noteDiagnostic("final mayor logs:\n%s", mayorLogs)
	}
	if tutorialMailID != "" {
		if mailBead, err := ws.runShell("bd show "+tutorialMailID+" --json", ""); err == nil {
			ws.noteDiagnostic("tutorial mail bead:\n%s", mailBead)
		}
	}
	if data, err := os.ReadFile(filepath.Join(myCity, "city.toml")); err == nil {
		ws.noteDiagnostic("final city.toml:\n%s", string(data))
	}
}
