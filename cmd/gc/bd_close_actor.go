package main

import (
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// closeActorForOwnClaim returns the actor a session's owner-only bd operation
// on its own claimed work should run under, or "" to leave the actor alone.
// The operations are the ones ownClaimActorTargets selects: a close (`close`,
// `update --status closed`) and a lease refresh (`heartbeat <id>`).
//
// bd authorizes both by comparing the bead's assignee with the actor as
// strings. A pool session claims work under its session bead id but acts under
// its session name (BEADS_ACTOR), so its close of a bead it holds is refused
// ("assignee is <bead id>, actor is <session name>; reclaim or use --force"),
// and workers learn to force every close; its heartbeat of that bead is
// refused the same way, so every claimed lease expires minutes after the
// claim and `bd reclaim` cannot tell a live holder from a dead one. When every
// assigned target is held by this session's own identity, acting under that
// exact identity is the same principal speaking, and bd's check passes. A bead
// held by anyone else keeps the session's own actor, so bd still refuses it.
//
// The session's own identities are exactly its session bead id (GC_SESSION_ID,
// unique to this session) and the effective BEADS_ACTOR the bd child runs
// under. GC_SESSION_NAME and GC_ALIAS are deliberately not: for tmux_alias
// pools and legacy rows the session name is a shared chair (#6324), so
// treating it as this session's identity would let a successor close its
// predecessor's chair-named claim without --force. effectiveActor is the
// child's command-env BEADS_ACTOR, which can differ from the process env.
func closeActorForOwnClaim(bdArgs []string, targets map[string]beads.Bead, getenv func(string) string, effectiveActor string) string {
	ids, ok := ownClaimActorTargets(bdArgs)
	if !ok {
		return ""
	}
	sessionID := strings.TrimSpace(getenv("GC_SESSION_ID"))
	if sessionID == "" {
		return "" // not running as a session
	}
	effectiveActor = strings.TrimSpace(effectiveActor)
	own := map[string]bool{sessionID: true}
	if effectiveActor != "" {
		own[effectiveActor] = true
	}
	actor := ""
	for _, id := range ids {
		bead, ok := targets[id]
		if !ok {
			return "" // unread target: leave bd's check to decide
		}
		assignee := strings.TrimSpace(bead.Assignee)
		if assignee == "" {
			continue // unassigned beads pass bd's check under any actor
		}
		if !own[assignee] || (actor != "" && actor != assignee) {
			return ""
		}
		actor = assignee
	}
	if actor == effectiveActor {
		return ""
	}
	return actor
}

// ownClaimActorTargets returns the bead IDs of a bd invocation that bd
// authorizes owner-only against the bead's assignee, and whether the
// invocation is one at all. It covers the close forms workRecordCloseTargets
// recognizes (`close`, `update --status closed`) and the lone-id lease refresh
// `heartbeat <id>` (rewriteBdHeartbeatArgs has already reduced heartbeat to
// exactly that shape before doBd reaches this point). Anything else reports
// not-a-target so the actor rewrite stays out of the way.
func ownClaimActorTargets(bdArgs []string) ([]string, bool) {
	if len(bdArgs) == 2 && bdArgs[0] == "heartbeat" && strings.TrimSpace(bdArgs[1]) != "" {
		return bdArgs[1:2], true
	}
	return workRecordCloseTargets(bdArgs)
}

// ownClaimCloseEnv returns the bd child's env for bdArgs: unchanged, or with
// BEADS_ACTOR set to the claim identity closeActorForOwnClaim chose, judged
// against the BEADS_ACTOR already in env (what bd would otherwise see).
func ownClaimCloseEnv(env, bdArgs []string, targets map[string]beads.Bead, getenv func(string) string) []string {
	actor := closeActorForOwnClaim(bdArgs, targets, getenv, hookClaimEnvValue(env, "BEADS_ACTOR"))
	if actor == "" {
		return env
	}
	return withEnvValue(env, "BEADS_ACTOR", actor)
}

// withEnvValue returns env with key set to value, replacing every prior entry.
func withEnvValue(env []string, key, value string) []string {
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if name, _, ok := strings.Cut(entry, "="); ok && name == key {
			continue
		}
		out = append(out, entry)
	}
	return append(out, key+"="+value)
}
