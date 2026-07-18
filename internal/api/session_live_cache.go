package api

import (
	"time"

	"github.com/gastownhall/gascity/internal/session"
)

// sessionLiveCacheTTL bounds how stale the view=full warm cache may be before a
// request triggers a refresh. The live-observation fields it holds (running,
// active_bead, attached, last_active) change at roughly the controller patrol
// cadence (~30s), and the dashboard status bar polls on a ~15s loop, so a TTL in
// that band keeps the bar's data fresh while coalescing bursts of requests into
// a single refresh. It is a var (not a const) so tests can shorten or lengthen
// the window. See gc-tnvok.
var sessionLiveCacheTTL = 15 * time.Second

// sessionLiveCacheKey is the singleflight key for the per-Server warm refresh.
// Each Server owns one city, so a single constant key coalesces all concurrent
// refreshes of that city's session live fields into one runtime sweep.
const sessionLiveCacheKey = "sessions"

// sessionLiveFields is the set of per-session fields that require a live runtime
// probe (a tmux fork on the tmux provider) and therefore cannot be served from
// the cheap metadata-only summary projection. view=full overlays these onto the
// summary projection from the warm cache so the request path forks no tmux.
//
//   - running / attached / lastActive come from the per-session runtime
//     observation in session.Manager.EnrichInfo and the State() probe in
//     enrichSessionResponse (the ~2N uncached tmux forks per request this cache
//     eliminates).
//   - state carries the live active->asleep downgrade EnrichInfo applies, so
//     view=full keeps reporting a stale "awake" bead as asleep.
//   - activeBead is already fork-free (an in-process CachedList lookup); it is
//     cached alongside the others so the overlay is a single map read.
type sessionLiveFields struct {
	state      session.State
	running    bool
	activeBead string
	attached   bool
	lastActive time.Time
}

// sessionLiveSnapshot is a point-in-time map of live fields keyed by session ID,
// stamped with the time it was computed. view=full serves a snapshot directly
// (zero forks) and refreshes it asynchronously once it ages past
// sessionLiveCacheTTL.
type sessionLiveSnapshot struct {
	fields    map[string]sessionLiveFields
	fetchedAt time.Time
}

// sessionLiveFieldsSnapshot returns the warm map of per-session live fields for
// view=full, refreshing it stale-while-revalidate:
//
//   - Fresh snapshot (younger than the TTL): returned immediately, no probe.
//   - Cold cache (no snapshot yet): blocks once on a synchronous refresh so the
//     first request returns real data. singleflight coalesces concurrent cold
//     callers onto that one refresh.
//   - Stale snapshot: the current snapshot is returned immediately and a
//     refresh is kicked asynchronously for the next request, so the request path
//     never forks tmux. singleflight coalesces concurrent refreshes.
//
// The returned map is owned by the cache and must be treated as read-only by
// callers (the overlay only reads from it).
func (s *Server) sessionLiveFieldsSnapshot() map[string]sessionLiveFields {
	s.sessionLiveMu.Lock()
	snap := s.sessionLiveSnapshot
	s.sessionLiveMu.Unlock()

	if snap != nil && time.Since(snap.fetchedAt) < sessionLiveCacheTTL {
		return snap.fields
	}

	if snap == nil {
		// Cold cache: block once. The first view=full request pays for the
		// refresh so it returns real live fields instead of a blank snapshot.
		v, _, _ := s.sessionLiveGroup.Do(sessionLiveCacheKey, s.refreshSessionLiveSnapshot)
		if fields, ok := v.(map[string]sessionLiveFields); ok {
			return fields
		}
		return nil
	}

	// Stale cache: serve the current snapshot now and refresh in the background
	// so the request path stays fork-free. singleflight ensures the concurrent
	// refreshes collapse to one runtime sweep.
	go func() { _, _, _ = s.sessionLiveGroup.Do(sessionLiveCacheKey, s.refreshSessionLiveSnapshot) }()
	return snap.fields
}

// refreshSessionLiveSnapshot recomputes the live fields for every active session
// and stores the result as the current snapshot. It is the singleflight work
// function, so it runs at most once per coalesced refresh. On a transient read
// error it keeps the previous snapshot (returning its fields) rather than
// publishing an empty one, so a flaky store read does not blank out view=full.
func (s *Server) refreshSessionLiveSnapshot() (any, error) {
	fields := s.computeSessionLiveFields()
	if fields == nil {
		s.sessionLiveMu.Lock()
		prev := s.sessionLiveSnapshot
		s.sessionLiveMu.Unlock()
		if prev != nil {
			return prev.fields, nil
		}
		// Cold cache and the refresh failed: publish nothing and return an empty
		// (non-nil) map so the overlay is a no-op and the next request retries.
		return map[string]sessionLiveFields{}, nil
	}
	s.sessionLiveMu.Lock()
	s.sessionLiveSnapshot = &sessionLiveSnapshot{fields: fields, fetchedAt: time.Now()}
	s.sessionLiveMu.Unlock()
	return fields, nil
}

// computeSessionLiveFields runs the full enriched listing — the same
// filterEnrichReadModel + enrichSessionResponse path view=full used to run
// inline on every request — for the active sessions and projects each result
// down to its live fields. This is where the per-session tmux forks happen; the
// warm cache confines them to one sweep per TTL window instead of one per
// request. Returns nil on a store read error so the caller can keep the
// previous snapshot.
func (s *Server) computeSessionLiveFields() map[string]sessionLiveFields {
	store := s.state.SessionsBeadStore()
	if store.Store == nil {
		return nil
	}
	listings, _, err := sessionReadModelListings(session.NewStore(store))
	if err != nil {
		return nil
	}
	cfg := s.state.Config()
	mgr := s.sessionManager(store.Store)
	// Only active sessions carry live fields: EnrichInfo probes the runtime
	// solely for state==active, and closed/asleep/none beads observe nothing. So
	// the warm snapshot covers exactly the active set regardless of the request's
	// own state filter; the overlay simply skips any session absent from it.
	sessions, _ := filterEnrichReadModel(mgr, listings, string(session.StateActive), "")
	fields := make(map[string]sessionLiveFields, len(sessions))
	for _, sess := range sessions {
		var resp sessionResponse
		s.enrichSessionResponse(&resp, sess, cfg, s.runtimeSessionResponseHandle(sess), false, false, false, 0)
		fields[sess.ID] = sessionLiveFields{
			state:      sess.State,
			running:    resp.Running,
			activeBead: resp.ActiveBead,
			attached:   sess.Attached,
			lastActive: sess.LastActive,
		}
	}
	return fields
}

// applySessionLiveFields overlays one session's warm-cached live fields onto its
// summary-projected response. Sessions created since the last refresh are absent
// from the snapshot and keep the summary projection's zero live fields until the
// next refresh — the documented staleness of the stale-while-revalidate cache.
func applySessionLiveFields(resp *sessionResponse, lf sessionLiveFields) {
	if lf.state != "" {
		resp.State = string(lf.state)
	}
	resp.Running = lf.running
	resp.ActiveBead = lf.activeBead
	resp.Attached = lf.attached
	if !lf.lastActive.IsZero() {
		resp.LastActive = lf.lastActive.Format(time.RFC3339)
	}
}
