package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/session"
)

func newStuckCreatingCheck(store beads.Store, cfg *config.City, now time.Time) *stuckCreatingDoctorCheck {
	return &stuckCreatingDoctorCheck{
		cfg:      cfg,
		cityPath: "unused-city-path",
		newStore: func(string) (beads.Store, error) { return store, nil },
		now:      func() time.Time { return now },
	}
}

func createStuckCreatingSessionBead(t *testing.T, store beads.Store, meta map[string]string) beads.Bead {
	t.Helper()
	merged := map[string]string{"state": string(session.StateCreating)}
	for k, v := range meta {
		merged[k] = v
	}
	b, err := store.Create(beads.Bead{
		Title:    "session under test",
		Type:     session.BeadType,
		Labels:   []string{session.LabelSession},
		Metadata: merged,
	})
	if err != nil {
		t.Fatalf("Create(session bead): %v", err)
	}
	return b
}

func TestStuckCreatingCheckOKWithNoStuckSessions(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		seed func(t *testing.T, store beads.Store)
	}{
		{"no session beads", func(_ *testing.T, _ beads.Store) {}},
		{"active session", func(t *testing.T, store beads.Store) {
			createStuckCreatingSessionBead(t, store, map[string]string{"state": "active"})
		}},
		{"creating under warn threshold", func(t *testing.T, store beads.Store) {
			createStuckCreatingSessionBead(t, store, map[string]string{
				"pending_create_started_at": now.Add(-time.Minute).Format(time.RFC3339),
			})
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			store := beads.NewMemStore()
			tt.seed(t, store)
			r := newStuckCreatingCheck(store, nil, now).Run(nil)
			if r.Status != doctor.StatusOK {
				t.Fatalf("Run() status = %v, want StatusOK (message %q, details %v)", r.Status, r.Message, r.Details)
			}
		})
	}
}

func TestStuckCreatingCheckWarnsBetweenThresholds(t *testing.T) {
	now := time.Now().UTC()
	store := beads.NewMemStore()
	b := createStuckCreatingSessionBead(t, store, map[string]string{
		"template":                  "gascity/worker",
		"pending_create_started_at": now.Add(-4 * time.Minute).Format(time.RFC3339),
	})

	r := newStuckCreatingCheck(store, nil, now).Run(nil)

	if r.Status != doctor.StatusWarning {
		t.Fatalf("Run() status = %v, want StatusWarning (message %q)", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "gascity/worker") {
		t.Errorf("message %q does not name the stuck template", r.Message)
	}
	if !strings.Contains(r.Message, stuckCreatingWarnAfter.String()) {
		t.Errorf("message %q does not mention warn threshold %s", r.Message, stuckCreatingWarnAfter)
	}
	if len(r.Details) == 0 || !strings.Contains(strings.Join(r.Details, "\n"), b.ID) {
		t.Errorf("details %v do not reference stuck session bead %s", r.Details, b.ID)
	}
}

func TestStuckCreatingCheckFailsPastTwiceThreshold(t *testing.T) {
	now := time.Now().UTC()
	store := beads.NewMemStore()
	b := createStuckCreatingSessionBead(t, store, map[string]string{
		"template":                  "gascity/worker",
		"pending_create_started_at": now.Add(-7 * time.Minute).Format(time.RFC3339),
	})

	r := newStuckCreatingCheck(store, nil, now).Run(nil)

	if r.Status != doctor.StatusError {
		t.Fatalf("Run() status = %v, want StatusError (message %q)", r.Status, r.Message)
	}
	if r.Severity != doctor.SeverityBlocking {
		t.Errorf("Run() severity = %v, want SeverityBlocking", r.Severity)
	}
	if !strings.Contains(r.Message, "gascity/worker") {
		t.Errorf("message %q does not name the stuck template", r.Message)
	}
	if !strings.Contains(r.Message, stuckCreatingFailAfter.String()) {
		t.Errorf("message %q does not mention fail threshold %s", r.Message, stuckCreatingFailAfter)
	}
	if len(r.Details) == 0 || !strings.Contains(strings.Join(r.Details, "\n"), b.ID) {
		t.Errorf("details %v do not reference stuck session bead %s", r.Details, b.ID)
	}
	if r.FixHint == "" {
		t.Error("FixHint is empty; operators need a next step")
	}
}

// TestStuckCreatingCheckRespectsConfiguredStartupTimeout verifies that a slow
// start still within a configured [session].startup_timeout is not flagged
// (gc-c1rpx review P2): the fixed 3m/6m bands shift to begin at the timeout so
// the reconciler's valid-create window is honored before warn/fail fire.
func TestStuckCreatingCheckRespectsConfiguredStartupTimeout(t *testing.T) {
	now := time.Now().UTC()
	cfg := &config.City{Session: config.SessionConfig{StartupTimeout: "12m"}}

	t.Run("within startup_timeout is not flagged", func(t *testing.T) {
		store := beads.NewMemStore()
		// 7m would fail at the fixed 6m band, but the 12m startup_timeout means
		// the reconciler still considers this start valid.
		createStuckCreatingSessionBead(t, store, map[string]string{
			"template":                  "gascity/worker",
			"pending_create_started_at": now.Add(-7 * time.Minute).Format(time.RFC3339),
		})
		r := newStuckCreatingCheck(store, cfg, now).Run(nil)
		if r.Status != doctor.StatusOK {
			t.Fatalf("Run() status = %v, want StatusOK (start within 12m startup_timeout); message %q", r.Status, r.Message)
		}
	})

	t.Run("past startup_timeout plus grace fails", func(t *testing.T) {
		store := beads.NewMemStore()
		// 16m exceeds startup_timeout (12m) + the warn grace (3m) = 15m fail band.
		createStuckCreatingSessionBead(t, store, map[string]string{
			"template":                  "gascity/worker",
			"pending_create_started_at": now.Add(-16 * time.Minute).Format(time.RFC3339),
		})
		r := newStuckCreatingCheck(store, cfg, now).Run(nil)
		if r.Status != doctor.StatusError {
			t.Fatalf("Run() status = %v, want StatusError (past 12m+3m); message %q", r.Status, r.Message)
		}
	})
}

// TestStuckCreatingCheckPrefersPendingCreateMarker pins the age anchor to the
// per-attempt pending_create_started_at marker, not bead CreatedAt — the same
// preference the reconciler's staleness logic uses. A bead created long ago
// whose latest create attempt is recent must not be flagged.
func TestStuckCreatingCheckPrefersPendingCreateMarker(t *testing.T) {
	base := time.Now().UTC()
	store := beads.NewMemStore()
	// CreatedAt is stamped ≈base by the store; the marker says the current
	// attempt began 9 minutes later. At now=base+10m the attempt is only
	// 1 minute old even though the bead itself is 10 minutes old.
	createStuckCreatingSessionBead(t, store, map[string]string{
		"pending_create_started_at": base.Add(9 * time.Minute).Format(time.RFC3339),
	})

	r := newStuckCreatingCheck(store, nil, base.Add(10*time.Minute)).Run(nil)

	if r.Status != doctor.StatusOK {
		t.Fatalf("Run() status = %v, want StatusOK; anchor must prefer pending_create_started_at (message %q)", r.Status, r.Message)
	}
}

func TestStuckCreatingCheckFallsBackToCreatedAt(t *testing.T) {
	base := time.Now().UTC()
	store := beads.NewMemStore()
	b := createStuckCreatingSessionBead(t, store, nil) // no marker; CreatedAt ≈ base

	r := newStuckCreatingCheck(store, nil, base.Add(7*time.Minute)).Run(nil)

	if r.Status != doctor.StatusError {
		t.Fatalf("Run() status = %v, want StatusError via CreatedAt fallback (message %q, details %v)", r.Status, r.Message, r.Details)
	}
	if !strings.Contains(strings.Join(r.Details, "\n"), b.ID) {
		t.Errorf("details %v do not reference stuck session bead %s", r.Details, b.ID)
	}
}

// TestStuckCreatingCheckTreatsUnknownAnchorAsStuck covers corrupt beads with
// neither a parseable pending_create_started_at nor a CreatedAt: nothing can
// ever age them out, so the check reports them stuck — mirroring
// isStaleCreating's zero-CreatedAt handling.
func TestStuckCreatingCheckTreatsUnknownAnchorAsStuck(t *testing.T) {
	store := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     "gc-corrupt",
		Status: "open",
		Type:   session.BeadType,
		Metadata: map[string]string{
			"state":                     string(session.StateCreating),
			"pending_create_started_at": "not-a-timestamp",
		},
	}}, nil)

	r := newStuckCreatingCheck(store, nil, time.Now().UTC()).Run(nil)

	if r.Status != doctor.StatusError {
		t.Fatalf("Run() status = %v, want StatusError for unknown anchor (message %q)", r.Status, r.Message)
	}
	if !strings.Contains(strings.Join(r.Details, "\n"), "gc-corrupt") {
		t.Errorf("details %v do not reference corrupt session bead", r.Details)
	}
}

func TestStuckCreatingCheckAllowlistsPreStartTemplates(t *testing.T) {
	now := time.Now().UTC()
	cfgWithPreStart := &config.City{
		Workspace: config.Workspace{Name: "demo"},
		Agents: []config.Agent{{
			Name:     "worker",
			Dir:      "gascity",
			PreStart: []string{"./slow-warmup.sh"},
		}},
	}

	t.Run("template with pre_start is excluded", func(t *testing.T) {
		store := beads.NewMemStore()
		b := createStuckCreatingSessionBead(t, store, map[string]string{
			"template":                  "gascity/worker",
			"pending_create_started_at": now.Add(-30 * time.Minute).Format(time.RFC3339),
		})

		r := newStuckCreatingCheck(store, cfgWithPreStart, now).Run(nil)

		if r.Status != doctor.StatusOK {
			t.Fatalf("Run() status = %v, want StatusOK for allowlisted template (message %q)", r.Status, r.Message)
		}
		if !strings.Contains(strings.Join(r.Details, "\n"), b.ID) {
			t.Errorf("details %v do not surface allowlisted session %s for verbose visibility", r.Details, b.ID)
		}
	})

	t.Run("alias resolves the template when template metadata is absent", func(t *testing.T) {
		store := beads.NewMemStore()
		createStuckCreatingSessionBead(t, store, map[string]string{
			"alias":                     "gascity/worker",
			"pending_create_started_at": now.Add(-30 * time.Minute).Format(time.RFC3339),
		})

		r := newStuckCreatingCheck(store, cfgWithPreStart, now).Run(nil)

		if r.Status != doctor.StatusOK {
			t.Fatalf("Run() status = %v, want StatusOK for allowlisted alias (message %q)", r.Status, r.Message)
		}
	})

	t.Run("template without pre_start is still flagged", func(t *testing.T) {
		cfg := &config.City{
			Workspace: config.Workspace{Name: "demo"},
			Agents:    []config.Agent{{Name: "worker", Dir: "gascity"}},
		}
		store := beads.NewMemStore()
		createStuckCreatingSessionBead(t, store, map[string]string{
			"template":                  "gascity/worker",
			"pending_create_started_at": now.Add(-7 * time.Minute).Format(time.RFC3339),
		})

		r := newStuckCreatingCheck(store, cfg, now).Run(nil)

		if r.Status != doctor.StatusError {
			t.Fatalf("Run() status = %v, want StatusError for template without pre_start (message %q)", r.Status, r.Message)
		}
	})
}

func TestStuckCreatingCheckIgnoresClosedAndForeignStates(t *testing.T) {
	now := time.Now().UTC()
	store := beads.NewMemStore()
	stale := now.Add(-time.Hour).Format(time.RFC3339)

	closed := createStuckCreatingSessionBead(t, store, map[string]string{
		"pending_create_started_at": stale,
	})
	if err := store.Close(closed.ID); err != nil {
		t.Fatalf("Close(%s): %v", closed.ID, err)
	}
	createStuckCreatingSessionBead(t, store, map[string]string{
		"state":                     "active",
		"pending_create_started_at": stale,
	})
	createStuckCreatingSessionBead(t, store, map[string]string{
		"state":                     "start_pending",
		"pending_create_started_at": stale,
	})

	r := newStuckCreatingCheck(store, nil, now).Run(nil)

	if r.Status != doctor.StatusOK {
		t.Fatalf("Run() status = %v, want StatusOK; closed/active/start_pending beads must be ignored (message %q, details %v)", r.Status, r.Message, r.Details)
	}
}

func TestStuckCreatingCheckSkipsWhenStoreUnavailable(t *testing.T) {
	check := &stuckCreatingDoctorCheck{
		cityPath: "unused-city-path",
		newStore: func(string) (beads.Store, error) { return nil, errors.New("dolt offline") },
	}

	r := check.Run(nil)

	if r.Status != doctor.StatusWarning {
		t.Fatalf("Run() status = %v, want StatusWarning when store unavailable (message %q)", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "skipped") {
		t.Errorf("message %q should say diagnostics were skipped", r.Message)
	}
}

func TestStuckCreatingCheckMessageCapsIdentityList(t *testing.T) {
	now := time.Now().UTC()
	store := beads.NewMemStore()
	for i := 0; i < 7; i++ {
		createStuckCreatingSessionBead(t, store, map[string]string{
			"template":                  fmt.Sprintf("gascity/worker-%d", i),
			"pending_create_started_at": now.Add(-10 * time.Minute).Format(time.RFC3339),
		})
	}

	r := newStuckCreatingCheck(store, nil, now).Run(nil)

	if r.Status != doctor.StatusError {
		t.Fatalf("Run() status = %v, want StatusError (message %q)", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "+2 more") {
		t.Errorf("message %q should cap the identity list at 5 names and summarize the rest", r.Message)
	}
	if len(r.Details) < 7 {
		t.Errorf("details should list all 7 stuck sessions, got %d: %v", len(r.Details), r.Details)
	}
}

func TestStuckCreatingStartedAtAnchors(t *testing.T) {
	created := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	marker := time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)
	cases := []struct {
		name   string
		bead   beads.Bead
		want   time.Time
		wantOK bool
	}{
		{
			name: "marker preferred over CreatedAt",
			bead: beads.Bead{
				CreatedAt: created,
				Metadata:  map[string]string{"pending_create_started_at": marker.Format(time.RFC3339)},
			},
			want:   marker,
			wantOK: true,
		},
		{
			name:   "CreatedAt fallback when marker missing",
			bead:   beads.Bead{CreatedAt: created},
			want:   created,
			wantOK: true,
		},
		{
			name: "CreatedAt fallback when marker unparseable",
			bead: beads.Bead{
				CreatedAt: created,
				Metadata:  map[string]string{"pending_create_started_at": "garbage"},
			},
			want:   created,
			wantOK: true,
		},
		{
			name:   "no anchor available",
			bead:   beads.Bead{},
			wantOK: false,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stuckCreatingStartedAt(tt.bead)
			if ok != tt.wantOK {
				t.Fatalf("stuckCreatingStartedAt() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && !got.Equal(tt.want) {
				t.Errorf("stuckCreatingStartedAt() = %v, want %v", got, tt.want)
			}
		})
	}
}
