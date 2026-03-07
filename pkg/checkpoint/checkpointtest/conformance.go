// Package checkpointtest provides a conformance test suite for
// checkpoint.Store implementations.
package checkpointtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gastownhall/gascity/pkg/checkpoint"
)

// RunStoreTests runs the full conformance suite against a Store implementation.
// The newStore function must return a fresh, empty store for each call.
func RunStoreTests(t *testing.T, newStore func() checkpoint.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("SaveAndLoadRoundTrip", func(t *testing.T) {
		s := newStore()
		m := validManifest("ws-1", 1)
		if err := s.Save(ctx, m); err != nil {
			t.Fatal(err)
		}
		got, err := s.Load(ctx, "ws-1")
		if err != nil {
			t.Fatal(err)
		}
		assertManifestEqual(t, m, got)
	})

	t.Run("LoadReturnsLatestEpoch", func(t *testing.T) {
		s := newStore()
		m1 := validManifest("ws-1", 1)
		m2 := validManifest("ws-1", 2)
		m2.SnapshotID = "snap-002"
		if err := s.Save(ctx, m1); err != nil {
			t.Fatal(err)
		}
		if err := s.Save(ctx, m2); err != nil {
			t.Fatal(err)
		}
		got, err := s.Load(ctx, "ws-1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Epoch != 2 {
			t.Errorf("Load returned epoch %d, want 2", got.Epoch)
		}
		if got.SnapshotID != "snap-002" {
			t.Errorf("Load returned snapshot %q, want %q", got.SnapshotID, "snap-002")
		}
	})

	t.Run("ListOrderByEpoch", func(t *testing.T) {
		s := newStore()
		for _, epoch := range []int64{3, 1, 2} {
			if err := s.Save(ctx, validManifest("ws-1", epoch)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.List(ctx, "ws-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("List returned %d manifests, want 3", len(got))
		}
		for i, want := range []int64{1, 2, 3} {
			if got[i].Epoch != want {
				t.Errorf("got[%d].Epoch = %d, want %d", i, got[i].Epoch, want)
			}
		}
	})

	t.Run("LoadNotFound", func(t *testing.T) {
		s := newStore()
		_, err := s.Load(ctx, "nonexistent")
		if err == nil {
			t.Fatal("Load on missing workspace should return error")
		}
		if !errors.Is(err, checkpoint.ErrNotFound) {
			t.Errorf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("ListEmptyWorkspace", func(t *testing.T) {
		s := newStore()
		got, err := s.List(ctx, "nonexistent")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("List on missing workspace returned %d manifests, want 0", len(got))
		}
	})

	t.Run("OverwriteSameEpoch", func(t *testing.T) {
		s := newStore()
		m := validManifest("ws-1", 1)
		m.SnapshotID = "snap-original"
		if err := s.Save(ctx, m); err != nil {
			t.Fatal(err)
		}
		m.SnapshotID = "snap-updated"
		if err := s.Save(ctx, m); err != nil {
			t.Fatal(err)
		}
		got, err := s.Load(ctx, "ws-1")
		if err != nil {
			t.Fatal(err)
		}
		if got.SnapshotID != "snap-updated" {
			t.Errorf("SnapshotID = %q, want %q", got.SnapshotID, "snap-updated")
		}
		list, err := s.List(ctx, "ws-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 {
			t.Errorf("List after overwrite returned %d manifests, want 1", len(list))
		}
	})

	t.Run("ValidateOnSave", func(t *testing.T) {
		s := newStore()
		bad := checkpoint.RecoveryManifest{} // all zero values
		if err := s.Save(ctx, bad); err == nil {
			t.Error("Save with invalid manifest should return error")
		}
	})

	t.Run("IsolatedWorkspaces", func(t *testing.T) {
		s := newStore()
		if err := s.Save(ctx, validManifest("ws-a", 1)); err != nil {
			t.Fatal(err)
		}
		if err := s.Save(ctx, validManifest("ws-b", 1)); err != nil {
			t.Fatal(err)
		}
		got, err := s.List(ctx, "ws-a")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Errorf("List(ws-a) returned %d manifests, want 1", len(got))
		}
		if got[0].WorkspaceID != "ws-a" {
			t.Errorf("got workspace %q, want %q", got[0].WorkspaceID, "ws-a")
		}
	})
}

func validManifest(wsID string, epoch int64) checkpoint.RecoveryManifest {
	return checkpoint.RecoveryManifest{
		ManifestVersion:     1,
		WorkspaceID:         wsID,
		Epoch:               epoch,
		LeaseID:             "lease-1",
		SnapshotID:          "snap-001",
		SnapshotChecksum:    "sha256:abc",
		TranscriptCursor:    "cursor-0",
		EventCursor:         0,
		BeadSequence:        0,
		ControlPlaneVersion: "1.0.0",
		CreatedAt:           time.Now().Truncate(time.Second),
	}
}

func assertManifestEqual(t *testing.T, want, got checkpoint.RecoveryManifest) {
	t.Helper()
	if got.ManifestVersion != want.ManifestVersion {
		t.Errorf("ManifestVersion = %d, want %d", got.ManifestVersion, want.ManifestVersion)
	}
	if got.WorkspaceID != want.WorkspaceID {
		t.Errorf("WorkspaceID = %q, want %q", got.WorkspaceID, want.WorkspaceID)
	}
	if got.Epoch != want.Epoch {
		t.Errorf("Epoch = %d, want %d", got.Epoch, want.Epoch)
	}
	if got.SnapshotID != want.SnapshotID {
		t.Errorf("SnapshotID = %q, want %q", got.SnapshotID, want.SnapshotID)
	}
	if got.SnapshotChecksum != want.SnapshotChecksum {
		t.Errorf("SnapshotChecksum = %q, want %q", got.SnapshotChecksum, want.SnapshotChecksum)
	}
	if got.LeaseID != want.LeaseID {
		t.Errorf("LeaseID = %q, want %q", got.LeaseID, want.LeaseID)
	}
}
