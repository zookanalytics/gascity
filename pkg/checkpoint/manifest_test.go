package checkpoint

import (
	"testing"
	"time"
)

func validManifest() RecoveryManifest {
	return RecoveryManifest{
		ManifestVersion:     1,
		WorkspaceID:         "ws-123",
		Epoch:               1,
		LeaseID:             "lease-abc",
		SnapshotID:          "snap-001",
		SnapshotChecksum:    "sha256:abc123",
		TranscriptCursor:    "cursor-1",
		EventCursor:         42,
		BeadSequence:        10,
		ControlPlaneVersion: "1.0.0",
		CreatedAt:           time.Now(),
	}
}

func TestManifestValidateSuccess(t *testing.T) {
	m := validManifest()
	if err := m.Validate(); err != nil {
		t.Errorf("valid manifest should not error: %v", err)
	}
}

func TestManifestValidateRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*RecoveryManifest)
	}{
		{"manifest_version", func(m *RecoveryManifest) { m.ManifestVersion = 0 }},
		{"workspace_id", func(m *RecoveryManifest) { m.WorkspaceID = "" }},
		{"epoch", func(m *RecoveryManifest) { m.Epoch = 0 }},
		{"snapshot_id", func(m *RecoveryManifest) { m.SnapshotID = "" }},
		{"created_at", func(m *RecoveryManifest) { m.CreatedAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mod(&m)
			if err := m.Validate(); err == nil {
				t.Errorf("invalid %s should cause validation error", tc.name)
			}
		})
	}
}
