package checkpoint

import (
	"errors"
	"time"
)

// RecoveryManifest captures the state needed to resume a workspace.
type RecoveryManifest struct {
	ManifestVersion     int       `json:"manifest_version"`
	WorkspaceID         string    `json:"workspace_id"`
	Epoch               int64     `json:"epoch"`
	LeaseID             string    `json:"lease_id"`
	SnapshotID          string    `json:"snapshot_id"`
	SnapshotChecksum    string    `json:"snapshot_checksum"`
	TranscriptCursor    string    `json:"transcript_cursor"`
	EventCursor         uint64    `json:"event_cursor"`
	BeadSequence        int64     `json:"bead_sequence"`
	ControlPlaneVersion string    `json:"control_plane_version"`
	CreatedAt           time.Time `json:"created_at"`
}

// Validate checks that required fields are present.
func (m *RecoveryManifest) Validate() error {
	if m.ManifestVersion < 1 {
		return errors.New("recovery manifest: manifest_version must be >= 1")
	}
	if m.WorkspaceID == "" {
		return errors.New("recovery manifest: missing workspace_id")
	}
	if m.Epoch < 1 {
		return errors.New("recovery manifest: epoch must be >= 1")
	}
	if m.SnapshotID == "" {
		return errors.New("recovery manifest: missing snapshot_id")
	}
	if m.CreatedAt.IsZero() {
		return errors.New("recovery manifest: missing created_at")
	}
	return nil
}
