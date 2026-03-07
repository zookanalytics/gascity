package bundle

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// SchemaVersion is the current bundle schema version.
const SchemaVersion = 1

// Manifest describes a compiled bundle.
type Manifest struct {
	Version           int       `json:"version"`
	BundleID          string    `json:"bundle_id"`
	WorkspaceID       string    `json:"workspace_id,omitempty"`
	Environment       string    `json:"environment,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	CreatedBy         string    `json:"created_by,omitempty"`
	Checksum          string    `json:"checksum"`
	Signature         string    `json:"signature,omitempty"`
	MinRuntimeVersion string    `json:"min_runtime_version,omitempty"`
	MaxRuntimeVersion string    `json:"max_runtime_version,omitempty"`
}

// Validate checks that required fields are present and the version is valid.
func (m *Manifest) Validate() error {
	if m.Version < 1 {
		return errors.New("bundle manifest: version must be >= 1")
	}
	if m.BundleID == "" {
		return errors.New("bundle manifest: missing bundle_id")
	}
	if m.Checksum == "" {
		return errors.New("bundle manifest: missing checksum")
	}
	if m.CreatedAt.IsZero() {
		return errors.New("bundle manifest: missing created_at")
	}
	return nil
}

// LoadManifest reads and parses a bundle manifest from r.
func LoadManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("decoding bundle manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
