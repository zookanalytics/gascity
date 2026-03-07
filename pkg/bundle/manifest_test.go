package bundle

import (
	"strings"
	"testing"
	"time"
)

func validManifest() Manifest {
	return Manifest{
		Version:   SchemaVersion,
		BundleID:  "bundle-001",
		Checksum:  "sha256:abc123",
		CreatedAt: time.Now(),
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
		mod  func(*Manifest)
	}{
		{"version", func(m *Manifest) { m.Version = 0 }},
		{"bundle_id", func(m *Manifest) { m.BundleID = "" }},
		{"checksum", func(m *Manifest) { m.Checksum = "" }},
		{"created_at", func(m *Manifest) { m.CreatedAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mod(&m)
			if err := m.Validate(); err == nil {
				t.Errorf("missing %s should cause validation error", tc.name)
			}
		})
	}
}

func TestLoadManifest(t *testing.T) {
	json := `{
		"version": 1,
		"bundle_id": "bundle-001",
		"checksum": "sha256:abc123",
		"created_at": "2025-01-01T00:00:00Z"
	}`
	m, err := LoadManifest(strings.NewReader(json))
	if err != nil {
		t.Fatal(err)
	}
	if m.BundleID != "bundle-001" {
		t.Errorf("BundleID = %q, want %q", m.BundleID, "bundle-001")
	}
}

func TestLoadManifestInvalidJSON(t *testing.T) {
	_, err := LoadManifest(strings.NewReader("not json"))
	if err == nil {
		t.Error("invalid JSON should return error")
	}
}

func TestLoadManifestValidationFailure(t *testing.T) {
	json := `{"version": 0, "bundle_id": "", "checksum": "", "created_at": "0001-01-01T00:00:00Z"}`
	_, err := LoadManifest(strings.NewReader(json))
	if err == nil {
		t.Error("invalid manifest should return error")
	}
}

func TestManifestOptionalFields(t *testing.T) {
	m := validManifest()
	m.WorkspaceID = "ws-123"
	m.Environment = "production"
	m.CreatedBy = "user@example.com"
	m.Signature = "sig-abc"
	m.MinRuntimeVersion = "1.0.0"
	m.MaxRuntimeVersion = "2.0.0"
	if err := m.Validate(); err != nil {
		t.Errorf("optional fields should not cause error: %v", err)
	}
}
