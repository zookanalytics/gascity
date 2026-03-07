package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// LocalStore is a filesystem-backed checkpoint store. Each manifest is stored
// as <dir>/<workspaceID>/<epoch>.json with a "latest" symlink pointing to the
// most recent epoch file.
type LocalStore struct {
	dir string
}

// NewLocalStore creates a LocalStore rooted at dir.
func NewLocalStore(dir string) *LocalStore {
	return &LocalStore{dir: dir}
}

// Save persists a recovery manifest to disk. Validates before writing.
func (s *LocalStore) Save(_ context.Context, m RecoveryManifest) error {
	if err := m.Validate(); err != nil {
		return err
	}

	wsDir := filepath.Join(s.dir, m.WorkspaceID)
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		return fmt.Errorf("creating checkpoint dir: %w", err)
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	filename := fmt.Sprintf("%d.json", m.Epoch)
	filePath := filepath.Join(wsDir, filename)

	// Atomic write: temp file then rename.
	tmp := filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	if err := os.Rename(tmp, filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming checkpoint: %w", err)
	}

	// Update latest symlink.
	latestPath := filepath.Join(wsDir, "latest")
	_ = os.Remove(latestPath) // may not exist
	if err := os.Symlink(filename, latestPath); err != nil {
		// Non-fatal: the data is saved, symlink is convenience.
		return nil
	}

	return nil
}

// Load returns the latest recovery manifest for a workspace by following
// the "latest" symlink. Falls back to scanning epoch files if the symlink
// is missing.
func (s *LocalStore) Load(_ context.Context, workspaceID string) (RecoveryManifest, error) {
	wsDir := filepath.Join(s.dir, workspaceID)

	// Try the latest symlink first.
	latestPath := filepath.Join(wsDir, "latest")
	target, err := os.Readlink(latestPath)
	if err == nil {
		m, err := s.readManifest(filepath.Join(wsDir, target))
		if err == nil {
			return m, nil
		}
	}

	// Fallback: find the highest epoch file.
	manifests, err := s.listManifests(wsDir)
	if err != nil {
		return RecoveryManifest{}, err
	}
	if len(manifests) == 0 {
		return RecoveryManifest{}, ErrNotFound
	}
	return manifests[len(manifests)-1], nil
}

// List returns all recovery manifests for a workspace ordered by epoch.
func (s *LocalStore) List(_ context.Context, workspaceID string) ([]RecoveryManifest, error) {
	wsDir := filepath.Join(s.dir, workspaceID)
	return s.listManifests(wsDir)
}

func (s *LocalStore) readManifest(path string) (RecoveryManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RecoveryManifest{}, fmt.Errorf("reading manifest: %w", err)
	}
	var m RecoveryManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return RecoveryManifest{}, fmt.Errorf("parsing manifest: %w", err)
	}
	return m, nil
}

func (s *LocalStore) listManifests(wsDir string) ([]RecoveryManifest, error) {
	entries, err := os.ReadDir(wsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading checkpoint dir: %w", err)
	}

	var manifests []RecoveryManifest
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		epochStr := strings.TrimSuffix(name, ".json")
		if _, err := strconv.ParseInt(epochStr, 10, 64); err != nil {
			continue // skip non-epoch files
		}
		m, err := s.readManifest(filepath.Join(wsDir, name))
		if err != nil {
			continue // skip unreadable files
		}
		manifests = append(manifests, m)
	}

	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].Epoch < manifests[j].Epoch
	})
	return manifests, nil
}
