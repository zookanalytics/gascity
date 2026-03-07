package checkpoint

import (
	"context"
	"errors"
)

// ErrNotFound is returned when no checkpoint exists for the requested workspace.
var ErrNotFound = errors.New("checkpoint: not found")

// Store persists and retrieves recovery manifests.
type Store interface {
	// Save persists a recovery manifest. If a manifest with the same
	// workspace ID and epoch already exists, it is overwritten.
	Save(ctx context.Context, m RecoveryManifest) error

	// Load returns the latest recovery manifest for a workspace.
	// Returns ErrNotFound if no checkpoint exists.
	Load(ctx context.Context, workspaceID string) (RecoveryManifest, error)

	// List returns all recovery manifests for a workspace, ordered by
	// epoch ascending. Returns an empty slice (not an error) if none exist.
	List(ctx context.Context, workspaceID string) ([]RecoveryManifest, error)
}
