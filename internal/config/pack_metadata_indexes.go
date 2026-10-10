package config

import (
	"fmt"
	"path/filepath"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// PackBeadsConfig is the [beads] table of a pack.toml.
type PackBeadsConfig struct {
	// MetadataIndexes lists the bead metadata keys this pack's queries look
	// up with closed beads included. Gas City keeps a functional index on
	// each key, alongside the keys its own queries use, in the issues and
	// wisps tables of every bead store it manages, on a Dolt server that
	// passes its index self-test, so such a lookup reads the index instead
	// of every row's metadata. A key must start with a letter or underscore
	// and contain only letters, digits, underscores, dots and slashes.
	MetadataIndexes []string `toml:"metadata_indexes,omitempty"`
}

// DiscoveredMetadataIndex is one metadata key a pack declares in
// [beads].metadata_indexes, with the declaring pack for diagnostics.
type DiscoveredMetadataIndex struct {
	// Key is the bead metadata key.
	Key string
	// PackName and PackDir identify the declaring pack.
	PackName string
	PackDir  string
}

// packLocalMetadataIndexes validates a pack's own [beads].metadata_indexes.
// An invalid key is a load error: no bead query can filter on it, and the
// key is spliced into the index expression.
func packLocalMetadataIndexes(tc *PackConfig, packDir string) ([]DiscoveredMetadataIndex, error) {
	var out []DiscoveredMetadataIndex
	seen := map[string]bool{}
	for _, key := range tc.Beads.MetadataIndexes {
		if err := beadmeta.ValidateKey(key); err != nil {
			return nil, fmt.Errorf("pack %q [beads].metadata_indexes: %w", tc.Pack.Name, err)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, DiscoveredMetadataIndex{Key: key, PackName: tc.Pack.Name, PackDir: packDir})
	}
	return out, nil
}

// mergeCityMetadataIndexes adds pack-declared metadata keys to the city.
// Metadata indexes are city-wide, so keys from rig-imported packs land here
// too. A key two packs declare keeps its first declaration.
func mergeCityMetadataIndexes(cfg *City, indexes []DiscoveredMetadataIndex) {
	for _, ix := range indexes {
		declared := false
		for _, existing := range cfg.PackMetadataIndexes {
			if existing.Key == ix.Key {
				declared = true
				break
			}
		}
		if !declared {
			cfg.PackMetadataIndexes = append(cfg.PackMetadataIndexes, ix)
		}
	}
}

// filterMetadataIndexesByPackDir keeps only the keys declared by the pack at
// packDir, for a non-transitive import.
func filterMetadataIndexesByPackDir(indexes []DiscoveredMetadataIndex, packDir string) []DiscoveredMetadataIndex {
	absPackDir, _ := filepath.Abs(packDir)
	var out []DiscoveredMetadataIndex
	for _, ix := range indexes {
		absDir, _ := filepath.Abs(ix.PackDir)
		if absDir == absPackDir {
			out = append(out, ix)
		}
	}
	return out
}

// cachedPackMetadataIndexes returns the metadata keys accumulated for a loaded
// pack directory: the pack's own plus its include and import closure.
func cachedPackMetadataIndexes(cache *packLoadCache, topoDir string) []DiscoveredMetadataIndex {
	return cachedPackField(cache, topoDir, func(r *packLoadResult) []DiscoveredMetadataIndex {
		return append([]DiscoveredMetadataIndex(nil), r.indexKeys...)
	})
}

// MetadataIndexKeys returns the metadata keys the city's packs declare in
// [beads].metadata_indexes, in declaration order.
func (c *City) MetadataIndexKeys() []string {
	keys := make([]string, 0, len(c.PackMetadataIndexes))
	for _, ix := range c.PackMetadataIndexes {
		keys = append(keys, ix.Key)
	}
	return keys
}
