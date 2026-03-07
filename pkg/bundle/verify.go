package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CanAccept reports whether a runtime can load a bundle with the given
// schema version. Accepts current (N) and previous (N-1) versions.
func CanAccept(schemaVersion int) bool {
	return schemaVersion == SchemaVersion || schemaVersion == SchemaVersion-1
}

// VerifyChecksum verifies that the contents of bundleDir match the expected
// checksum. The checksum format is "sha256:<hex>". Files are hashed in
// sorted order for determinism. The manifest file itself is excluded.
func VerifyChecksum(bundleDir string, expected string) error {
	parts := strings.SplitN(expected, ":", 2)
	if len(parts) != 2 || parts[0] != "sha256" {
		return fmt.Errorf("unsupported checksum format: %q", expected)
	}
	wantHex := parts[1]

	h := sha256.New()
	var files []string
	err := filepath.Walk(bundleDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(bundleDir, path)
		if err != nil {
			return err
		}
		// Skip the manifest file itself.
		if rel == "manifest.json" {
			return nil
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walking bundle dir: %w", err)
	}

	sort.Strings(files)
	for _, rel := range files {
		// Hash the relative path for determinism.
		if _, err := fmt.Fprintf(h, "%s\n", rel); err != nil {
			return fmt.Errorf("hashing path %s: %w", rel, err)
		}
		f, err := os.Open(filepath.Join(bundleDir, rel))
		if err != nil {
			return fmt.Errorf("opening %s: %w", rel, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return fmt.Errorf("reading %s: %w", rel, err)
		}
		_ = f.Close()
	}

	gotHex := hex.EncodeToString(h.Sum(nil))
	if gotHex != wantHex {
		return fmt.Errorf("checksum mismatch: got sha256:%s, want sha256:%s", gotHex, wantHex)
	}
	return nil
}
