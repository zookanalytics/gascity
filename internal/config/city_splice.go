package config

import (
	"bytes"
	"fmt"

	"github.com/gastownhall/gascity/internal/tomledit"
)

// SpliceCityForWrite renders cfg as the checked-in city.toml form while
// keeping the exact bytes of every stanza the edit did not change.
//
// city.toml records operator decisions and how to revert them, and it
// hot-reloads. Re-encoding the whole file would drop the commentary holding
// those reasons, and would change live behavior for any stanza the struct
// round trip does not carry back.
//
// original is the file's current content. It is compared against its own
// canonical rendering, so a stanza whose encoded text is still in the
// output is provably untouched and its bytes can be copied through. When
// original states its tables in a form the encoder does not emit, no such
// correspondence exists, and the plain rendering is returned instead. That
// is what the caller would have written anyway.
//
// Copying a stanza through does not preserve what the checked-in form
// deliberately drops. A stanza carrying a machine-local rig path is
// re-encoded without it, the same as a full rewrite would. A comment about a
// key whose value the edit changed is dropped too: it states why that key
// held its old value, and city.toml is read for what is true now.
func SpliceCityForWrite(original []byte, cfg *City) ([]byte, error) {
	desired, err := cfg.MarshalForWrite()
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(original)) == 0 {
		return desired, nil
	}
	current, err := Parse(original)
	if err != nil {
		return nil, fmt.Errorf("re-reading the current city.toml to preserve its comments: %w", err)
	}
	baseline, err := current.MarshalForWrite()
	if err != nil {
		return nil, err
	}
	spliced, ok := tomledit.Splice(string(original), string(baseline), string(desired))
	if !ok {
		return desired, nil
	}
	return []byte(spliced), nil
}
