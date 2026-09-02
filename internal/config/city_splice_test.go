package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

// commentedCityFixture builds a city.toml in the shape the bug was measured
// against: operator commentary explaining decisions, and 22
// [[orders.overrides]] stanzas recording them.
func commentedCityFixture() string {
	var b strings.Builder
	b.WriteString(`[workspace]
provider = "claude"

[providers]
[providers.claude]
base = "builtin:claude"
# claude-watch: the monitoring tier for patrol agents. The model override
# MUST live on this separate provider, not on [providers.claude], or
# polecats silently drop a tier.
[providers.claude-watch]
base = "builtin:claude"

[[rigs]]
name = "gc-toolkit"
prefix = "tk"

[[rigs]]
name = "gascity"
prefix = "ga"

# ---------------------------------------------------------------------------
# Tooling-spend controls (operator decision 2026-08-15).
#
# These slow the background orders that manufacture work. All are reversible
# by deleting the block; city.toml hot-reloads, so no restart is needed.
# TO RELEASE: delete these four blocks.
# ---------------------------------------------------------------------------

[orders]
`)
	rigs := []string{"gc-toolkit", "gascity", "signal-loom", "shutupandlisten"}
	for _, order := range []string{"doc-keeper-drift-audit", "doc-keeper-memory-audit"} {
		fmt.Fprintf(&b, "# %s is retired; the override stays so a stale pack cannot revive it.\n", order)
		for _, rig := range rigs {
			fmt.Fprintf(&b, "[[orders.overrides]]\nname = %q\nrig = %q\nenabled = false\n\n", order, rig)
		}
		// The city-level twin, which carries no rig key.
		fmt.Fprintf(&b, "[[orders.overrides]]\nname = %q\nenabled = false\n\n", order)
	}
	b.WriteString(`# liveness-sweep: 6h -> 24h. The window lives in the precheck script, not
# in an interval key: this order is trigger = "condition", which ignores
# interval. Do not "simplify" it to a cooldown trigger.
`)
	for _, rig := range rigs {
		fmt.Fprintf(&b, "[[orders.overrides]]\nname = \"liveness-sweep\"\nrig = %q\ncheck = \"LIVENESS_SWEEP_INTERVAL=86400 precheck.sh\"\n\n", rig)
	}
	b.WriteString(`# feedback-distiller runs on one rig only: the promotion PR is city-wide,
# so four of them would race each other.
`)
	for _, rig := range rigs {
		fmt.Fprintf(&b, "[[orders.overrides]]\nname = \"feedback-distiller\"\nrig = %q\nenabled = false\n\n", rig)
	}
	b.WriteString(`# feedback-miner: 48h rather than 72h. At 72h the observation backlog
# outran the distiller's cap and the loop stopped converging.
`)
	for _, rig := range rigs {
		fmt.Fprintf(&b, "[[orders.overrides]]\nname = \"feedback-miner\"\nrig = %q\ninterval = \"48h\"\n\n", rig)
	}
	b.WriteString(`[agent_defaults]
default_sling_formula = "mol-polecat-work"
# wake_mode is pinned to resume: a fresh wake loses the hook claim and the
# pool re-offers work an agent is already holding.
wake_mode = "resume"
`)
	return b.String()
}

func TestSpliceCityForWritePreservesEveryUntouchedRegion(t *testing.T) {
	original := commentedCityFixture()
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse fixture: %v", err)
	}
	if got := len(cfg.Orders.Overrides); got != 22 {
		t.Fatalf("fixture parsed %d order overrides, want 22", got)
	}

	// A single unrelated edit, the shape `gc agent suspend` makes.
	cfg.AgentDefaults.DefaultSlingFormula = "mol-review"

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}

	if !strings.Contains(string(got), `default_sling_formula = "mol-review"`) {
		t.Fatalf("the edit was not applied:\n%s", got)
	}

	// Every region the edit did not name must come back byte-identical.
	untouched := []string{
		"# claude-watch: the monitoring tier for patrol agents. The model override\n" +
			"# MUST live on this separate provider, not on [providers.claude], or\n" +
			"# polecats silently drop a tier.\n" +
			"[providers.claude-watch]\nbase = \"builtin:claude\"\n",
		"# Tooling-spend controls (operator decision 2026-08-15).",
		"# TO RELEASE: delete these four blocks.",
		"# feedback-miner: 48h rather than 72h. At 72h the observation backlog\n" +
			"# outran the distiller's cap and the loop stopped converging.\n",
		"[[orders.overrides]]\nname = \"feedback-distiller\"\nrig = \"gc-toolkit\"\nenabled = false\n",
		"[[rigs]]\nname = \"gc-toolkit\"\nprefix = \"tk\"\n",
	}
	for _, region := range untouched {
		if !strings.Contains(string(got), region) {
			t.Fatalf("untouched region was not preserved byte-for-byte:\n--- want ---\n%s\n--- got ---\n%s", region, got)
		}
	}

	// The comment above the edited stanza is the operator's, not the edit's.
	if !strings.Contains(string(got), "# wake_mode is pinned to resume") {
		t.Fatalf("edited stanza lost the operator commentary around it:\n%s", got)
	}

	// The rewrite must still be the config it claims to be.
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("spliced output does not parse: %v\n%s", err, got)
	}
	if len(after.Orders.Overrides) != 22 {
		t.Fatalf("spliced output carries %d order overrides, want 22 — stanzas were dropped:\n%s",
			len(after.Orders.Overrides), got)
	}
	if after.AgentDefaults.DefaultSlingFormula != "mol-review" {
		t.Fatalf("spliced output lost the edit: %q", after.AgentDefaults.DefaultSlingFormula)
	}
}

func TestSpliceCityForWriteIsIdempotentAcrossRepeatedEdits(t *testing.T) {
	// Config-edit commands run over and over against the same file. Once
	// its stanzas are in the form the encoder emits, a write that changes
	// nothing must leave the bytes exactly as they are, or every command
	// churns the file and the comments erode a pass at a time.
	once := spliceUnchanged(t, []byte(commentedCityFixture()))
	twice := spliceUnchanged(t, once)
	if string(twice) != string(once) {
		t.Fatalf("a second write with no change reformatted the file:\n%s", twice)
	}

	// The pass that canonicalizes the stanzas must still carry the
	// commentary and every stanza through.
	for _, want := range []string{
		"# TO RELEASE: delete these four blocks.",
		"# feedback-distiller runs on one rig only: the promotion PR is city-wide,",
		"# claude-watch: the monitoring tier for patrol agents. The model override",
	} {
		if !strings.Contains(string(once), want) {
			t.Fatalf("missing %q in:\n%s", want, once)
		}
	}
	after, err := Parse(once)
	if err != nil {
		t.Fatalf("output does not parse: %v", err)
	}
	if len(after.Orders.Overrides) != 22 {
		t.Fatalf("order overrides = %d, want 22", len(after.Orders.Overrides))
	}
}

// spliceUnchanged writes city back through the edit writer without editing
// it, returning the bytes that land on disk.
func spliceUnchanged(t *testing.T, city []byte) []byte {
	t.Helper()
	cfg, err := Parse(city)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got, err := SpliceCityForWrite(city, cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	return got
}

func TestSpliceCityForWriteFallsBackWhenTheEditRemovesAStanza(t *testing.T) {
	// Removing a stanza moves every stanza below it, and a [[rigs]] element
	// dropped from above its [rigs.imports] sub-tables would re-parent them
	// onto the element before it. Rather than guess where the survivors
	// belong, the splice steps aside and the file is rewritten plainly: the
	// edit lands, and the commentary goes the way it did before the splice
	// existed.
	original := commentedCityFixture()
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse fixture: %v", err)
	}
	cfg.Orders.Overrides = cfg.Orders.Overrides[1:]

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("spliced output does not parse: %v\n%s", err, got)
	}
	if len(after.Orders.Overrides) != 21 {
		t.Fatalf("order overrides = %d, want 21:\n%s", len(after.Orders.Overrides), got)
	}
	plain, err := cfg.MarshalForWrite()
	if err != nil {
		t.Fatalf("MarshalForWrite: %v", err)
	}
	if string(got) != string(plain) {
		t.Fatalf("a removal must fall back to the plain rewrite:\n%s", got)
	}
}

func TestSpliceCityForWriteFallsBackWhenTheFileUsesALayoutTheEncoderCannotEmit(t *testing.T) {
	// A dotted key states the same table the encoder writes as a header, so
	// no block of the canonical rendering corresponds to it.
	original := "workspace.provider = \"claude\"\n"
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cfg.Workspace.Provider = "codex"

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("fallback output does not parse: %v\n%s", err, got)
	}
	if after.Workspace.Provider != "codex" {
		t.Fatalf("fallback lost the edit: %q\n%s", after.Workspace.Provider, got)
	}
	if strings.Count(string(got), "provider") != 1 {
		t.Fatalf("fallback must emit the table once, got:\n%s", got)
	}
}

func TestSpliceCityForWriteFallsBackWhenAParentHeaderIsImplicit(t *testing.T) {
	// [[orders.overrides]] states orders.overrides without naming [orders],
	// which the encoder always writes. The layouts no longer correspond, so
	// the splice steps aside rather than guess where the header belongs.
	original := `# why this order is off
[[orders.overrides]]
name = "feedback-distiller"
enabled = false
`
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	enabled := true
	cfg.Orders.Overrides[0].Enabled = &enabled

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("fallback output does not parse: %v\n%s", err, got)
	}
	if len(after.Orders.Overrides) != 1 {
		t.Fatalf("fallback emitted %d overrides, want 1 — a stanza was duplicated or lost:\n%s",
			len(after.Orders.Overrides), got)
	}
	if after.Orders.Overrides[0].Enabled == nil || !*after.Orders.Overrides[0].Enabled {
		t.Fatalf("fallback lost the edit:\n%s", got)
	}
}

func TestWriteCityAndRigSiteBindingsForEditKeepsComments(t *testing.T) {
	dir := t.TempDir()
	cityPath := filepath.Join(dir, "city.toml")
	original := commentedCityFixture()
	if err := os.WriteFile(cityPath, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(fsys.OSFS{}, cityPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg.AgentDefaults.DefaultSlingFormula = "mol-review"

	if err := WriteCityAndRigSiteBindingsForEdit(fsys.OSFS{}, cityPath, cfg); err != nil {
		t.Fatalf("WriteCityAndRigSiteBindingsForEdit: %v", err)
	}
	written, err := os.ReadFile(cityPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(written), "# TO RELEASE: delete these four blocks.") {
		t.Fatalf("the edit writer stripped operator commentary:\n%s", written)
	}
	after, err := Parse(written)
	if err != nil {
		t.Fatalf("written city.toml does not parse: %v", err)
	}
	if len(after.Orders.Overrides) != 22 {
		t.Fatalf("the edit writer dropped order overrides: %d, want 22", len(after.Orders.Overrides))
	}
	if after.AgentDefaults.DefaultSlingFormula != "mol-review" {
		t.Fatalf("the edit was not written: %q", after.AgentDefaults.DefaultSlingFormula)
	}
}

func TestSpliceCityForWriteMeansWhatThePlainRewriteMeans(t *testing.T) {
	// The splice exists to change bytes, so the property that keeps it safe
	// is that it never changes meaning: whatever it writes must parse to the
	// same config a full rewrite would have produced. city.toml hot-reloads,
	// so a divergence here is a live behavior change nobody asked for --
	// the failure this whole change is about.
	original := commentedCityFixture()
	fresh := "fresh"
	on := true
	for _, tc := range []struct {
		name string
		edit func(*City)
	}{
		{"no edit at all", func(*City) {}},
		{"a value already stated", func(c *City) { c.AgentDefaults.WakeMode = fresh }},
		{"a key the file omits", func(c *City) { c.Orders.Overrides[0].Enabled = &on }},
		{"a stanza removed", func(c *City) { c.Orders.Overrides = c.Orders.Overrides[1:] }},
		{"a stanza added", func(c *City) {
			c.Orders.Overrides = append(c.Orders.Overrides, c.Orders.Overrides[0])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(original))
			if err != nil {
				t.Fatalf("Parse fixture: %v", err)
			}
			tc.edit(cfg)

			got, err := SpliceCityForWrite([]byte(original), cfg)
			if err != nil {
				t.Fatalf("SpliceCityForWrite: %v", err)
			}
			spliced, err := Parse(got)
			if err != nil {
				t.Fatalf("spliced output does not parse: %v\n%s", err, got)
			}

			plainBytes, err := cfg.MarshalForWrite()
			if err != nil {
				t.Fatalf("MarshalForWrite: %v", err)
			}
			plain, err := Parse(plainBytes)
			if err != nil {
				t.Fatalf("plain rewrite does not parse: %v", err)
			}
			if !reflect.DeepEqual(spliced, plain) {
				t.Fatalf("spliced config differs from the plain rewrite:\n%s", got)
			}
		})
	}
}

func TestSpliceCityForWriteDropsTheCommentAboveARootKeyItChanged(t *testing.T) {
	// include is stated ahead of the first table header, in the region of the
	// file that has no header of its own, and `gc import` edits it through
	// this writer. A comment above a key describes the value written under
	// it, so an edit that changes that value leaves the comment behind; every
	// other comment in the file is untouched by it.
	original := `# fragments merge in order; the last one to state a key wins.
# TO REVERT the spend controls: drop the second entry.
include = ["fragments/base.toml", "fragments/spend.toml"]

[workspace]
provider = "claude"

# claude-watch is the monitoring tier for patrol agents.
[providers]
[providers.claude-watch]
base = "builtin:claude"
`
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cfg.Include = append(cfg.Include, "fragments/audit.toml")

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	if !strings.Contains(string(got), "# claude-watch is the monitoring tier for patrol agents.") {
		t.Fatalf("an edit to include dropped a comment written elsewhere in the file:\n%s", got)
	}
	for _, gone := range []string{
		"# fragments merge in order; the last one to state a key wins.",
		"# TO REVERT the spend controls: drop the second entry.",
	} {
		if strings.Contains(string(got), gone) {
			t.Fatalf("a comment describing the old include list survived onto the new one: %q\n%s", gone, got)
		}
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("spliced output does not parse: %v\n%s", err, got)
	}
	if len(after.Include) != 3 {
		t.Fatalf("include = %v, want 3 entries:\n%s", after.Include, got)
	}
}

func TestSpliceCityForWriteKeepsRootCommentsWhenTheEditIsElsewhere(t *testing.T) {
	// A stanza is re-encoded whenever its bytes are not already the form the
	// encoder emits, and an include list written across several lines is one
	// such form. That re-encoding is reached by any edit to the file, so the
	// comments above the first root key have to survive an edit that never
	// names it.
	original := `# fragments merge in order; the last one to state a key wins.
include = [
  "fragments/base.toml",
]

[workspace]
provider = "claude"
`
	cfg, err := Parse([]byte(original))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cfg.Workspace.Provider = "codex"

	got, err := SpliceCityForWrite([]byte(original), cfg)
	if err != nil {
		t.Fatalf("SpliceCityForWrite: %v", err)
	}
	if !strings.Contains(string(got), "# fragments merge in order; the last one to state a key wins.") {
		t.Fatalf("an edit elsewhere in the file dropped the commentary above include:\n%s", got)
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("spliced output does not parse: %v\n%s", err, got)
	}
	if len(after.Include) != 1 || after.Include[0] != "fragments/base.toml" {
		t.Fatalf("include = %v, want [fragments/base.toml]:\n%s", after.Include, got)
	}
	if after.Workspace.Provider != "codex" {
		t.Fatalf("the edit was not applied: %q\n%s", after.Workspace.Provider, got)
	}
}

func TestSpliceCityForWriteKeepsInlineComments(t *testing.T) {
	// A reason written at the end of an assignment is as much a record of an
	// operator decision as one written above it, and city.toml is edited by
	// commands that name neither.
	original := `[workspace]
name = "loomington"
prefix = "lx" # bead ids read as lx-*; changing it orphans every open bead
provider = "claude"

# claude-watch is the monitoring tier for patrol agents.
[providers]
[providers.claude-watch]
base = "builtin:claude" # the watch tier tracks the base image on purpose
ready_delay_ms = 0
`
	inline := []string{
		`prefix = "lx" # bead ids read as lx-*; changing it orphans every open bead`,
		`base = "builtin:claude" # the watch tier tracks the base image on purpose`,
	}

	t.Run("an edit that changes nothing", func(t *testing.T) {
		cfg, err := Parse([]byte(original))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		got, err := SpliceCityForWrite([]byte(original), cfg)
		if err != nil {
			t.Fatalf("SpliceCityForWrite: %v", err)
		}
		if string(got) != original {
			t.Fatalf("an edit that changes nothing rewrote the file:\ngot:\n%s\nwant:\n%s", got, original)
		}
	})

	t.Run("an edit elsewhere", func(t *testing.T) {
		cfg, err := Parse([]byte(original))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		cfg.Workspace.Provider = "codex"

		got, err := SpliceCityForWrite([]byte(original), cfg)
		if err != nil {
			t.Fatalf("SpliceCityForWrite: %v", err)
		}
		for _, want := range inline {
			if !strings.Contains(string(got), want) {
				t.Fatalf("an edit elsewhere in the file dropped %q:\n%s", want, got)
			}
		}
		after, err := Parse(got)
		if err != nil {
			t.Fatalf("spliced output does not parse: %v\n%s", err, got)
		}
		if after.Workspace.Provider != "codex" {
			t.Fatalf("the edit was not applied: %q\n%s", after.Workspace.Provider, got)
		}
		if after.Workspace.Prefix != "lx" {
			t.Fatalf("prefix = %q, want lx:\n%s", after.Workspace.Prefix, got)
		}
	})
}
