package tomledit

import (
	"strings"
	"testing"
)

const commentedCity = `# city banner
# second banner line
name = "loomington"

[workspace]
provider = "claude"

# why claude-watch exists
# and how to revert it
[providers.claude-watch]
base = "builtin:claude"

[[orders.overrides]]
name = "liveness-sweep"
enabled = false

# the distiller is off on purpose
[[orders.overrides]]
name = "feedback-distiller"
enabled = false
# trailing note at end of file
`

func TestSplitRoundTripsSourceByteForByte(t *testing.T) {
	sources := map[string]string{
		"commented city": commentedCity,
		"empty":          "",
		"only newline":   "\n",
		"no final newline": `[a]
x = 1`,
		"crlf": "[a]\r\nx = 1\r\n",
		"comment only": `# nothing but commentary
`,
		"multiline string holding a header": `[a]
note = """
[not.a.header]
still inside
"""
x = 1
`,
		"multiline array holding a bracket line": `[a]
matrix = [
["a"],
["b"],
]
x = 1
`,
		"header with trailing comment": `[a] # inline note
x = 1
`,
	}
	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			if got := Split(src).String(); got != src {
				t.Fatalf("Split/String round trip lost bytes\n got: %q\nwant: %q", got, src)
			}
		})
	}
}

func TestSplitAttachesCommentsToTheHeaderBelowThem(t *testing.T) {
	doc := Split(commentedCity)
	block := findBlock(t, doc, "[providers.claude-watch]")
	lead := strings.Join(block.Lead, "\n")
	if !strings.Contains(lead, "why claude-watch exists") {
		t.Fatalf("comments above a header must be its Lead, got %q", lead)
	}
	if body := strings.Join(block.Body, "\n"); strings.Contains(body, "why claude-watch exists") {
		t.Fatalf("the preceding comment must not stay in the previous block's Body, got %q", body)
	}
}

func TestSplitKeepsEndOfFileCommentsInTheTrail(t *testing.T) {
	doc := Split(commentedCity)
	last := doc.Blocks[len(doc.Blocks)-1]
	if trail := strings.Join(last.Trail, "\n"); !strings.Contains(trail, "trailing note at end of file") {
		t.Fatalf("end-of-file comments belong to the last block's Trail, got %q", trail)
	}
}

func TestSplitDoesNotReadHeadersInsideMultilineStrings(t *testing.T) {
	doc := Split(`[a]
note = """
[b]
"""
x = 1
`)
	if len(doc.Blocks) != 2 {
		t.Fatalf("blocks = %d, want 2 (root + [a]); a header inside a \"\"\" block is content", len(doc.Blocks))
	}
}

func TestSplitDoesNotReadHeadersInsideMultilineArrays(t *testing.T) {
	doc := Split(`[a]
rows = [
["b"],
]
`)
	if len(doc.Blocks) != 2 {
		t.Fatalf("blocks = %d, want 2 (root + [a]); a bracket line inside an open array is content", len(doc.Blocks))
	}
}

func TestSpliceKeepsCommentsOfStanzasTheEditDidNotTouch(t *testing.T) {
	original := `# banner

# note about a
[a]
x = 1

# note about b
[b]
y = 2
`
	baseline := `[a]
x = 1

[b]
y = 2
`
	desired := `[a]
x = 99

[b]
y = 2
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "# note about b") {
		t.Fatalf("untouched stanza lost its comment:\n%s", got)
	}
	if !strings.Contains(got, "# note about a") {
		t.Fatalf("edited stanza lost the comment written above it:\n%s", got)
	}
	if !strings.Contains(got, "x = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
	if strings.Contains(got, "x = 1") {
		t.Fatalf("old value survived the edit:\n%s", got)
	}
	if !strings.Contains(got, "# banner") {
		t.Fatalf("document banner was lost:\n%s", got)
	}
}

func TestSpliceDropsAStanzaTheEditRemoved(t *testing.T) {
	original := `# note about a
[[item]]
name = "a"

# note about b
[[item]]
name = "b"
`
	baseline := `[[item]]
name = "a"

[[item]]
name = "b"
`
	desired := `[[item]]
name = "b"
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if strings.Contains(got, `name = "a"`) || strings.Contains(got, "# note about a") {
		t.Fatalf("removed stanza survived with its comment:\n%s", got)
	}
	if !strings.Contains(got, "# note about b") {
		t.Fatalf("kept stanza lost its comment:\n%s", got)
	}
}

func TestSpliceAddsANewStanzaBesideItsSiblings(t *testing.T) {
	original := `[[item]]
name = "a"

# tail table
[z]
k = 1
`
	baseline := `[[item]]
name = "a"

[z]
k = 1
`
	desired := `[[item]]
name = "a"

[[item]]
name = "b"

[z]
k = 1
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, `name = "b"`) {
		t.Fatalf("added stanza is missing:\n%s", got)
	}
	if strings.Index(got, `name = "b"`) > strings.Index(got, "# tail table") {
		t.Fatalf("added stanza must land beside its siblings, not after the rest of the file:\n%s", got)
	}
}

func TestSpliceRefusesALayoutItsBaselineDoesNotDescribe(t *testing.T) {
	// The file states the table inline, a form the encoder never emits, so
	// no block of the baseline corresponds to it.
	original := `item = [{name = "a"}]
`
	baseline := `[[item]]
name = "a"
`
	desired := `[[item]]
name = "b"
`
	if _, ok := Splice(original, baseline, desired); ok {
		t.Fatal("Splice accepted a layout it cannot account for; the caller must fall back to the plain rewrite")
	}
}

func findBlock(t *testing.T, doc Document, key string) Block {
	t.Helper()
	for _, block := range doc.Blocks {
		if block.Key() == key {
			return block
		}
	}
	t.Fatalf("no block keyed %s in %#v", key, doc.Blocks)
	return Block{}
}

func TestSpliceKeepsCommentsWrittenBetweenTheKeysOfAnEditedStanza(t *testing.T) {
	original := `[a]
# why x is 1
x = 1
# why y is 2, which the edit does not touch
y = 2
`
	baseline := `[a]
x = 1
y = 2
`
	desired := `[a]
x = 99
y = 2
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	for _, want := range []string{"# why x is 1", "# why y is 2, which the edit does not touch", "x = 99"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestSpliceKeepsMultiLineValuesOfAnEditedStanzaIntact(t *testing.T) {
	original := `[a]
# the rollout note
note = """
line one
line two
"""
x = 1
`
	baseline := `[a]
note = """
line one
line two
"""
x = 1
`
	desired := `[a]
note = """
line one
line two
"""
x = 99
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "# the rollout note") {
		t.Fatalf("comment above a multi-line value was lost:\n%s", got)
	}
	if strings.Count(got, "line one") != 1 {
		t.Fatalf("multi-line value was duplicated or lost:\n%s", got)
	}
	if !strings.Contains(got, "x = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}

func TestSpliceDoesNotRestoreAKeyTheCanonicalFormStrips(t *testing.T) {
	// baseline carries no path key: the encoder strips it on the way out.
	// Copying original's bytes through would put it back.
	original := `# the rig that runs the toolkit
[[rigs]]
name = "gc-toolkit"
path = "/tmp/gc-toolkit"
`
	baseline := `[[rigs]]
name = "gc-toolkit"
`
	desired := `[[rigs]]
name = "gc-toolkit"
prefix = "tk"
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if strings.Contains(got, "path =") {
		t.Fatalf("splice restored a key the canonical form strips:\n%s", got)
	}
	if !strings.Contains(got, "# the rig that runs the toolkit") {
		t.Fatalf("comment was lost:\n%s", got)
	}
	if !strings.Contains(got, `prefix = "tk"`) {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}
