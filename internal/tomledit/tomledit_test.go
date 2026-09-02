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

func TestSpliceRefusesAnEditThatRemovesAStanza(t *testing.T) {
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
	if _, ok := Splice(original, baseline, desired); ok {
		t.Fatal("Splice accepted an edit that removes a stanza; removals move every stanza after them")
	}
}

func TestSpliceRefusesAnEditThatAddsAStanzaAboveDependentSubTables(t *testing.T) {
	// The last [[rigs]] element owns the [rigs.imports] block beneath it.
	// A third rig spliced in after the last [[rigs]] header would land
	// between them and take that sub-table for itself.
	original := `[[rigs]]
name = "a"

[[rigs]]
name = "b"
[rigs.imports]
source = "rigs/b"
`
	baseline := `[[rigs]]
name = "a"

[[rigs]]
name = "b"
[rigs.imports]
source = "rigs/b"
`
	desired := `[[rigs]]
name = "a"

[[rigs]]
name = "b"
[rigs.imports]
source = "rigs/b"

[[rigs]]
name = "c"
`
	if _, ok := Splice(original, baseline, desired); ok {
		t.Fatal("Splice accepted an added stanza; inserting one can re-parent the sub-tables that follow it")
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
	for _, want := range []string{"# why y is 2, which the edit does not touch", "x = 99"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "# why x is 1") {
		t.Fatalf("a comment stating why x was 1 survived onto its new value:\n%s", got)
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

func TestSpliceHandlesDocumentsWithNoTables(t *testing.T) {
	// Split promises a block for the region ahead of the first header, and
	// Splice reads the last block's trail unconditionally. A source with
	// nothing in it must still produce that block, or the read runs off the
	// front of the slice.
	for _, source := range []string{"", "\n", "   \n"} {
		if len(Split(source).Blocks) == 0 {
			t.Fatalf("Split(%q) returned no blocks", source)
		}
	}
	got, ok := Splice("", "", "")
	if !ok {
		t.Fatal("Splice refused a document with nothing in it")
	}
	if got != "" {
		t.Fatalf("Splice of empty documents = %q, want empty", got)
	}
}

func TestSpliceKeepsTheCommentsOfRootLevelKeysAnEditDidNotTouch(t *testing.T) {
	// Root assignments sit ahead of the first table header, in the one block
	// that has no header line of its own. Their comments attach to them the
	// same way the comments inside a table attach to its keys, and survive an
	// edit to a neighboring key the same way.
	original := `# why the city is named this
name = "loomington"
# fragments merge in order; the last to state a key wins
include = ["fragments/base.toml"]

[workspace]
provider = "claude"
`
	baseline := `name = "loomington"
include = ["fragments/base.toml"]

[workspace]
provider = "claude"
`
	desired := `name = "loomington"
include = ["fragments/base.toml", "fragments/spend.toml"]

[workspace]
provider = "claude"
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	for _, want := range []string{
		"# why the city is named this",
		`include = ["fragments/base.toml", "fragments/spend.toml"]`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "# fragments merge in order; the last to state a key wins") {
		t.Fatalf("a comment describing the old include list survived onto the new one:\n%s", got)
	}
	if strings.Count(got, `name = "loomington"`) != 1 {
		t.Fatalf("root assignment was duplicated or lost:\n%s", got)
	}
}

func TestSpliceKeepsCommentsInADocumentWithNoTables(t *testing.T) {
	// With no header anywhere, the whole document is the header-less block.
	original := `# why the city is named this
name = "loomington"
# fragments merge in order
include = ["fragments/base.toml"]
`
	baseline := `name = "loomington"
include = ["fragments/base.toml"]
`
	desired := `name = "loomington"
include = ["fragments/base.toml", "fragments/spend.toml"]
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "# why the city is named this") {
		t.Fatalf("comment above a key the edit did not touch was lost:\n%s", got)
	}
	if !strings.Contains(got, `"fragments/spend.toml"`) {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}

func TestSpliceKeepsInlineCommentsOnAStanzaTheEditDidNotTouch(t *testing.T) {
	// A comment written at the end of an assignment is a comment like any
	// other. What proves a stanza untouched is the values it states, so
	// commentary beside a value does not make the stanza look edited.
	original := `[a]
x = 1 # keep why x is set
y = 2
`
	baseline := `[a]
x = 1
y = 2
`
	got, ok := Splice(original, baseline, baseline)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if got != original {
		t.Fatalf("an edit that changes nothing rewrote the file:\ngot:\n%s\nwant:\n%s", got, original)
	}
}

func TestSpliceKeepsInlineCommentsOfKeysAnEditDidNotTouch(t *testing.T) {
	original := `[a]
x = 1 # keep why x is set
y = 2
`
	baseline := `[a]
x = 1
y = 2
`
	desired := `[a]
x = 1
y = 99
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "x = 1 # keep why x is set") {
		t.Fatalf("an edit elsewhere in the stanza dropped an inline comment:\n%s", got)
	}
	if !strings.Contains(got, "y = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}

func TestSpliceKeepsAKeyWrittenInAFormTheEncoderDoesNotEmit(t *testing.T) {
	// How a file writes a value is the author's, not the encoder's. What
	// decides whether a key's comments still hold is whether the edit
	// changed that key's value, which baseline and desired state between
	// them; original's own spacing has no bearing on it.
	original := `[a]
x=1 # keep why x is set
y = 2
`
	baseline := `[a]
x = 1
y = 2
`
	desired := `[a]
x = 1
y = 99
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "x=1 # keep why x is set") {
		t.Fatalf("an edit elsewhere in the stanza rewrote a key it did not change:\n%s", got)
	}
	if !strings.Contains(got, "y = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}

func TestSpliceKeepsAStanzaWrittenInAFormTheEncoderDoesNotEmit(t *testing.T) {
	// The stanza states what the encoder would, written differently. An
	// edit that changes nothing about it has nothing to re-state, so it
	// keeps the bytes the author wrote.
	original := `[a]
x=1 # keep why x is set
y  =  2
`
	baseline := `[a]
x = 1
y = 2
`
	got, ok := Splice(original, baseline, baseline)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if got != original {
		t.Fatalf("an edit that changes nothing rewrote the file:\ngot:\n%s\nwant:\n%s", got, original)
	}
}

func TestSpliceDropsAnInlineCommentOnAValueItChanged(t *testing.T) {
	original := `[a]
x = 1 # x is 1 because the pool is small
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
	if strings.Contains(got, "x is 1 because the pool is small") {
		t.Fatalf("a comment stating why x was 1 survived onto its new value:\n%s", got)
	}
	if !strings.Contains(got, "x = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}

func TestSpliceDropsACommentTheNewValueContradicts(t *testing.T) {
	original := `[[orders.overrides]]
name = "feedback-distiller"
# the distiller is off on purpose: the promotion PR is city-wide, so four
# of them would race each other.
enabled = false
`
	baseline := `[[orders.overrides]]
name = "feedback-distiller"
enabled = false
`
	desired := `[[orders.overrides]]
name = "feedback-distiller"
enabled = true
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if strings.Contains(got, "the distiller is off on purpose") {
		t.Fatalf("a comment the new value contradicts survived the edit:\n%s", got)
	}
	if !strings.Contains(got, "enabled = true") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
	if !strings.Contains(got, `name = "feedback-distiller"`) {
		t.Fatalf("a key the edit did not touch was lost:\n%s", got)
	}
}

func TestSpliceAppliesAnEditInsideAStringStatingAHash(t *testing.T) {
	// A "#" inside a string opens no comment, so what stands after it is
	// part of the value. Reading it as a comment would make an edited value
	// compare equal to the old one and the edit would be dropped.
	original := `[a]
note = "before # after"
x = 1
`
	baseline := `[a]
note = "before # after"
x = 1
`
	desired := `[a]
note = "before # changed"
x = 1
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, `note = "before # changed"`) {
		t.Fatalf("edit was not applied:\n%s", got)
	}
	if strings.Contains(got, `note = "before # after"`) {
		t.Fatalf("old value survived the edit:\n%s", got)
	}
}

func TestSpliceKeepsACommentWrittenOnTheHeaderOfAnEditedStanza(t *testing.T) {
	original := `[a] # the tier polecats read
x = 1
y = 2
`
	baseline := `[a]
x = 1
y = 2
`
	desired := `[a]
x = 1
y = 99
`
	got, ok := Splice(original, baseline, desired)
	if !ok {
		t.Fatal("Splice refused a document whose layout matches its baseline")
	}
	if !strings.Contains(got, "[a] # the tier polecats read") {
		t.Fatalf("an edit inside the stanza dropped the comment on its header:\n%s", got)
	}
	if !strings.Contains(got, "y = 99") {
		t.Fatalf("edit was not applied:\n%s", got)
	}
}
