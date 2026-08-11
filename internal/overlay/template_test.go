package overlay

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemplateTargetName(t *testing.T) {
	cases := []struct {
		name      string
		want      string
		wantOK    bool
		rationale string
	}{
		{name: "hooks.template.json", want: "hooks.json", wantOK: true, rationale: "the convention this seam exists for"},
		{name: "server.template.toml", want: "server.toml", wantOK: true, rationale: "same rule the MCP catalog loader already uses"},
		{name: "AGENTS.template.md", want: "AGENTS.md", wantOK: true, rationale: "not JSON-specific"},
		{name: ".hooks.template.json", want: ".hooks.json", wantOK: true, rationale: "a dotfile stem still renders"},
		{name: "hooks.json", wantOK: false, rationale: "no marker: plain byte copy"},
		{name: "settings.template", wantOK: false, rationale: "marker as the extension is not the convention"},
		{name: ".template.json", wantOK: false, rationale: "marker-only stem has no target name to stage to"},
		{name: "template.json", wantOK: false, rationale: "marker must be a separate segment"},
		{name: "hooks.template.d.json", wantOK: false, rationale: "marker must be the segment before the extension"},
		{name: "README", wantOK: false, rationale: "extensionless files are never templates"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := TemplateTargetName(tc.name)
			if ok != tc.wantOK {
				t.Fatalf("TemplateTargetName(%q) ok = %v, want %v (%s)", tc.name, ok, tc.wantOK, tc.rationale)
			}
			if ok && got != tc.want {
				t.Errorf("TemplateTargetName(%q) = %q, want %q", tc.name, got, tc.want)
			}
			if !ok && got != "" {
				t.Errorf("TemplateTargetName(%q) = %q, want empty when not templated", tc.name, got)
			}
		})
	}
}

// TestCopyDirForProviders_RendersTemplateOverlayFile is the acceptance case:
// a per-provider overlay file named <name>.template.<ext> is rendered through
// text/template with the supplied data map and lands at <name>.<ext>.
func TestCopyDirForProviders_RendersTemplateOverlayFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.template.json"),
		`{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"gc --city {{.CityRoot}} prime"}]}]}}`)

	if err := CopyDirForProviders(src, dst, []string{"codex"}, io.Discard,
		WithTemplateData(map[string]string{"CityRoot": "/city/root"})); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}

	staged := filepath.Join(dst, ".codex", "hooks.json")
	data, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("reading staged hooks.json: %v", err)
	}
	if !strings.Contains(string(data), "/city/root") {
		t.Errorf("staged hooks.json = %s, want the rendered CityRoot", data)
	}
	if strings.Contains(string(data), "{{") {
		t.Errorf("staged hooks.json still holds template tokens: %s", data)
	}
	if _, err := os.Stat(filepath.Join(dst, ".codex", "hooks.template.json")); !os.IsNotExist(err) {
		t.Errorf("the .template.json source was staged as a stray file; want only the rendered target")
	}
}

// TestCopyDirForProviders_RenderedFileSurvivesIdentityKeyedMerge pins that a
// rendered file merges as its TARGET path, not its source path. The merge keys
// hook entries on matcher, so a same-matcher entry replaces rather than
// appends — routing the render through the target name is what keeps a
// templated settings file behaving like its non-templated twin.
func TestCopyDirForProviders_RenderedFileSurvivesIdentityKeyedMerge(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.template.json"),
		`{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"gc --city {{.CityRoot}} prime"}]}],`+
			`"PreCompact":[{"matcher":"","hooks":[{"type":"command","command":"gc handoff"}]}]}}`)
	// A pre-existing on-disk file with the SAME matcher under SessionStart plus
	// an unrelated user category that must survive.
	writeOverlayFile(t, filepath.Join(dst, ".codex", "hooks.json"),
		`{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"stale"}]}],`+
			`"UserPromptSubmit":[{"matcher":"user","hooks":[{"type":"command","command":"mine"}]}]}}`)

	if err := CopyDirForProviders(src, dst, []string{"codex"}, io.Discard,
		WithTemplateData(map[string]string{"CityRoot": "/city/root"})); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}

	var doc struct {
		Hooks map[string][]struct {
			Matcher string `json:"matcher"`
			Hooks   []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	data, err := os.ReadFile(filepath.Join(dst, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("reading merged hooks.json: %v", err)
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("merged hooks.json is not valid JSON: %v\n%s", err, data)
	}
	if got := len(doc.Hooks["SessionStart"]); got != 1 {
		t.Fatalf("SessionStart entries = %d, want 1 (same matcher replaces, not appends)\n%s", got, data)
	}
	if got := doc.Hooks["SessionStart"][0].Hooks[0].Command; got != "gc --city /city/root prime" {
		t.Errorf("SessionStart command = %q, want the rendered managed command", got)
	}
	if got := len(doc.Hooks["UserPromptSubmit"]); got != 1 {
		t.Errorf("UserPromptSubmit entries = %d, want the pre-existing user category preserved\n%s", got, data)
	}
	if got := len(doc.Hooks["PreCompact"]); got != 1 {
		t.Errorf("PreCompact entries = %d, want the rendered new category added\n%s", got, data)
	}
}

// TestCopyDirForProviders_UnrenderableTokenFails pins missingkey=error: a token
// with no entry in the data map fails the copy loudly instead of staging a
// half-bound file.
//
// The two overlay halves report failure differently, and both are pinned here:
// the universal walk returns on the first error, while the per-provider walk is
// historically best-effort and reports per-file failures on stderr. Neither is
// silent, and runtime.StageProviderOverlayDir promotes a non-preservation
// stderr line to a hard error, so a render failure aborts staging either way.
func TestCopyDirForProviders_UnrenderableTokenFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rel        []string
		wantReturn bool
	}{
		{name: "universal", rel: []string{".codex", "hooks.template.json"}, wantReturn: true},
		{name: "per-provider", rel: []string{PerProviderDir, "codex", ".codex", "hooks.template.json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			dst := t.TempDir()
			writeOverlayFile(t, filepath.Join(append([]string{src}, tc.rel...)...), `{"city":"{{.NoSuchKey}}"}`)

			var stderr strings.Builder
			err := CopyDirForProviders(src, dst, []string{"codex"}, &stderr,
				WithTemplateData(map[string]string{"CityRoot": "/city/root"}))

			reported := stderr.String()
			if tc.wantReturn {
				if err == nil {
					t.Fatal("CopyDirForProviders succeeded; want an error naming the unrenderable template")
				}
				reported = err.Error()
			} else if err != nil {
				t.Fatalf("CopyDirForProviders returned %v; the per-provider walk reports on stderr", err)
			}
			if !strings.Contains(reported, "hooks.template.json") {
				t.Errorf("failure report = %q, want it to name the offending template", reported)
			}
			if !strings.Contains(reported, "NoSuchKey") {
				t.Errorf("failure report = %q, want it to name the unresolvable token", reported)
			}
			if IsPreserveExistingWarning(reported) {
				t.Errorf("failure report %q classifies as a nonfatal preservation warning", reported)
			}
			if _, statErr := os.Stat(filepath.Join(dst, ".codex", "hooks.json")); !os.IsNotExist(statErr) {
				t.Errorf("a half-bound hooks.json was staged despite the render failure")
			}
		})
	}
}

// TestCopyDirForProviders_NilTemplateDataStillFailsClosed pins that opting in
// with a nil map is still opting in: a caller that reaches the seam but has no
// values to bind cannot silently ship an unbound file. Staging callers pass
// WithTemplateData unconditionally, so a Config whose map was never populated
// surfaces here as a loud failure rather than a half-bound staged file.
func TestCopyDirForProviders_NilTemplateDataStillFailsClosed(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.template.json"),
		`{"city":"{{.CityRoot}}"}`)

	var stderr strings.Builder
	if err := CopyDirForProviders(src, dst, []string{"codex"}, &stderr, WithTemplateData(nil)); err != nil {
		t.Fatalf("CopyDirForProviders returned %v; the per-provider walk reports on stderr", err)
	}
	if !strings.Contains(stderr.String(), "CityRoot") {
		t.Errorf("stderr = %q, want a missingkey report naming CityRoot", stderr.String())
	}
	if _, err := os.Stat(filepath.Join(dst, ".codex", "hooks.json")); !os.IsNotExist(err) {
		t.Error("an unbound hooks.json was staged with no template data supplied")
	}
}

// promptTemplateBody is a pack's agent prompt template: it carries the
// .template.md name and a function only the prompt renderer registers.
// Parsing it as an overlay template fails at parse time, before any data map
// is consulted.
const promptTemplateBody = "# Coder\n\nYou are **{{ basename .AgentName }}**, a peer coder.\n"

// TestCopyDirWithSkip_LeavesTemplatesAloneWithoutOptIn pins the boundary
// between the two file classes that share the .template.<ext> name.
//
// A pack's agents/<name>/prompt.template.md belongs to the prompt renderer,
// which expands it later against a different data map and a funcmap overlay
// staging does not carry. Rendering is therefore opt-in per copy: a caller
// that was handed no install data has no business claiming the file, and must
// copy it through under its own name.
//
// This is the exact path `gc init --from-dir` takes to materialize a city from
// an example directory. Claiming those prompts here fails on `basename` and
// breaks init for every shipped example city.
func TestCopyDirWithSkip_LeavesTemplatesAloneWithoutOptIn(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	rel := filepath.Join("packs", "swarm", "agents", "coder", "prompt.template.md")
	writeOverlayFile(t, filepath.Join(src, rel), promptTemplateBody)

	if err := CopyDirWithSkip(src, dst, func(string, bool) bool { return false }, io.Discard); err != nil {
		t.Fatalf("CopyDirWithSkip: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dst, rel))
	if err != nil {
		t.Fatalf("prompt template was not copied under its own name: %v", err)
	}
	if string(got) != promptTemplateBody {
		t.Errorf("prompt template = %q, want a byte-for-byte copy", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "packs", "swarm", "agents", "coder", "prompt.md")); !os.IsNotExist(err) {
		t.Error("a copy with no template data renamed a prompt template to its rendered target")
	}
}

// TestCopyDir_LeavesTemplatesAloneWithoutOptIn pins the same boundary on the
// best-effort walk, which reports per-file failures to stderr rather than
// returning them — so a claimed prompt template would vanish from the
// destination instead of failing the copy.
func TestCopyDir_LeavesTemplatesAloneWithoutOptIn(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, "agents", "mayor", "prompt.template.md"), promptTemplateBody)

	var stderr strings.Builder
	if err := CopyDir(src, dst, &stderr); err != nil {
		t.Fatalf("CopyDir: %v", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("CopyDir reported %q; a prompt template is not an overlay template", stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(dst, "agents", "mayor", "prompt.template.md"))
	if err != nil {
		t.Fatalf("prompt template was not copied under its own name: %v", err)
	}
	if string(got) != promptTemplateBody {
		t.Errorf("prompt template = %q, want a byte-for-byte copy", got)
	}
}

// TestCopyDirForProviders_NoOptInCopiesTemplateVerbatim pins that the opt-in
// governs the provider walks too. Without WithTemplateData the historical
// byte-copy behavior stands: the marker stays in the name and the bytes are
// untouched.
func TestCopyDirForProviders_NoOptInCopiesTemplateVerbatim(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	const body = `{"city":"{{.CityRoot}}"}`
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.template.json"), body)

	var stderr strings.Builder
	if err := CopyDirForProviders(src, dst, []string{"codex"}, &stderr); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("CopyDirForProviders reported %q; want a silent byte copy", stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(dst, ".codex", "hooks.template.json"))
	if err != nil {
		t.Fatalf("template was not copied under its own name: %v", err)
	}
	if string(got) != body {
		t.Errorf("copied template = %q, want the unrendered source bytes", got)
	}
	if _, err := os.Stat(filepath.Join(dst, ".codex", "hooks.json")); !os.IsNotExist(err) {
		t.Error("a copy with no template data staged a rendered hooks.json")
	}
}

// TestCopyDirForProviders_NonTemplateFilesUnchanged pins acceptance criterion
// 2: both branches of the copy (plain byte copy and JSON merge) keep their
// current behavior for files with no .template marker.
func TestCopyDirForProviders_NonTemplateFilesUnchanged(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, "AGENTS.md"), "# instructions\n")
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.json"),
		`{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"fresh"}]}]}}`)
	writeOverlayFile(t, filepath.Join(dst, ".codex", "hooks.json"),
		`{"hooks":{"UserPromptSubmit":[{"matcher":"user","hooks":[{"type":"command","command":"mine"}]}]}}`)

	if err := CopyDirForProviders(src, dst, []string{"codex"}, io.Discard,
		WithTemplateData(map[string]string{"CityRoot": "/city/root"})); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dst, "AGENTS.md"))
	if err != nil {
		t.Fatalf("reading staged AGENTS.md: %v", err)
	}
	if string(got) != "# instructions\n" {
		t.Errorf("AGENTS.md = %q, want a byte-for-byte copy", got)
	}
	merged, err := os.ReadFile(filepath.Join(dst, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("reading merged hooks.json: %v", err)
	}
	for _, want := range []string{"SessionStart", "UserPromptSubmit"} {
		if !strings.Contains(string(merged), want) {
			t.Errorf("merged hooks.json lost %q: %s", want, merged)
		}
	}
}

// TestCopyDirForProviders_TemplateWithoutTokensStillRenames pins that the
// rename is unconditional: the marker names the file class, so a token-free
// template still lands at its target name rather than as a stray file.
func TestCopyDirForProviders_TemplateWithoutTokensStillRenames(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, "notes.template.md"), "static\n")

	if err := CopyDirForProviders(src, dst, nil, io.Discard, WithTemplateData(nil)); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "notes.md")); err != nil {
		t.Fatalf("token-free template did not land at its target name: %v", err)
	}
}

// TestCopyDirForProviders_TemplatePreservesSourcePermissions pins that a
// rendered file is created with the source template's permission bits, the
// same as the byte-copy path.
func TestCopyDirForProviders_TemplatePreservesSourcePermissions(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	script := filepath.Join(src, "run.template.sh")
	writeOverlayFile(t, script, "#!/bin/sh\necho {{.CityRoot}}\n")
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	if err := CopyDirForProviders(src, dst, nil, io.Discard,
		WithTemplateData(map[string]string{"CityRoot": "/city/root"})); err != nil {
		t.Fatalf("CopyDirForProviders: %v", err)
	}
	info, err := os.Stat(filepath.Join(dst, "run.sh"))
	if err != nil {
		t.Fatalf("stat staged script: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Errorf("staged script mode = %v, want 0755 (source permissions preserved)", info.Mode().Perm())
	}
}

// TestCopyDirForProvider_RendersTemplateOverlayFile pins that the single-slot
// provider copy carries the same seam as the multi-slot one.
func TestCopyDirForProvider_RendersTemplateOverlayFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeOverlayFile(t, filepath.Join(src, PerProviderDir, "codex", ".codex", "hooks.template.json"),
		`{"city":"{{.CityRoot}}"}`)

	if err := CopyDirForProvider(src, dst, "codex", io.Discard,
		WithTemplateData(map[string]string{"CityRoot": "/city/root"})); err != nil {
		t.Fatalf("CopyDirForProvider: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dst, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("reading staged hooks.json: %v", err)
	}
	if !strings.Contains(string(data), "/city/root") {
		t.Errorf("staged hooks.json = %s, want the rendered CityRoot", data)
	}
}

func writeOverlayFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}
