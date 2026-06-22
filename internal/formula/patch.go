package formula

import "fmt"

// Patch overlays a single named formula in place, without renaming or
// copying it. It is the formula-level analog of an agent patch: a pack can
// declare `[[patches.formula]]` to adjust an imported formula whose name is
// pinned by the engine (e.g. the patrol formulas matched by
// sling.SlingFormulaUsesTargetBranch) or reused verbatim by base prompts.
//
// The overlay reuses the exact merge machinery the `extends` path already
// implements (mergeSteps for step override/append, the var-merge loop, and
// mergeComposeRules) so a patched formula composes identically to an
// `extends` child — except the formula keeps its original name.
//
// Application happens at resolve time, after the `extends` chain is fully
// merged and before name-pinned consumers read the formula (see
// Parser.Resolve / Parser.WithPatches).
type Patch struct {
	// Formula is the target formula name (required). It MUST match the name of
	// the formula being patched; the overlay never renames its target.
	Formula string `toml:"formula" json:"formula"`

	// Description overrides the target's description when non-empty. This
	// mirrors the child-wins description rule in Resolve.
	Description string `toml:"description,omitempty" json:"description,omitempty"`

	// Vars adds or overrides template variables. Existing vars with the same
	// name are replaced; new vars are added. Mirrors the child var-override
	// loop in Resolve.
	Vars map[string]*VarDef `toml:"vars,omitempty" json:"vars,omitempty"`

	// Steps override existing steps BY ID, preserving position. Every step
	// listed here MUST already exist in the resolved target; overriding a
	// step that does not exist is a hard error (a typo would otherwise
	// silently append a broken step). Authored as `[[patches.formula.step]]`.
	Steps []*Step `toml:"step,omitempty" json:"steps,omitempty"`

	// AppendSteps append NEW steps at the end. Every step listed here MUST
	// carry an ID that does not already exist in the resolved target; reusing
	// an existing ID is a hard error (use Steps to override instead).
	// Authored as `[[patches.formula.append_step]]`.
	AppendSteps []*Step `toml:"append_step,omitempty" json:"append_steps,omitempty"`

	// Compose merges composition rules into the target via mergeComposeRules
	// (bond points override by ID; hooks/expand/map append).
	Compose *ComposeRules `toml:"compose,omitempty" json:"compose,omitempty"`
}

// IsEmpty reports whether the patch carries no overlay operations. A patch
// that only names a target but changes nothing is still structurally valid
// (it is a no-op), so this is used by callers that want to skip work, not for
// validation.
func (p *Patch) IsEmpty() bool {
	return p == nil ||
		(p.Description == "" &&
			len(p.Vars) == 0 &&
			len(p.Steps) == 0 &&
			len(p.AppendSteps) == 0 &&
			p.Compose == nil)
}

// ApplyPatch returns a new formula with the patch overlaid onto base.
// base is never mutated: its step slice, var map, and template slice are
// copied before any overlay is applied, so a cached parsed formula can be
// safely patched.
//
// Errors are returned for: a target-name mismatch, an override step whose ID
// is not present in base, an append step whose ID already exists, and a
// resulting formula that fails validation.
func ApplyPatch(base *Formula, patch *Patch) (*Formula, error) {
	if patch == nil {
		return base, nil
	}
	if patch.Formula != base.Formula {
		return nil, fmt.Errorf("formula patch targets %q but was applied to %q", patch.Formula, base.Formula)
	}

	out := clonePatchable(base)

	// Index existing step IDs so override vs. append intent can be validated
	// before any merge runs.
	existing := make(map[string]bool, len(out.Steps))
	for _, s := range out.Steps {
		existing[s.ID] = true
	}
	for _, s := range patch.Steps {
		if !existing[s.ID] {
			return nil, fmt.Errorf("formula patch %q: cannot override unknown step id %q (use [[patches.formula.append_step]] to add a new step)", patch.Formula, s.ID)
		}
	}
	for _, s := range patch.AppendSteps {
		if existing[s.ID] {
			return nil, fmt.Errorf("formula patch %q: cannot append step id %q because it already exists (use [[patches.formula.step]] to override it)", patch.Formula, s.ID)
		}
	}

	// Override existing steps in place (mergeSteps overrides by ID, preserving
	// position) then append new steps (mergeSteps appends IDs it does not find).
	// Reuses the same machinery as the extends path.
	if len(patch.Steps) > 0 {
		out.Steps = mergeSteps(out.Steps, patch.Steps)
	}
	if len(patch.AppendSteps) > 0 {
		out.Steps = mergeSteps(out.Steps, patch.AppendSteps)
	}

	// Var-merge: overlay vars win over base vars, mirroring child overrides.
	for name, def := range patch.Vars {
		out.Vars[name] = def
	}

	// Compose-rule merge (overlay over base).
	out.Compose = mergeComposeRules(out.Compose, patch.Compose)

	if patch.Description != "" {
		out.Description = patch.Description
	}

	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("formula patch %q: patched formula is invalid: %w", patch.Formula, err)
	}
	return out, nil
}

// clonePatchable returns a shallow struct copy of f with its mutable
// containers (Steps, Template, Vars) re-allocated so overlay edits never
// reach back into the source formula. The contained *Step / *VarDef pointers
// are shared: overlay operations replace pointers rather than mutating their
// targets, so sharing is safe.
func clonePatchable(f *Formula) *Formula {
	out := *f
	out.Steps = append([]*Step(nil), f.Steps...)
	out.Template = append([]*Step(nil), f.Template...)
	out.Vars = make(map[string]*VarDef, len(f.Vars))
	for k, v := range f.Vars {
		out.Vars[k] = v
	}
	return &out
}
