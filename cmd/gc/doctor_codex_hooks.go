package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/hooks"
	"github.com/gastownhall/gascity/internal/suspensionstate"
	workdirutil "github.com/gastownhall/gascity/internal/workdir"
)

// codexHooksDriftCheck audits the Codex hooks files Codex reads for the city's
// Codex agents for copies of Gas City's managed hooks.
type codexHooksDriftCheck struct {
	dirs []string
}

func newCodexHooksDriftCheck(dirs []string) *codexHooksDriftCheck {
	return &codexHooksDriftCheck{dirs: cleanCodexHookDirs(dirs)}
}

func codexHookWorkDirs(cityPath string, cfg *config.City) []string {
	var dirs []string
	addCodexHookDir(&dirs, cityPath)
	if cfg == nil {
		return dirs
	}
	suspState, _ := loadSuspensionState(fsys.OSFS{}, cityPath)
	suspendedRigPaths := map[string]bool{}
	for i := range cfg.Rigs {
		rig := &cfg.Rigs[i]
		suspended := suspensionstate.EffectiveRigSuspended(suspState, rig.Name, rig.EffectiveSuspendedOnStart())
		if suspended || strings.TrimSpace(rig.Path) == "" {
			if suspended && strings.TrimSpace(rig.Path) != "" {
				suspendedRigPaths[filepath.Clean(rig.Path)] = true
			}
			continue
		}
		addCodexHookDir(&dirs, rig.Path)
	}
	for i := range cfg.Agents {
		agent := &cfg.Agents[i]
		if agent.Suspended || agentInSuspendedRig(cityPath, agent, cfg.Rigs, suspendedRigPaths) {
			continue
		}
		if !agentUsesCodexHookSurface(cfg, agent) {
			continue
		}
		addCodexHookAgentWorkDirs(&dirs, cityPath, cfg, agent)
	}
	return dirs
}

func cleanCodexHookDirs(dirs []string) []string {
	var cleaned []string
	for _, dir := range dirs {
		addCodexHookDir(&cleaned, dir)
	}
	sort.Strings(cleaned)
	return cleaned
}

func addCodexHookDir(dirs *[]string, dir string) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return
	}
	dir = filepath.Clean(dir)
	for _, existing := range *dirs {
		if existing == dir {
			return
		}
	}
	*dirs = append(*dirs, dir)
}

func agentUsesCodexHookSurface(cfg *config.City, agent *config.Agent) bool {
	if cfg == nil || agent == nil {
		return false
	}
	if codexHookProviderName(codexHookEffectiveAgentProvider(cfg, agent), cfg.Providers) {
		return true
	}
	for _, provider := range config.ResolveInstallHooks(agent, &cfg.Workspace) {
		if codexHookProviderName(provider, cfg.Providers) {
			return true
		}
	}
	return false
}

func codexHookEffectiveAgentProvider(cfg *config.City, agent *config.Agent) string {
	if agent == nil {
		return ""
	}
	if provider := strings.TrimSpace(agent.Provider); provider != "" {
		return provider
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.Workspace.Provider)
	}
	return ""
}

func codexHookProviderName(name string, providers map[string]config.ProviderSpec) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	return name == "codex" || config.BuiltinFamily(name, providers) == "codex"
}

func addCodexHookAgentWorkDirs(dirs *[]string, cityPath string, cfg *config.City, agent *config.Agent) {
	addCodexHookAgentWorkDir(dirs, cityPath, cfg, agent, agent.QualifiedName())
	for _, slot := range codexHookPoolSlots(agent) {
		instanceAgent, qualifiedInstance, _ := poolDesiredRequestIdentity(agent, slot)
		if qualifiedInstance == agent.QualifiedName() {
			continue
		}
		addCodexHookAgentWorkDir(dirs, cityPath, cfg, instanceAgent, qualifiedInstance)
	}
}

func addCodexHookAgentWorkDir(dirs *[]string, cityPath string, cfg *config.City, agent *config.Agent, qualifiedName string) {
	workDir, err := resolveCodexHookAgentWorkDir(cityPath, cfg, agent, qualifiedName)
	if err != nil {
		return
	}
	addCodexHookDir(dirs, workDir)
}

func resolveCodexHookAgentWorkDir(cityPath string, cfg *config.City, agent *config.Agent, qualifiedName string) (string, error) {
	if agent == nil {
		return "", nil
	}
	cityName := loadedCityName(cfg, cityPath)
	var rigs []config.Rig
	if cfg != nil {
		rigs = cfg.Rigs
	}
	if strings.TrimSpace(qualifiedName) == "" {
		qualifiedName = agent.QualifiedName()
	}
	workDir, err := workdirutil.ResolveWorkDirPathStrict(cityPath, cityName, qualifiedName, *agent, rigs)
	if err != nil {
		return "", err
	}
	if err := workdirutil.ValidateAncestorWorktreesNotStale(workDir); err != nil {
		return "", err
	}
	return workDir, nil
}

func codexHookPoolSlots(agent *config.Agent) []int {
	if agent == nil || !agent.SupportsInstanceExpansion() {
		return nil
	}
	limit := 1
	if len(agent.NamepoolNames) > 0 {
		limit = len(agent.NamepoolNames)
	} else if maxSessions := agent.EffectiveMaxActiveSessions(); maxSessions != nil {
		if *maxSessions <= 1 {
			return nil
		}
		limit = *maxSessions
	} else if minSessions := agent.EffectiveMinActiveSessions(); minSessions > 1 {
		limit = minSessions
	}
	slots := make([]int, 0, limit)
	for slot := 1; slot <= limit; slot++ {
		slots = append(slots, slot)
	}
	return slots
}

func (c *codexHooksDriftCheck) Name() string { return "codex-hooks-drift" }

func (c *codexHooksDriftCheck) CanFix() bool { return true }

// Fix removes Gas City's managed hook entries from each audited Codex hooks
// file, keeping every entry Gas City does not manage.
func (c *codexHooksDriftCheck) Fix(_ *doctor.CheckContext) error {
	for _, dir := range c.dirs {
		if !codexHooksHaveManagedEntries(filepath.Join(dir, ".codex", "hooks.json")) {
			continue
		}
		if err := hooks.StripManagedCodexHooks(fsys.OSFS{}, dir); err != nil {
			return fmt.Errorf("removing managed Codex hooks from %s: %w", dir, err)
		}
	}
	return nil
}

// Run reports each audited Codex hooks file that holds a Gas City managed hook
// entry. Every managed Codex session gets those hooks from its launch command,
// so a copy in a file Codex reads for the session runs each hook a second
// time. The audited directories are the ones Codex reads project hooks from
// for a Codex agent: the agent's work directory, the city root above a work
// directory inside the city, and a rig's main checkout, whose .codex is where
// Codex looks for a linked worktree of that rig.
func (c *codexHooksDriftCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	var duplicates []string
	for _, dir := range c.dirs {
		path := filepath.Join(dir, ".codex", "hooks.json")
		if codexHooksHaveManagedEntries(path) {
			duplicates = append(duplicates, path)
		}
	}
	if len(duplicates) == 0 {
		return okCheck(c.Name(), "no Codex hooks file repeats the hooks Codex sessions get at launch")
	}
	return warnCheck(c.Name(),
		fmt.Sprintf("%d Codex hooks file(s) repeat Gas City's managed hooks", len(duplicates)),
		"Codex sessions get Gas City's hooks from their launch command, so a copy in these files runs each hook twice; run `gc doctor --fix` to remove the copies",
		duplicates)
}

func codexHooksHaveManagedEntries(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return hooks.CodexHooksHaveManagedEntries(data)
}
