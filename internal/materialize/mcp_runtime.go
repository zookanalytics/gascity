package materialize

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/git"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
	workdirutil "github.com/gastownhall/gascity/internal/workdir"
)

// EffectiveMCPForSession loads, expands, and resolves the effective MCP
// catalog for one concrete session context.
func EffectiveMCPForSession(
	cfg *config.City,
	cityPath string,
	agent *config.Agent,
	identity string,
	workDir string,
) (MCPCatalog, error) {
	cfgForMCP := cfg
	if cfg != nil && cfg.PackMCPDir == "" {
		cityMCPDir := filepath.Join(cityPath, "mcp")
		if info, err := os.Stat(cityMCPDir); err == nil && info.IsDir() {
			clone := *cfg
			clone.PackMCPDir = cityMCPDir
			cfgForMCP = &clone
		}
	}
	return EffectiveMCPForAgent(cfgForMCP, agent, PackTemplateData(cfgForMCP, cityPath, agent, identity, workDir))
}

// PackTemplateData builds the template expansion surface every templated pack
// file expands against, for one concrete session context. Both templated file
// classes share it: MCP catalog entries named "<name>.template.toml" and
// overlay files named "<name>.template.<ext>" staged into an agent's work
// directory. One surface means a pack author learns one vocabulary —
// CityRoot, RigRoot, WorkDir, AgentName, the queries, and the agent env —
// rather than one per loader.
//
// CityRootShellQuoted is the shell-safe form of CityRoot (shellquote.Quote):
// an overlay file that embeds the city root inside a shell command must expand
// it through this key, not by wrapping {{.CityRoot}} in literal quotes, or a
// city root containing a shell metacharacter renders a malformed command.
func PackTemplateData(
	cfg *config.City,
	cityPath string,
	agent *config.Agent,
	identity string,
	workDir string,
) map[string]string {
	if agent == nil {
		branch := defaultMCPBranch(workDir)
		return map[string]string{
			"CityRoot":                cityPath,
			"CityRootShellQuoted":     shellquote.Quote(cityPath),
			"CityRootShellQuotedJSON": CityRootShellQuotedJSON(cityPath),
			"AgentName":               identity,
			"TemplateName":            identity,
			"WorkDir":                 workDir,
			"Branch":                  branch,
			"DefaultBranch":           branch,
		}
	}
	var rigs []config.Rig
	beadsCfg := config.BeadsConfig{}
	if cfg != nil {
		rigs = cfg.Rigs
		beadsCfg = cfg.Beads
	}
	rigName := workdirutil.ConfiguredRigName(cityPath, *agent, rigs)
	rigRoot := workdirutil.RigRootForName(rigName, rigs)
	templateName := agentutil.RoutedToIdentity(agent)
	if templateName == "" {
		templateName = identity
	}
	data := make(map[string]string, len(agent.Env)+15)
	for key, value := range agent.Env {
		data[key] = value
	}
	branch := defaultMCPBranch(workDir)
	data["CityRoot"] = cityPath
	data["CityRootShellQuoted"] = shellquote.Quote(cityPath)
	data["CityRootShellQuotedJSON"] = CityRootShellQuotedJSON(cityPath)
	data["AgentName"] = identity
	data["TemplateName"] = templateName
	data["RigName"] = rigName
	data["RigRoot"] = rigRoot
	data["WorkDir"] = workDir
	data["IssuePrefix"] = mcpRigPrefix(rigName, rigs)
	data["Branch"] = branch
	data["DefaultBranch"] = branch
	data["WorkQuery"] = agent.EffectiveWorkQueryForBeads(beadsCfg)
	data["AssignedInProgressQuery"] = agent.EffectiveAssignedInProgressQueryForBeads(beadsCfg)
	data["AssignedReadyQuery"] = agent.EffectiveAssignedReadyQueryForBeads(beadsCfg)
	data["RoutedPoolQuery"] = agent.EffectiveRoutedPoolQueryForBeads(beadsCfg)
	data["SlingQuery"] = agent.EffectiveSlingQuery()
	return data
}

// CityRootShellQuotedJSON is the shell-safe city root (shellquote.Quote)
// additionally escaped for placement inside a JSON string value. An overlay
// template that carries a shell command inside a JSON document — the Codex
// .codex/hooks.json commands are the one shipped case — must expand the city
// root through this key rather than CityRootShellQuoted: shellquote.Quote
// renders an embedded apostrophe as the sequence '\”, whose backslash is not
// a valid JSON string escape, so binding the raw shell-quoted form stages a
// hooks document that no JSON parser can load. The JSON-escaped form keeps the
// staged document valid JSON while decoding back to exactly the shell-safe
// --city binding. It equals CityRootShellQuoted for any city root without a
// character JSON must escape (the common case).
func CityRootShellQuotedJSON(cityPath string) string {
	return jsonStringInner(shellquote.Quote(cityPath))
}

// jsonStringInner returns s escaped for placement inside a JSON string literal,
// without the surrounding double quotes — the JSON-context analog of shell
// quoting. It escapes through encoding/json (HTML escaping disabled so shell
// metacharacters stay readable) rather than by hand, then strips the quotes the
// encoder adds.
func jsonStringInner(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// Encoding a string never errors; Encode appends the literal plus a newline.
	_ = enc.Encode(s)
	encoded := strings.TrimRight(buf.String(), "\n")
	return encoded[1 : len(encoded)-1]
}

// RuntimeMCPServers converts neutral MCP servers into runtime-owned ACP
// session/new server definitions.
func RuntimeMCPServers(servers []MCPServer) []runtime.MCPServerConfig {
	if len(servers) == 0 {
		return nil
	}
	out := make([]runtime.MCPServerConfig, 0, len(servers))
	for _, server := range servers {
		entry := runtime.MCPServerConfig{
			Name:    server.Name,
			Command: server.Command,
			Args:    append([]string(nil), server.Args...),
			Env:     cloneStringMap(server.Env),
			URL:     server.URL,
			Headers: cloneStringMap(server.Headers),
		}
		switch server.Transport {
		case MCPTransportHTTP:
			entry.Transport = runtime.MCPTransportHTTP
		case MCPTransportSSE:
			entry.Transport = runtime.MCPTransportSSE
		default:
			entry.Transport = runtime.MCPTransportStdio
		}
		out = append(out, entry)
	}
	return runtime.NormalizeMCPServerConfigs(out)
}

func mcpRigPrefix(rigName string, rigs []config.Rig) string {
	for i := range rigs {
		if rigs[i].Name == rigName {
			return rigs[i].EffectivePrefix()
		}
	}
	return ""
}

func defaultMCPBranch(dir string) string {
	if dir == "" {
		return "main"
	}
	g := git.New(filepath.Clean(dir))
	branch, _ := g.DefaultBranch()
	if branch == "" {
		return "main"
	}
	return branch
}
