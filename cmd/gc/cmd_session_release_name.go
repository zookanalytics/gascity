package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/spf13/cobra"
)

// newSessionReleaseNameCmd creates the "gc session release-name <name-or-alias>"
// command.
func newSessionReleaseNameCmd(stdout, stderr io.Writer) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "release-name <session-name-or-alias>",
		Short: "Free a runtime name still reserved by a closed session",
		Long: `Clear the runtime session name, alias, and canonical-identity record held
by closed session beads matching the given name or alias.

A closed session bead keeps the identifiers it ran under, and name resolution
still consults them, so the name can stay reserved after every runtime that
used it is gone. Nothing else releases it: nudge, wake, and kill all reject a
closed bead, and prune does not accept the closed state. Use this when
creating or waking a session fails with "already belongs to <bead-id>
(closed)".

A name held by a live session is refused — stop that session instead.`,
		Example: `  gc session release-name rig/pack.refinery
  gc session release-name city--pack__refinery --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if cmdSessionReleaseName(args[0], stdout, stderr, jsonOutput) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit JSONL")
	return cmd
}

// cmdSessionReleaseName is the CLI entry point for "gc session release-name".
func cmdSessionReleaseName(target string, stdout, stderr io.Writer, jsonOutput ...bool) int {
	asJSON := sessionJSONRequested(jsonOutput)

	store, code := openCityStore(stderr, "gc session release-name")
	if store == nil {
		return code
	}

	// The reservation lives on session beads, so route through the session
	// coordination-class store for relocation-safety, as the other session
	// catalog commands do.
	cityPath, cityErr := resolveCity()
	var cfg *config.City
	if cityErr == nil {
		cfg, _ = loadCityConfig(cityPath, configWarnWriter(asJSON, stderr))
	}
	sessStore := cliSessionStore(store, cfg, cityPath)

	released, err := session.ReleaseSessionNameClaim(sessStore, target)
	if err != nil {
		fmt.Fprintf(stderr, "gc session release-name: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}

	count := len(released)
	if asJSON {
		result := sessionActionResult{
			Action:   "release-name",
			Identity: strings.TrimSpace(target),
			Count:    &count,
		}
		if count == 1 {
			result.SessionID = released[0].BeadID
		}
		if err := writeSessionActionJSON(stdout, result); err != nil {
			fmt.Fprintf(stderr, "gc session release-name: %v\n", err) //nolint:errcheck // best-effort stderr
			return 1
		}
		return 0
	}

	if count == 0 {
		fmt.Fprintf(stdout, "No closed session reserves %q.\n", target) //nolint:errcheck // best-effort stdout
		return 0
	}
	ids := make([]string, 0, count)
	for _, r := range released {
		ids = append(ids, r.BeadID)
	}
	fmt.Fprintf(stdout, "Released %q from %s.\n", target, strings.Join(ids, ", ")) //nolint:errcheck // best-effort stdout
	return 0
}
