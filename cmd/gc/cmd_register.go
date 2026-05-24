package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/gastownhall/gascity/internal/api"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/supervisor"
	"github.com/spf13/cobra"
)

func newRegisterCmd(stdout, stderr io.Writer) *cobra.Command {
	var nameFlag string
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "register [path]",
		Short: "Register a city with the machine-wide supervisor",
		Long: `Register a city directory with the machine-wide supervisor.

If no path is given, registers the current city (discovered from cwd).
Use --name to set the machine-local registration alias. The alias is stored
in the machine-local supervisor registry and never written back to city.toml.
When --name is omitted, the current effective city identity is used
(site-bound workspace name if present, otherwise legacy workspace.name,
otherwise the directory basename) — in every case city.toml is not modified.
Registration is idempotent — registering the same city twice is a no-op.
The supervisor is started if needed and immediately reconciles the city.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if doRegisterWithOptionsJSON(args, nameFlag, jsonOut, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&nameFlag, "name", "", "machine-local alias for this city registration")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit JSONL summary")
	cmd.Flags().BoolVar(&assumeYesForSupervisorCycle, "yes", false, "bypass the cross-city supervisor cycle confirmation prompt (warning is still printed for the audit trail)")
	return cmd
}

func doRegister(args []string, stdout, stderr io.Writer) int {
	return doRegisterWithOptions(args, "", stdout, stderr)
}

func doRegisterWithOptions(args []string, nameOverride string, stdout, stderr io.Writer) int {
	return doRegisterWithOptionsJSON(args, nameOverride, false, stdout, stderr)
}

func doRegisterWithOptionsJSON(args []string, nameOverride string, jsonOut bool, stdout, stderr io.Writer) int {
	var cityPath string
	var err error
	if len(args) > 0 {
		cityPath, err = validateCityPath(args[0])
	} else {
		cityPath, err = resolveCommandCity(nil)
	}
	if err != nil {
		fmt.Fprintf(stderr, "gc register: %v\n", err) //nolint:errcheck
		return 1
	}

	// Verify it's a city directory (city.toml is the defining marker).
	if _, sErr := os.Stat(filepath.Join(cityPath, "city.toml")); sErr != nil {
		fmt.Fprintf(stderr, "gc register: %s is not a city directory (no city.toml found)\n", cityPath) //nolint:errcheck
		return 1
	}
	registerName, err := resolveRegistrationName(cityPath, nameOverride)
	if err != nil {
		fmt.Fprintf(stderr, "gc register: %v\n", err) //nolint:errcheck
		return 1
	}
	registerStdout := stdout
	var registerProgress bytes.Buffer
	if jsonOut {
		registerStdout = &registerProgress
	}
	code := registerCityWithSupervisorNamed(cityPath, registerName, registerStdout, stderr, "gc register", true)
	if code != 0 {
		replayJSONModeProgress(stderr, &registerProgress)
		return code
	}
	if !jsonOut {
		return code
	}
	return writeLifecycleActionJSONOrExit(stdout, stderr, "gc register", lifecycleActionJSON{
		Command:  "register",
		Action:   "register",
		Message:  "City registered.",
		CityName: registerName,
		CityPath: cityPath,
	})
}

// resolveRegistrationName returns the machine-local alias to store in the
// supervisor registry. The alias is never written back to city.toml — the
// registry is the sole source of truth for registration identity
// (gastownhall/gascity#602).
func resolveRegistrationName(cityPath, nameOverride string) (string, error) {
	cfg, err := config.Load(fsys.OSFS{}, filepath.Join(cityPath, "city.toml"))
	if err != nil {
		return "", fmt.Errorf("loading city.toml: %w", err)
	}
	if alias := strings.TrimSpace(nameOverride); alias != "" {
		return alias, nil
	}
	return config.EffectiveCityName(cfg, filepath.Base(filepath.Clean(cityPath))), nil
}

func newUnregisterCmd(stdout, stderr io.Writer) *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "unregister [path]",
		Short: "Remove a city from the machine-wide supervisor",
		Long: `Remove a city from the machine-wide supervisor registry.

If no path is given, unregisters the current city (discovered from cwd).
If the supervisor is running, it immediately stops managing the city.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if doUnregisterJSON(args, jsonOut, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit JSONL summary")
	return cmd
}

func doUnregister(args []string, stdout, stderr io.Writer) int {
	return doUnregisterJSON(args, false, stdout, stderr)
}

func doUnregisterJSON(args []string, jsonOut bool, stdout, stderr io.Writer) int {
	var cityPath string
	var err error
	if len(args) > 0 {
		cityPath, err = filepath.Abs(args[0])
		if err == nil {
			cityPath = normalizePathForCompare(cityPath)
		}
	} else {
		cityPath, err = resolveCommandCity(nil)
	}
	if err != nil {
		fmt.Fprintf(stderr, "gc unregister: %v\n", err) //nolint:errcheck
		return 1
	}
	entry, registered, _ := registeredCityEntry(cityPath)
	unregisterStdout := stdout
	var unregisterProgress bytes.Buffer
	if jsonOut {
		unregisterStdout = &unregisterProgress
	}
	_, code := unregisterCityFromSupervisor(cityPath, unregisterStdout, stderr)
	if code != 0 {
		replayJSONModeProgress(stderr, &unregisterProgress)
		return code
	}
	if !jsonOut {
		return code
	}
	cityName := ""
	if registered {
		cityName = entry.EffectiveName()
	}
	return writeLifecycleActionJSONOrExit(stdout, stderr, "gc unregister", lifecycleActionJSON{
		Command:  "unregister",
		Action:   "unregister",
		Message:  "City unregistered.",
		CityName: cityName,
		CityPath: cityPath,
	})
}

func replayJSONModeProgress(stderr io.Writer, progress *bytes.Buffer) {
	if progress == nil || progress.Len() == 0 {
		return
	}
	_, _ = io.Copy(stderr, progress)
}

func newCitiesCmd(stdout, stderr io.Writer) *cobra.Command {
	var jsonOutput bool
	runList := func(_ *cobra.Command, _ []string) error {
		if doCities(jsonOutput, stdout, stderr) != 0 {
			return errExit
		}
		return nil
	}
	cmd := &cobra.Command{
		Use:   "cities",
		Short: "List registered cities",
		Long:  `List all cities registered with the machine-wide supervisor.`,
		Args:  cobra.NoArgs,
		RunE:  runList,
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output one JSONL result record")
	listCmd := &cobra.Command{
		Use:     "list",
		Short:   "List registered cities",
		Aliases: []string{"ls"},
		Args:    cobra.NoArgs,
		RunE:    runList,
	}
	listCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output one JSONL result record")
	cmd.AddCommand(listCmd)
	return cmd
}

type citiesListJSON struct {
	SchemaVersion string             `json:"schema_version"`
	RegistryPath  string             `json:"registry_path"`
	Cities        []cityRegistryJSON `json:"cities"`
}

type cityRegistryJSON struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

func doCities(jsonOutput bool, stdout, stderr io.Writer) int {
	registryPath := supervisor.RegistryPath()
	reg := supervisor.NewRegistry(registryPath)
	entries, err := reg.List()
	if err != nil {
		fmt.Fprintf(stderr, "gc cities: %v\n", err) //nolint:errcheck
		return 1
	}

	if jsonOutput {
		cities := make([]cityRegistryJSON, 0, len(entries))
		for _, e := range entries {
			cities = append(cities, cityRegistryJSON{
				Name: e.EffectiveName(),
				Path: e.Path,
			})
		}
		if err := writeCLIJSONLine(stdout, citiesListJSON{
			SchemaVersion: "1",
			RegistryPath:  registryPath,
			Cities:        cities,
		}); err != nil {
			fmt.Fprintf(stderr, "gc cities: writing JSON: %v\n", err) //nolint:errcheck
			return 1
		}
		return 0
	}

	if len(entries) == 0 {
		fmt.Fprintln(stdout, "No cities registered. Use 'gc register' to add a city.") //nolint:errcheck
		return 0
	}

	// stateByPath maps normalized registry paths to their runtime state
	// label. Empty when the supervisor isn't running so the column degrades
	// to "stopped" for every entry rather than misleading "running" output
	// when the API is unreachable.
	stateByPath := supervisorCityStates()

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATE\tPATH") //nolint:errcheck
	for _, e := range entries {
		state := "stopped"
		if normalized, nErr := normalizeRegisteredCityPath(e.Path); nErr == nil {
			if s, ok := stateByPath[normalized]; ok {
				state = s
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", e.EffectiveName(), state, e.Path) //nolint:errcheck
	}
	tw.Flush() //nolint:errcheck
	return 0
}

// supervisorCityStates returns a map from normalized city path to a
// state label ("running", "suspended", or "stopped") sourced from the
// supervisor API. Returns an empty map when the supervisor is not
// reachable; doCities then treats every registered city as "stopped".
func supervisorCityStates() map[string]string {
	if supervisorAliveHook() == 0 {
		return nil
	}
	baseURL, err := supervisorAPIBaseURL()
	if err != nil {
		return nil
	}
	client := api.NewClient(baseURL)
	cities, err := client.ListCities()
	if err != nil {
		return nil
	}
	out := make(map[string]string, len(cities))
	for _, c := range cities {
		normalized, err := normalizeRegisteredCityPath(c.Path)
		if err != nil {
			continue
		}
		out[normalized] = cityStateLabel(c)
	}
	return out
}

// cityStateLabel reduces the (Running, Suspended) pair to a single
// label. Suspended takes precedence because operators chasing the
// "session attaches and immediately disappears" symptom care more about
// the suspended flag than about the supervisor's process management;
// "running" alone hides the state that actually drives session drains.
func cityStateLabel(c api.CityInfo) string {
	switch {
	case c.Suspended:
		return "suspended"
	case c.Running:
		return "running"
	default:
		return "stopped"
	}
}
