package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/gastownhall/gascity/internal/nudgequeue"
	"github.com/spf13/cobra"
)

// The canonical answers `gc nudge show` gives about one queued nudge.
//
// A caller that nudges an agent and hears nothing back cannot act on that
// silence until it knows whether the message arrived: "healthy but did not
// answer" and "the nudge never reached anyone" call for opposite responses.
// These outcomes are that distinction, reported per nudge id.
//
// The two below cover a nudge still live in the queue. A nudge that has left
// the queue is classified by nudgequeue.OutcomeForState, which answers
// delivered, dropped, or unknown for a state it cannot classify.
const (
	// nudgeOutcomePending: accepted and still waiting for a delivery boundary.
	nudgeOutcomePending = "pending"
	// nudgeOutcomeInFlight: claimed by a delivery pass, not yet acknowledged.
	nudgeOutcomeInFlight = "in_flight"
	// nudgeOutcomeDelivered: handed to the target's runtime.
	nudgeOutcomeDelivered = nudgequeue.OutcomeDelivered
	// nudgeOutcomeDropped: terminalized without delivery — dead-lettered,
	// expired, withdrawn or superseded. Reason carries which.
	nudgeOutcomeDropped = nudgequeue.OutcomeDropped
)

// nudgeOutcomeReport is the resolved fate of a single queued nudge. State is
// the underlying record ("pending"/"in_flight"/"dead" for a live queue entry,
// or the shadow bead's lifecycle state) and Outcome is its classification;
// both are reported so a caller can act on the outcome without losing the
// detail that produced it.
type nudgeOutcomeReport struct {
	NudgeID   string
	Outcome   string
	State     string
	Reason    string
	Agent     string
	SessionID string
	Source    string
	Message   string
}

type nudgeShowJSON struct {
	SchemaVersion string `json:"schema_version"`
	Command       string `json:"command"`
	CityPath      string `json:"city_path"`
	NudgeID       string `json:"nudge_id"`
	Outcome       string `json:"outcome"`
	State         string `json:"state"`
	Reason        string `json:"reason,omitempty"`
	Agent         string `json:"agent,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	Source        string `json:"source,omitempty"`
	Message       string `json:"message,omitempty"`
}

func newNudgeShowCmd(stdout, stderr io.Writer) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "show <nudge-id>",
		Short: "Show whether a queued nudge was delivered or dropped",
		Long: `Show whether a queued nudge was delivered or dropped.

Takes the nudge id "gc session nudge" reports when it queues a nudge (the
nudge_id field of its --json output). Reports exactly one outcome:

  pending     accepted, waiting for a delivery boundary
  in_flight   claimed by a delivery pass, not yet acknowledged
  delivered   handed to the target's runtime
  dropped     terminalized without delivery; the reason says why

Use it before reading anything into a nudged agent's silence: a dropped
nudge never reached the agent, so silence says nothing about its health.

The command reports the outcome rather than judging it — a "dropped"
outcome still exits 0. Exit 1 means the nudge id is unknown to this city.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if cmdNudgeShow(args[0], jsonOutput, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	return cmd
}

func cmdNudgeShow(nudgeID string, jsonOutput bool, stdout, stderr io.Writer) int {
	nudgeID = strings.TrimSpace(nudgeID)
	if nudgeID == "" {
		fmt.Fprintln(stderr, "gc nudge show: nudge id required") //nolint:errcheck
		return 1
	}
	cityPath, err := resolveCity()
	if err != nil {
		fmt.Fprintf(stderr, "gc nudge show: %v\n", err) //nolint:errcheck
		return 1
	}
	report, found, err := resolveQueuedNudgeOutcome(cityPath, nudgeID)
	if err != nil {
		fmt.Fprintf(stderr, "gc nudge show: %v\n", err) //nolint:errcheck
		return 1
	}
	if !found {
		fmt.Fprintf(stderr, "gc nudge show: no record of nudge %q in this city\n", nudgeID) //nolint:errcheck
		return 1
	}
	return renderNudgeOutcome(cityPath, report, jsonOutput, stdout, stderr)
}

// resolveQueuedNudgeOutcome reports the fate of one nudge, reading the flock'd
// queue first and the durable shadow bead second.
//
// The order is the authority order, not a convenience: while an item is live
// the queue owns its state, and once it is delivered or terminalized it leaves
// the queue entirely and only the shadow bead remembers what happened. Reading
// the bead first would report a stale "queued" for an item the queue has since
// moved on.
func resolveQueuedNudgeOutcome(cityPath, nudgeID string) (nudgeOutcomeReport, bool, error) {
	state, err := loadNudgeQueueState(cityPath)
	if err != nil {
		return nudgeOutcomeReport{}, false, err
	}
	buckets := []struct {
		items   []queuedNudge
		state   string
		outcome string
	}{
		{items: state.Pending, state: "pending", outcome: nudgeOutcomePending},
		{items: state.InFlight, state: "in_flight", outcome: nudgeOutcomeInFlight},
		{items: state.Dead, state: "dead", outcome: nudgeOutcomeDropped},
	}
	for _, bucket := range buckets {
		for _, item := range bucket.items {
			if item.ID != nudgeID {
				continue
			}
			reason := item.LastError
			if bucket.state == "dead" {
				reason = deadReason(item)
			}
			return nudgeOutcomeReport{
				NudgeID:   item.ID,
				Outcome:   bucket.outcome,
				State:     bucket.state,
				Reason:    reason,
				Agent:     item.Agent,
				SessionID: item.SessionID,
				Source:    item.Source,
				Message:   item.Message,
			}, true, nil
		}
	}

	store := openNudgeBeadStore(cityPath)
	if store.Store == nil {
		return nudgeOutcomeReport{}, false, nil
	}
	defer closeBeadStoreHandle(store.Store) //nolint:errcheck // best-effort
	shadow, ok, err := nudgeFrontDoor(store).FindIncludingTerminal(nudgeID)
	if err != nil {
		return nudgeOutcomeReport{}, false, err
	}
	if !ok {
		return nudgeOutcomeReport{}, false, nil
	}
	return nudgeOutcomeReport{
		NudgeID:   nudgeID,
		Outcome:   nudgequeue.OutcomeForState(shadow.State),
		State:     shadow.State,
		Reason:    shadowNudgeReason(shadow),
		Agent:     shadow.Agent,
		SessionID: shadow.SessionID,
		Source:    shadow.Source,
		Message:   shadow.Message,
	}, true, nil
}

// shadowNudgeReason picks the most specific explanation the shadow bead
// carries. TerminalReason is the controller-supplied cause (e.g. wait-canceled);
// CloseReason is the canonical per-state fallback stamped for every
// terminalization, so it is only used when no specific cause was recorded.
func shadowNudgeReason(shadow nudgequeue.NudgeShadow) string {
	if reason := strings.TrimSpace(shadow.TerminalReason); reason != "" {
		return reason
	}
	return strings.TrimSpace(shadow.CloseReason)
}

func loadNudgeQueueState(cityPath string) (nudgeQueueState, error) {
	return nudgequeue.LoadState(cityPath)
}

func renderNudgeOutcome(cityPath string, report nudgeOutcomeReport, jsonOutput bool, stdout, stderr io.Writer) int {
	if jsonOutput {
		return writeCLIJSONLineOrExit(stdout, stderr, "gc nudge show", nudgeShowJSON{
			SchemaVersion: "1",
			Command:       "nudge show",
			CityPath:      cityPath,
			NudgeID:       report.NudgeID,
			Outcome:       report.Outcome,
			State:         report.State,
			Reason:        report.Reason,
			Agent:         report.Agent,
			SessionID:     report.SessionID,
			Source:        report.Source,
			Message:       report.Message,
		})
	}
	fmt.Fprintf(stdout, "Nudge:    %s\n", report.NudgeID) //nolint:errcheck
	fmt.Fprintf(stdout, "Outcome:  %s\n", report.Outcome) //nolint:errcheck
	fmt.Fprintf(stdout, "State:    %s\n", report.State)   //nolint:errcheck
	if report.Reason != "" {
		fmt.Fprintf(stdout, "Reason:   %s\n", report.Reason) //nolint:errcheck
	}
	if report.Agent != "" {
		fmt.Fprintf(stdout, "Agent:    %s\n", report.Agent) //nolint:errcheck
	}
	if report.SessionID != "" {
		fmt.Fprintf(stdout, "Session:  %s\n", report.SessionID) //nolint:errcheck
	}
	if report.Source != "" {
		fmt.Fprintf(stdout, "Source:   %s\n", report.Source) //nolint:errcheck
	}
	if report.Message != "" {
		fmt.Fprintf(stdout, "Message:  %s\n", report.Message) //nolint:errcheck
	}
	return 0
}
