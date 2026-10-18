package beads_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// briefRecordingRunner answers bd list, bd ready and bd query with rows that
// carry text, and records every argv. When refuse is set it answers any
// invocation carrying --brief the way bd 1.0.4 does.
type briefRecordingRunner struct {
	mu     sync.Mutex
	calls  [][]string
	refuse error
}

const briefRunnerRows = `[{"id":"bd-1","title":"one","status":"open","issue_type":"task","created_at":"2025-01-15T10:30:00Z",` +
	`"description":"text the caller never reads","assignee":"worker","labels":["core"],"metadata":{"route":"pool"}}]`

func (r *briefRecordingRunner) run(_, name string, args ...string) ([]byte, error) {
	r.mu.Lock()
	r.calls = append(r.calls, append([]string{name}, args...))
	r.mu.Unlock()
	if r.refuse != nil && slices.Contains(args, "--brief") {
		return nil, r.refuse
	}
	switch args[0] {
	case "list", "ready":
		return []byte(briefRunnerRows), nil
	case "query":
		return []byte(`[]`), nil
	}
	return nil, errors.New("unexpected command: " + strings.Join(args, " "))
}

func (r *briefRecordingRunner) invocations(verb string) [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]string
	for _, call := range r.calls {
		if len(call) > 1 && call[1] == verb {
			out = append(out, call)
		}
	}
	return out
}

// errBd104BriefRefusal is the error a runner returns for bd 1.0.4's answer to
// --brief: exit 1 with the cobra usage error on stderr.
var errBd104BriefRefusal = errors.New("exit status 1: Error: unknown flag: --brief\nUsage:\n  bd ready [flags]")

func TestBdStoreCoreReadyAsksBdForBriefRows(t *testing.T) {
	runner := &briefRecordingRunner{}
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.Ready(beads.ReadyQuery{TierMode: beads.TierBoth, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	ready := runner.invocations("ready")
	if len(ready) != 1 {
		t.Fatalf("bd ready ran %d times, want 1: %v", len(ready), ready)
	}
	if want := []string{"bd", "ready", "--json", "--include-ephemeral", "--limit", "5000", "--brief"}; !slices.Equal(ready[0], want) {
		t.Fatalf("bd ready argv = %v, want %v", ready[0], want)
	}
	if len(got) != 1 || got[0].ID != "bd-1" {
		t.Fatalf("Ready = %+v, want bd-1", got)
	}
	if got[0].Description != "" {
		t.Errorf("Description = %q, want empty on a core read", got[0].Description)
	}
	if got[0].Assignee != "worker" || got[0].Metadata["route"] != "pool" || !slices.Equal(got[0].Labels, []string{"core"}) {
		t.Errorf("core row lost a field it must keep: %+v", got[0])
	}
}

func TestBdStoreFullReadsDoNotAskForBriefRows(t *testing.T) {
	runner := &briefRecordingRunner{}
	s := beads.NewBdStore(t.TempDir(), runner.run)

	ready, err := s.Ready(beads.ReadyQuery{TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range runner.calls {
		if slices.Contains(call, "--brief") {
			t.Errorf("a full read sent %v", call)
		}
	}
	if len(ready) != 1 || ready[0].Description == "" || len(listed) != 1 || listed[0].Description == "" {
		t.Fatalf("full reads lost the text: ready=%+v listed=%+v", ready, listed)
	}
}

func TestBdStoreCoreListAsksBdForBriefRows(t *testing.T) {
	runner := &briefRecordingRunner{}
	s := beads.NewBdStore(t.TempDir(), runner.run)

	got, err := s.List(beads.ListQuery{Label: "core", IncludeClosed: true, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	list := runner.invocations("list")
	if len(list) != 1 || !slices.Contains(list[0], "--brief") {
		t.Fatalf("bd list argv = %v, want one call carrying --brief", list)
	}
	if len(got) != 1 || got[0].Description != "" {
		t.Fatalf("List = %+v, want bd-1 with no description", got)
	}
}

// A TierBoth list reads the wisp plane through bd query, which takes no
// --brief, so the store clears the text of those rows itself.
func TestBdStoreCoreListClearsTheWispLegText(t *testing.T) {
	runner := func(_, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "list":
			return []byte(`[]`), nil
		case "query":
			if slices.Contains(args, "--brief") {
				return nil, errors.New("exit status 1: Error: unknown flag: --brief")
			}
			return []byte(`[{"id":"bd-wisp-1","title":"wisp","status":"open","issue_type":"task","created_at":"2025-01-15T10:30:00Z","description":"wisp text"}]`), nil
		}
		return nil, errors.New("unexpected command: " + strings.Join(args, " "))
	}
	s := beads.NewBdStore(t.TempDir(), runner)

	got, err := s.List(beads.ListQuery{AllowScan: true, TierMode: beads.TierBoth, Fields: beads.FieldsCore})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "bd-wisp-1" {
		t.Fatalf("List = %+v, want the wisp", got)
	}
	if got[0].Description != "" {
		t.Errorf("wisp leg Description = %q, want empty on a core read", got[0].Description)
	}
}

// A bd older than 1.2.0 refuses --brief. The read is answered by a whole-row
// read, and the refusal is remembered for the scope, so a store built later
// over the same scope (the control dispatcher builds one per scan) does not
// spend a refused subprocess again.
func TestBdStoreBriefRefusalReadsWholeRowsAndLatchesForTheScope(t *testing.T) {
	dir := t.TempDir()
	runner := &briefRecordingRunner{refuse: errBd104BriefRefusal}

	got, err := beads.NewBdStore(dir, runner.run).Ready(beads.ReadyQuery{Fields: beads.FieldsCore})
	if err != nil {
		t.Fatalf("Ready on a bd that refuses --brief: %v", err)
	}
	if len(got) != 1 || got[0].Description != "" {
		t.Fatalf("Ready = %+v, want bd-1 with its text cleared", got)
	}
	first := runner.invocations("ready")
	if len(first) != 2 || !slices.Contains(first[0], "--brief") || slices.Contains(first[1], "--brief") {
		t.Fatalf("bd ready calls = %v, want one refused --brief call and one whole-row retry", first)
	}

	if _, err := beads.NewBdStore(dir, runner.run).List(beads.ListQuery{AllowScan: true, Fields: beads.FieldsCore}); err != nil {
		t.Fatal(err)
	}
	if _, err := beads.NewBdStore(dir, runner.run).Ready(beads.ReadyQuery{Fields: beads.FieldsCore}); err != nil {
		t.Fatal(err)
	}
	for _, call := range runner.calls[2:] {
		if slices.Contains(call, "--brief") {
			t.Errorf("a store over a scope whose bd refused --brief sent %v", call)
		}
	}
	if n := len(runner.calls); n != 4 {
		t.Errorf("bd ran %d times, want 4: one refusal, then one call per read", n)
	}

	other := &briefRecordingRunner{}
	if _, err := beads.NewBdStore(t.TempDir(), other.run).Ready(beads.ReadyQuery{Fields: beads.FieldsCore}); err != nil {
		t.Fatal(err)
	}
	if calls := other.invocations("ready"); len(calls) != 1 || !slices.Contains(calls[0], "--brief") {
		t.Errorf("another scope's read = %v, want --brief: the refusal belongs to its own scope", calls)
	}
}

// Only bd's own refusal of --brief latches. A capable bd's usage echo lists
// --brief among its flags on any flag error, so an error about another flag
// must reach the caller and leave the scope asking for brief rows.
func TestBdStoreBriefLatchNeedsBdToRefuseThatFlag(t *testing.T) {
	dir := t.TempDir()
	otherFlag := errors.New("exit status 1: Error: unknown flag: --nope\nUsage:\n  bd ready [flags]\n\nFlags:\n      --brief   Omit the free-form text")
	runner := &briefRecordingRunner{refuse: otherFlag}

	if _, err := beads.NewBdStore(dir, runner.run).Ready(beads.ReadyQuery{Fields: beads.FieldsCore}); err == nil {
		t.Fatal("Ready succeeded, want the bd error")
	}
	if calls := runner.invocations("ready"); len(calls) != 1 {
		t.Fatalf("bd ready calls = %v, want exactly the failed one: an unrelated error must not trigger a retry", calls)
	}

	runner.refuse = nil
	if _, err := beads.NewBdStore(dir, runner.run).Ready(beads.ReadyQuery{Fields: beads.FieldsCore}); err != nil {
		t.Fatal(err)
	}
	calls := runner.invocations("ready")
	if last := calls[len(calls)-1]; !slices.Contains(last, "--brief") {
		t.Errorf("the next read sent %v, want --brief: an unrelated flag error must not latch the scope", last)
	}
}

// The work-outcome veto reads a blocker's status and metadata, never its text,
// so its blocker lookup asks bd for brief rows whatever the caller's field set.
func TestBdStoreReadyBlockerLookupAsksForBriefRows(t *testing.T) {
	runner, listArgs := bdStoreWorkOutcomeReadyRunner(`,"metadata":{"gc.work_outcome":"blocked"}`)
	s := beads.NewBdStore(t.TempDir(), runner)
	if _, err := s.Ready(); err != nil {
		t.Fatal(err)
	}
	if len(*listArgs) == 0 {
		t.Fatal("Ready() never issued the blocker lookup")
	}
	for _, args := range *listArgs {
		if !strings.Contains(args, "--brief") {
			t.Errorf("blocker lookup %q does not ask for brief rows", args)
		}
	}
}
