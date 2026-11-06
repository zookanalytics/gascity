package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads/queryindex"
	"github.com/gastownhall/gascity/internal/doctor"
)

// beadStoreIndexesCheckTimeout bounds one store's catalog read and, when it
// runs, the index self-test.
const beadStoreIndexesCheckTimeout = 45 * time.Second

// beadStoreIndexesCheck reports, for one bead store whose Dolt server gc
// owns, the query indexes the store lacks, the metadata indexes held back
// because the server fails the index self-test, and any metadata index the
// store carries on such a server. Reaching a verdict runs the self-test, which
// writes scratch tables in the store's database that dolt_ignore keeps out of
// its history, and drops them; one doctor run tests each Dolt version once.
type beadStoreIndexesCheck struct {
	cityPath  string
	scopeRoot string
	label     string
	keys      []string
	verdicts  *queryindex.Verdicts
}

func newBeadStoreIndexesCheck(cityPath, scopeRoot, label string, keys []string, verdicts *queryindex.Verdicts) *beadStoreIndexesCheck {
	return &beadStoreIndexesCheck{cityPath: cityPath, scopeRoot: scopeRoot, label: label, keys: keys, verdicts: verdicts}
}

func (c *beadStoreIndexesCheck) Name() string { return "bead-store-indexes:" + c.label }

func (c *beadStoreIndexesCheck) CanFix() bool { return false }

// Fix is a no-op: the controller builds missing indexes once their tables
// are quiet, and dropping an exposed index stalls the store's writes for as
// long as a build, which is the operator's call to schedule.
func (c *beadStoreIndexesCheck) Fix(_ *doctor.CheckContext) error { return nil }

func (c *beadStoreIndexesCheck) WarmupEligible() bool { return false }

func (c *beadStoreIndexesCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	store, ok, err := beadStoreIndexStore(c.cityPath, c.scopeRoot, c.label)
	if err == nil && !ok {
		return &doctor.CheckResult{
			Name: c.Name(), Severity: doctor.SeverityAdvisory, Status: doctor.StatusOK,
			Message: fmt.Sprintf("query indexes not checked for %s: gc has no direct connection to its Dolt server", c.label),
		}
	}
	var status queryindex.Status
	if err == nil {
		var want []queryindex.Index
		want, err = queryindex.Expected(c.keys)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), beadStoreIndexesCheckTimeout)
			status, err = queryindex.CheckStore(ctx, store, want, c.verdicts)
			cancel()
		}
	}
	return beadStoreIndexesResult(c.Name(), c.label, status, err)
}

// beadStoreIndexesResult renders a store's index status as a check result.
func beadStoreIndexesResult(name, label string, status queryindex.Status, err error) *doctor.CheckResult {
	res := &doctor.CheckResult{Name: name, Severity: doctor.SeverityAdvisory}
	if err != nil {
		res.Status = doctor.StatusWarning
		res.Message = fmt.Sprintf("query indexes unknown for %s: %v", label, err)
		return res
	}
	if len(status.Exposed) > 0 {
		drops := make([]string, len(status.Exposed))
		for i, ix := range status.Exposed {
			drops[i] = fmt.Sprintf("DROP INDEX `%s` ON `%s`;", ix.Name, ix.Table)
		}
		res.Status = doctor.StatusError
		res.Message = fmt.Sprintf("%s carries %s on Dolt %s, which fails the index self-test: %s",
			label, describeIndexes(status.Exposed), status.Version, status.Verdict.Failure)
		res.FixHint = "Writes to these tables can fail or be stored wrong on this server. Drop the indexes while the store is idle; " +
			"each drop takes as long as a build: " + strings.Join(drops, " ")
		return res
	}
	held := ""
	if len(status.Held) > 0 {
		held = fmt.Sprintf("; held back on Dolt %s, which fails the index self-test (%s): %s",
			status.Version, status.Verdict.Failure, describeIndexes(status.Held))
	}
	if len(status.Missing) > 0 {
		res.Status = doctor.StatusWarning
		res.Message = fmt.Sprintf("%s lacks %s%s", label, describeIndexes(status.Missing), held)
		res.FixHint = "The controller builds missing indexes one at a time, once a table has been quiet for 30s; " +
			"its log reports each build on a bead-store-index line."
		return res
	}
	res.Status = doctor.StatusOK
	res.Message = fmt.Sprintf("%s carries its query indexes%s", label, held)
	return res
}

// describeIndexes lists indexes by what they cover.
func describeIndexes(indexes []queryindex.Index) string {
	names := make([]string, len(indexes))
	for i, ix := range indexes {
		names[i] = ix.String()
	}
	noun := "index"
	if len(indexes) != 1 {
		noun = "indexes"
	}
	return fmt.Sprintf("%d query %s (%s)", len(indexes), noun, strings.Join(names, ", "))
}
