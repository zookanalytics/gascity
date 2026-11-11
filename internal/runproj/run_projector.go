package runproj

import (
	"cmp"
	"context"
	"slices"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
)

// RunProjector folds bead lifecycle events into the run view and keeps its
// summary current as it folds. It holds only the beads RunBeadFilter admits,
// grouped by run root, and Summary rebuilds the lane of a run only when an
// event changed one of its beads, so a fold costs work in proportion to what
// it changed rather than to the city's whole history. The per-city run tailer
// drives one: a ColdLoad over the retained event history, then Apply of each
// newly tailed batch.
//
// Its results are the full builders' over the filtered fold: Beads is
// FilterRunBeads over a Projector's Beads for the same events, and Summary is
// BuildRunSummaryWithAllLanes and CountCanonicalRunStatuses over Beads.
//
// A RunProjector is not safe for concurrent use. The beads and lanes it
// returns are never mutated afterwards, so a caller may publish them to
// concurrent readers.
type RunProjector struct {
	lastSeq      uint64
	decodeMisses int

	// seen holds the first-seen position of every bead id in the fold, run
	// bead or not, so a bead that starts to pass RunBeadFilter keeps the place
	// a full fold gives it. A delete forgets the id.
	seen    map[string]uint64
	nextPos uint64

	beads map[string]projectedRunBead
	// order lists the run beads by first-seen position.
	order []runBeadPosition

	// groups holds every run root's group of beads, run or not; runs holds
	// the groups that form a run. dirty names the roots whose group changed
	// since the last Summary.
	groups map[string]*runGroup
	runs   map[string]*runGroup
	dirty  map[string]struct{}

	// started counts, per run id, the started beads that count toward it
	// (startedRunRefs), for the census.
	started map[string]int

	// keys interns metadata keys: the run beads share a small key vocabulary,
	// and one string per distinct key keeps the fold's live heap from holding
	// a copy of every key on every bead.
	keys map[string]string

	// laneRefreshes counts the lanes Summary has rebuilt, so a test can prove
	// the rebuild is incremental.
	laneRefreshes int
}

type projectedRunBead struct {
	bead beads.Bead
	pos  uint64
	root string
}

type runBeadPosition struct {
	id  string
	pos uint64
}

// runGroup is the beads one run root groups. run is the derivation Summary
// rebuilds when the group changes, nil while the group is not a run.
type runGroup struct {
	members []string
	// firstPos is the earliest first-seen position among the members: the
	// group's place in a full build's grouping order.
	firstPos uint64
	run      *runDerivation
}

// runDerivation is what a run's group derives: its lane, the lane's sort time,
// and its newest member changes, at most recentChangesCap, newest first.
type runDerivation struct {
	lane    RunLane
	updated int64
	recent  []recentMember
}

type recentMember struct {
	id     string
	millis int64
	pos    uint64
}

// noFeedScopes is the empty feed-scope map a build without WithFeedScopes uses.
var noFeedScopes = map[string]RunFeedScope{}

// NewRunProjector returns an empty run projector.
func NewRunProjector() *RunProjector {
	return &RunProjector{
		seen:    make(map[string]uint64),
		beads:   make(map[string]projectedRunBead),
		groups:  make(map[string]*runGroup),
		runs:    make(map[string]*runGroup),
		dirty:   make(map[string]struct{}),
		started: make(map[string]int),
		keys:    make(map[string]string),
	}
}

// ColdLoad folds the event log's whole retained history at path: its
// archives, any in-flight events.jsonl.rotating-* segment and the active log
// (events.ReadFilteredHistoryEach). It folds each event as it reads it, so the
// replay holds the fold, never the history. On an error, including ctx's, the
// projector keeps the events folded before it. Call it once, on a fresh
// projector, before the incremental tail begins; Apply is seq-idempotent, so a
// tail that re-reads events the replay folded changes nothing.
func (p *RunProjector) ColdLoad(ctx context.Context, path string) error {
	return events.ReadFilteredHistoryEach(ctx, path, events.Filter{}, func(e events.Event) error {
		p.apply(&e)
		return nil
	})
}

// Apply folds a chronological event slice and reports whether a run bead
// changed, so the caller can skip a rebuild when only other beads or non-bead
// events arrived. It advances the cursor past every event, and counts a
// bead.* event whose payload does not decode (DecodeMisses) rather than
// dropping it silently.
func (p *RunProjector) Apply(evts []events.Event) (changed bool) {
	for i := range evts {
		if p.apply(&evts[i]) {
			changed = true
		}
	}
	return changed
}

func (p *RunProjector) apply(e *events.Event) bool {
	if e.Seq > p.lastSeq {
		p.lastSeq = e.Seq
	}
	if !beadEventTypes[e.Type] {
		return false
	}
	b, ok := decodeBead(*e)
	if !ok {
		p.decodeMisses++
		return false
	}
	if e.Type == events.BeadDeleted {
		delete(p.seen, b.ID)
		return p.drop(b.ID)
	}
	pos, known := p.seen[b.ID]
	if !known {
		pos = p.nextPos
		p.nextPos++
		p.seen[b.ID] = pos
	}
	if !RunBeadFilter(b) {
		return p.drop(b.ID)
	}
	if prior, ok := p.beads[b.ID]; ok {
		p.detach(b.ID, prior)
	} else {
		p.insertOrder(b.ID, pos)
	}
	b.Metadata = p.internKeys(b.Metadata)
	p.attach(b, pos)
	return true
}

// drop removes a run bead from the projection and reports whether there was
// one.
func (p *RunProjector) drop(id string) bool {
	prior, ok := p.beads[id]
	if !ok {
		return false
	}
	p.detach(id, prior)
	delete(p.beads, id)
	p.removeOrder(id, prior.pos)
	return true
}

// attach records b as a run bead and adds it to its run root's group.
func (p *RunProjector) attach(b beads.Bead, pos uint64) {
	root := runRootID(fromBead(b))
	p.beads[b.ID] = projectedRunBead{bead: b, pos: pos, root: root}
	g := p.groups[root]
	if g == nil {
		g = &runGroup{}
		p.groups[root] = g
	}
	g.members = append(g.members, b.ID)
	p.dirty[root] = struct{}{}
	p.countStarted(b, 1)
}

// detach takes the prior snapshot of a run bead out of its group and the
// started counts; the caller replaces or removes the snapshot itself.
func (p *RunProjector) detach(id string, prior projectedRunBead) {
	if g := p.groups[prior.root]; g != nil {
		if i := slices.Index(g.members, id); i >= 0 {
			g.members = slices.Delete(g.members, i, i+1)
		}
	}
	p.dirty[prior.root] = struct{}{}
	p.countStarted(prior.bead, -1)
}

// countStarted adds delta to every run a started bead counts toward. No run in
// the projection has an empty id, so an empty reference is not counted.
func (p *RunProjector) countStarted(b beads.Bead, delta int) {
	for _, rootID := range startedRunRefs(b) {
		if rootID == "" {
			continue
		}
		if n := p.started[rootID] + delta; n != 0 {
			p.started[rootID] = n
		} else {
			delete(p.started, rootID)
		}
	}
}

// internKeys returns md with each key replaced by the projector's one copy of
// it.
func (p *RunProjector) internKeys(md beads.StringMap) beads.StringMap {
	if len(md) == 0 {
		return md
	}
	out := make(beads.StringMap, len(md))
	for k, v := range md {
		key, ok := p.keys[k]
		if !ok {
			key = k
			p.keys[k] = k
		}
		out[key] = v
	}
	return out
}

func (p *RunProjector) orderIndex(pos uint64) (int, bool) {
	return slices.BinarySearchFunc(p.order, pos, func(o runBeadPosition, target uint64) int {
		return cmp.Compare(o.pos, target)
	})
}

func (p *RunProjector) insertOrder(id string, pos uint64) {
	if n := len(p.order); n == 0 || p.order[n-1].pos < pos {
		p.order = append(p.order, runBeadPosition{id: id, pos: pos})
		return
	}
	i, _ := p.orderIndex(pos)
	p.order = slices.Insert(p.order, i, runBeadPosition{id: id, pos: pos})
}

func (p *RunProjector) removeOrder(id string, pos uint64) {
	if i, found := p.orderIndex(pos); found && p.order[i].id == id {
		p.order = slices.Delete(p.order, i, i+1)
	}
}

// Beads returns the run beads in first-seen order.
func (p *RunProjector) Beads() []beads.Bead {
	out := make([]beads.Bead, 0, len(p.order))
	for _, o := range p.order {
		out = append(out, p.beads[o.id].bead)
	}
	return out
}

// LastSeq returns the highest event seq applied: the cursor a live tail
// resumes from.
func (p *RunProjector) LastSeq() uint64 { return p.lastSeq }

// DecodeMisses returns the cumulative count of bead.* events whose payload did
// not decode to a bead with an id.
func (p *RunProjector) DecodeMisses() int { return p.decodeMisses }

// Summary returns the run summary and the canonical status census of every
// run. It rebuilds the lane of each run whose beads changed since the last
// call and reuses every other run's lane.
func (p *RunProjector) Summary() (RunSummary, CanonicalRunStatusCounts) {
	for root := range p.dirty {
		p.refresh(root)
	}
	clear(p.dirty)

	groups := make([]*runGroup, 0, len(p.runs))
	for _, g := range p.runs {
		groups = append(groups, g)
	}
	slices.SortFunc(groups, func(a, b *runGroup) int {
		return compareLaneKeys(a.run.updated, a.run.lane.ID, b.run.updated, b.run.lane.ID)
	})
	sortedLanes := make([]RunLane, len(groups))
	var census CanonicalRunStatusCounts
	for i, g := range groups {
		lane := g.run.lane
		sortedLanes[i] = lane
		var root *beads.Bead
		if rb, ok := p.beads[lane.ID]; ok {
			root = &rb.bead
		}
		census.add(CanonicalRunStatusForLane(lane, root, p.started[lane.ID]))
	}
	return summarizeLanes(sortedLanes, p.recentChanges(groups)), census
}

// refresh rebuilds the derivation of root's group, as buildAllRunLanes derives
// it: the members in first-seen order, the run test, and the lane.
func (p *RunProjector) refresh(root string) {
	g := p.groups[root]
	if g == nil {
		return
	}
	if len(g.members) == 0 {
		delete(p.groups, root)
		delete(p.runs, root)
		return
	}
	slices.SortFunc(g.members, func(a, b string) int {
		return cmp.Compare(p.beads[a].pos, p.beads[b].pos)
	})
	g.firstPos = p.beads[g.members[0]].pos
	issues := make([]runIssue, len(g.members))
	for i, id := range g.members {
		issues[i] = fromBead(p.beads[id].bead)
	}
	if isDanglingRootGroup(root, issues) || !isRunGroup(root, issues) {
		g.run = nil
		delete(p.runs, root)
		return
	}
	p.laneRefreshes++
	run := &runDerivation{lane: runLane(root, issues, noFeedScopes)}
	run.updated = laneUpdatedMillis(run.lane)
	for i, issue := range issues {
		if issue.updatedAt == "" {
			continue
		}
		run.recent = append(run.recent, recentMember{id: issue.id, millis: parseTimestamp(issue.updatedAt), pos: p.beads[g.members[i]].pos})
	}
	slices.SortFunc(run.recent, compareRecentMembers)
	if len(run.recent) > recentChangesCap {
		run.recent = slices.Clone(run.recent[:recentChangesCap])
	}
	g.run = run
	p.runs[root] = g
}

// compareRecentMembers orders a group's changes as recentChanges does: newest
// first, the earlier-seen member first on a tie.
func compareRecentMembers(a, b recentMember) int {
	if c := cmp.Compare(b.millis, a.millis); c != 0 {
		return c
	}
	return cmp.Compare(a.pos, b.pos)
}

// recentChanges returns the newest recentChangesCap member changes across the
// runs. recentChanges breaks a tie on time by the order a full build lists the
// lanes' beads in, its groups in first-seen order and each group's members in
// first-seen order; firstPos and pos give the same order.
func (p *RunProjector) recentChanges(groups []*runGroup) []RunChange {
	type candidate struct {
		member   recentMember
		groupPos uint64
	}
	compare := func(a, b candidate) int {
		if c := cmp.Compare(b.member.millis, a.member.millis); c != 0 {
			return c
		}
		if c := cmp.Compare(a.groupPos, b.groupPos); c != 0 {
			return c
		}
		return cmp.Compare(a.member.pos, b.member.pos)
	}
	top := make([]candidate, 0, recentChangesCap+1)
	for _, g := range groups {
		for _, m := range g.run.recent {
			c := candidate{member: m, groupPos: g.firstPos}
			if len(top) == recentChangesCap && compare(c, top[len(top)-1]) >= 0 {
				break
			}
			i, _ := slices.BinarySearchFunc(top, c, compare)
			top = slices.Insert(top, i, c)
			if len(top) > recentChangesCap {
				top = top[:recentChangesCap]
			}
		}
	}
	out := make([]RunChange, 0, len(top))
	for _, c := range top {
		issue := fromBead(p.beads[c.member.id].bead)
		out = append(out, RunChange{ID: issue.id, Title: issue.title, Status: issue.status, UpdatedAt: issue.updatedAt})
	}
	return out
}
