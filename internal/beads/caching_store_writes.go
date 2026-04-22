package beads

import (
	"errors"
	"fmt"
	"time"
)

// Create passes through to the backing store and updates the cache.
func (c *CachingStore) Create(b Bead) (Bead, error) {
	created, err := c.backing.Create(b)
	if err != nil {
		return created, err
	}

	c.mu.Lock()
	c.mutationSeq++
	c.beads[created.ID] = cloneBead(created)
	delete(c.dirty, created.ID)
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()

	c.notifyChange("bead.created", created)
	return created, nil
}

// Update passes through to the backing store and refreshes the cache.
func (c *CachingStore) Update(id string, opts UpdateOpts) error {
	if err := c.backing.Update(id, opts); err != nil {
		return err
	}

	// Re-fetch from backing to get the authoritative state.
	fresh, err := c.backing.Get(id)
	if err != nil {
		c.mu.Lock()
		c.dirty[id] = struct{}{}
		c.mu.Unlock()
		c.recordProblem("refresh bead after update", fmt.Errorf("%s: %w", id, err))
		return nil
	}

	fresh = applyUpdateOptsToBead(fresh, opts)

	c.mu.Lock()
	c.mutationSeq++
	c.beads[id] = cloneBead(fresh)
	delete(c.dirty, id)
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()

	c.notifyChange("bead.updated", fresh)
	return nil
}

func applyUpdateOptsToBead(b Bead, opts UpdateOpts) Bead {
	b = cloneBead(b)
	if opts.Title != nil {
		b.Title = *opts.Title
	}
	if opts.Status != nil {
		b.Status = *opts.Status
	}
	if opts.Type != nil {
		b.Type = *opts.Type
	}
	if opts.Priority != nil {
		b.Priority = cloneIntPtr(opts.Priority)
	}
	if opts.Description != nil {
		b.Description = *opts.Description
	}
	if opts.ParentID != nil {
		b.ParentID = *opts.ParentID
	}
	if opts.Assignee != nil {
		b.Assignee = *opts.Assignee
	}
	if len(opts.Metadata) > 0 {
		if b.Metadata == nil {
			b.Metadata = make(map[string]string, len(opts.Metadata))
		}
		for k, v := range opts.Metadata {
			b.Metadata[k] = v
		}
	}
	if len(opts.Labels) > 0 {
		b.Labels = append(b.Labels, opts.Labels...)
	}
	if len(opts.RemoveLabels) > 0 {
		remove := make(map[string]bool, len(opts.RemoveLabels))
		for _, label := range opts.RemoveLabels {
			remove[label] = true
		}
		kept := b.Labels[:0]
		for _, label := range b.Labels {
			if !remove[label] {
				kept = append(kept, label)
			}
		}
		b.Labels = kept
	}
	return b
}

// Close marks a bead as closed in the backing store and cache.
func (c *CachingStore) Close(id string) error {
	if err := c.backing.Close(id); err != nil {
		return err
	}

	var closed Bead
	var found bool
	if fresh, err := c.backing.Get(id); err == nil {
		closed = fresh
		closed.Status = "closed"
		found = true
	} else if !errors.Is(err, ErrNotFound) {
		c.recordProblem("refresh bead after close", fmt.Errorf("%s: %w", id, err))
	}

	c.mu.Lock()
	c.mutationSeq++
	if b, ok := c.beads[id]; ok {
		b.Status = "closed"
		c.beads[id] = b
		delete(c.dirty, id)
		closed = cloneBead(b)
		found = true
		c.markFreshLocked(time.Now())
		c.updateStatsLocked()
	} else if found {
		c.beads[id] = cloneBead(closed)
		delete(c.dirty, id)
		c.markFreshLocked(time.Now())
		c.updateStatsLocked()
	}
	c.mu.Unlock()

	if found {
		c.notifyChange("bead.closed", closed)
	}
	return nil
}

// CloseAll closes multiple beads and sets metadata on each.
func (c *CachingStore) CloseAll(ids []string, metadata map[string]string) (int, error) {
	n, err := c.backing.CloseAll(ids, metadata)
	if err != nil && n == 0 {
		return n, err
	}

	type refreshedBead struct {
		id   string
		bead Bead
	}
	refreshed := make([]refreshedBead, 0, len(ids))
	var refreshErr error
	refreshFailed := make(map[string]struct{})
	for _, id := range ids {
		fresh, getErr := c.backing.Get(id)
		if getErr != nil {
			refreshFailed[id] = struct{}{}
			refreshErr = errors.Join(refreshErr, fmt.Errorf("refresh bead after close-all %s: %w", id, getErr))
			continue
		}
		refreshed = append(refreshed, refreshedBead{id: id, bead: fresh})
	}

	notifications := make([]cacheNotification, 0, len(refreshed))
	c.mu.Lock()
	c.mutationSeq++
	if refreshErr != nil {
		c.recordProblemLocked("close-all refresh", refreshErr)
	}
	for id := range refreshFailed {
		c.dirty[id] = struct{}{}
	}
	for _, item := range refreshed {
		previous, hadPrevious := c.beads[item.id]
		c.beads[item.id] = cloneBead(item.bead)
		delete(c.dirty, item.id)
		if item.bead.Status == "closed" {
			delete(c.deps, item.id)
		}
		if hadPrevious && previous.Status != "closed" && item.bead.Status == "closed" {
			notifications = append(notifications, cacheNotification{
				eventType: "bead.closed",
				bead:      cloneBead(item.bead),
			})
		}
	}
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	c.notifyChanges(notifications)
	return n, errors.Join(err, refreshErr)
}

// SetMetadata sets a single metadata key-value on a bead.
func (c *CachingStore) SetMetadata(id, key, value string) error {
	if err := c.backing.SetMetadata(id, key, value); err != nil {
		return err
	}

	c.mu.Lock()
	c.mutationSeq++
	if b, ok := c.beads[id]; ok {
		if b.Metadata == nil {
			b.Metadata = make(map[string]string)
		}
		b.Metadata[key] = value
		c.beads[id] = b
		delete(c.dirty, id)
	}
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	return nil
}

// SetMetadataBatch sets multiple metadata key-values on a bead.
func (c *CachingStore) SetMetadataBatch(id string, kvs map[string]string) error {
	if err := c.backing.SetMetadataBatch(id, kvs); err != nil {
		return err
	}

	c.mu.Lock()
	c.mutationSeq++
	if b, ok := c.beads[id]; ok {
		if b.Metadata == nil {
			b.Metadata = make(map[string]string, len(kvs))
		}
		for k, v := range kvs {
			b.Metadata[k] = v
		}
		c.beads[id] = b
		delete(c.dirty, id)
	}
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	return nil
}

// DepAdd adds a dependency and updates the cache.
func (c *CachingStore) DepAdd(issueID, dependsOnID, depType string) error {
	if err := c.backing.DepAdd(issueID, dependsOnID, depType); err != nil {
		return err
	}

	c.mu.Lock()
	c.mutationSeq++
	deps := c.deps[issueID]
	for i, d := range deps {
		if d.DependsOnID == dependsOnID {
			deps[i].Type = depType
			c.deps[issueID] = deps
			delete(c.dirty, issueID)
			c.markFreshLocked(time.Now())
			c.updateStatsLocked()
			c.mu.Unlock()
			return nil
		}
	}
	c.deps[issueID] = append(deps, Dep{IssueID: issueID, DependsOnID: dependsOnID, Type: depType})
	delete(c.dirty, issueID)
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	return nil
}

// DepRemove removes a dependency and updates the cache.
func (c *CachingStore) DepRemove(issueID, dependsOnID string) error {
	if err := c.backing.DepRemove(issueID, dependsOnID); err != nil {
		return err
	}

	c.mu.Lock()
	c.mutationSeq++
	deps := c.deps[issueID]
	for i, d := range deps {
		if d.DependsOnID == dependsOnID {
			c.deps[issueID] = append(deps[:i], deps[i+1:]...)
			delete(c.dirty, issueID)
			break
		}
	}
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	return nil
}

// Delete passes through to the backing store and removes from cache.
func (c *CachingStore) Delete(id string) error {
	if err := c.backing.Delete(id); err != nil {
		return err
	}

	c.mu.Lock()
	c.mutationSeq++
	delete(c.beads, id)
	delete(c.deps, id)
	delete(c.dirty, id)
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	return nil
}
