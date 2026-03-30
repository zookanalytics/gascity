package api

import (
	"net/http"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// workflowDeleteStoreInfo groups a bead store with its scope for the delete handler.
type workflowDeleteStoreInfo struct {
	scopeKind string
	scopeRef  string
	store     beads.Store
}

// handleWorkflowDelete closes (and optionally deletes) all beads in a workflow.
//
// DELETE /v0/workflow/{workflow_id}?scope_kind=&scope_ref=&delete=true
func (s *Server) handleWorkflowDelete(w http.ResponseWriter, r *http.Request) {
	workflowID := strings.TrimSpace(r.PathValue("workflow_id"))
	if workflowID == "" {
		writeError(w, http.StatusBadRequest, "invalid", "workflow_id is required")
		return
	}

	q := r.URL.Query()
	scopeKind := strings.TrimSpace(q.Get("scope_kind"))
	scopeRef := strings.TrimSpace(q.Get("scope_ref"))
	deleteFromStore := q.Get("delete") == "true"

	stores := workflowDeleteStores(s.state)

	closed := 0
	deleted := 0
	found := false

	for _, info := range stores {
		if info.store == nil {
			continue
		}
		// Skip stores that don't match the requested scope.
		if scopeKind != "" && info.scopeKind != scopeKind {
			continue
		}
		if scopeRef != "" && info.scopeRef != scopeRef {
			continue
		}

		all, err := info.store.List()
		if err != nil {
			continue
		}

		var ids []string
		for _, b := range all {
			if b.ID == workflowID || b.Metadata["gc.root_bead_id"] == workflowID {
				ids = append(ids, b.ID)
			}
		}
		if len(ids) == 0 {
			continue
		}
		found = true

		// Phase 1: Batch close all open beads.
		n, _ := info.store.CloseAll(ids, map[string]string{"gc.outcome": "skipped"})
		closed += n

		// Phase 2: Delete if requested.
		if deleteFromStore {
			for _, id := range ids {
				// Remove deps before delete.
				if deps, err := info.store.DepList(id, "down"); err == nil {
					for _, dep := range deps {
						_ = info.store.DepRemove(id, dep.DependsOnID)
					}
				}
				if deps, err := info.store.DepList(id, "up"); err == nil {
					for _, dep := range deps {
						_ = info.store.DepRemove(dep.IssueID, id)
					}
				}
				deleted++
			}
		}
	}

	if !found {
		writeError(w, http.StatusNotFound, "not_found", "workflow "+workflowID+" not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"workflow_id": workflowID,
		"closed":      closed,
		"deleted":     deleted,
	})
}

// workflowDeleteStores returns city + rig stores for delete operations.
func workflowDeleteStores(state State) []workflowDeleteStoreInfo {
	beadStores := state.BeadStores()
	stores := make([]workflowDeleteStoreInfo, 0, len(beadStores)+1)
	cityName := state.CityName()
	if cityName == "" {
		cityName = "city"
	}

	if cityStore := state.CityBeadStore(); cityStore != nil {
		stores = append(stores, workflowDeleteStoreInfo{
			scopeKind: "city",
			scopeRef:  cityName,
			store:     cityStore,
		})
	}

	for _, rigName := range sortedRigNames(beadStores) {
		if rigName == cityName {
			continue
		}
		store := state.BeadStore(rigName)
		if store == nil {
			continue
		}
		stores = append(stores, workflowDeleteStoreInfo{
			scopeKind: "rig",
			scopeRef:  rigName,
			store:     store,
		})
	}

	return stores
}
