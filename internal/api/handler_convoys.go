package api

import (
	"errors"
	"net/http"

	"github.com/gastownhall/gascity/internal/beads"
)

func (s *Server) listConvoys() []beads.Bead {
	stores := s.state.BeadStores()
	rigNames := sortedRigNames(stores)
	var convoys []beads.Bead
	for _, rigName := range rigNames {
		store := stores[rigName]
		list, err := store.List(beads.ListQuery{Type: "convoy"})
		if err != nil {
			continue
		}
		convoys = append(convoys, list...)
	}
	if convoys == nil {
		return []beads.Bead{}
	}
	return convoys
}

func (s *Server) getConvoySnapshot(id string) (convoySnapshotResponse, error) {
	stores := s.state.BeadStores()
	for _, rigName := range sortedRigNames(stores) {
		store := stores[rigName]
		b, err := store.Get(id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return convoySnapshotResponse{}, err
		}
		if b.Type != "convoy" {
			return convoySnapshotResponse{}, httpError{status: http.StatusNotFound, code: "not_found", message: "bead " + id + " is not a convoy"}
		}

		children, err := store.List(beads.ListQuery{
			ParentID:      id,
			IncludeClosed: true,
			Sort:          beads.SortCreatedAsc,
		})
		if err != nil {
			return convoySnapshotResponse{}, err
		}
		if children == nil {
			children = []beads.Bead{}
		}

		// Compute progress.
		total := len(children)
		closed := 0
		for _, c := range children {
			if c.Status == "closed" {
				closed++
			}
		}

		return convoySnapshotResponse{
			Convoy:   b,
			Children: children,
			Progress: convoyProgressResponse{Total: total, Closed: closed},
		}, nil
	}
	return convoySnapshotResponse{}, httpError{status: http.StatusNotFound, code: "not_found", message: "convoy " + id + " not found"}
}

// isGraphConvoyID checks if the bead is a formula-compiled graph convoy
// (workflow) by looking for the gc.kind=workflow marker.
func isGraphConvoyID(s *Server, id string) bool {
	stores := s.state.BeadStores()
	for _, rigName := range sortedRigNames(stores) {
		store := stores[rigName]
		b, err := store.Get(id)
		if err != nil {
			continue
		}
		return isGraphConvoyBead(b)
	}
	return false
}

// --- Shared methods for WS dispatch ---

type convoyCreateRequest struct {
	Rig   string   `json:"rig"`
	Title string   `json:"title"`
	Items []string `json:"items"`
}

func (s *Server) createConvoy(body convoyCreateRequest) (beads.Bead, error) {
	if body.Title == "" {
		return beads.Bead{}, httpError{status: 400, code: "invalid", message: "title is required"}
	}
	store := s.findStore(body.Rig)
	if store == nil {
		return beads.Bead{}, httpError{status: 400, code: "invalid", message: "rig is required when multiple rigs are configured"}
	}
	for _, itemID := range body.Items {
		if _, err := store.Get(itemID); err != nil {
			return beads.Bead{}, err
		}
	}
	convoy, err := store.Create(beads.Bead{Title: body.Title, Type: "convoy"})
	if err != nil {
		return beads.Bead{}, err
	}
	for _, itemID := range body.Items {
		pid := convoy.ID
		if err := store.Update(itemID, beads.UpdateOpts{ParentID: &pid}); err != nil {
			return beads.Bead{}, err
		}
	}
	return convoy, nil
}

func (s *Server) findConvoyStore(id string) (beads.Store, *beads.Bead, error) {
	stores := s.state.BeadStores()
	for _, rigName := range sortedRigNames(stores) {
		store := stores[rigName]
		b, err := store.Get(id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, nil, err
		}
		if b.Type != "convoy" {
			return nil, nil, httpError{status: 400, code: "invalid", message: "bead " + id + " is not a convoy"}
		}
		return store, &b, nil
	}
	return nil, nil, httpError{status: 404, code: "not_found", message: "convoy " + id + " not found"}
}

func (s *Server) convoyAddItems(id string, items []string) error {
	store, _, err := s.findConvoyStore(id)
	if err != nil {
		return err
	}
	for _, itemID := range items {
		if _, err := store.Get(itemID); err != nil {
			return err
		}
	}
	for _, itemID := range items {
		pid := id
		if err := store.Update(itemID, beads.UpdateOpts{ParentID: &pid}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) convoyRemoveItems(id string, items []string) error {
	store, _, err := s.findConvoyStore(id)
	if err != nil {
		return err
	}
	for _, itemID := range items {
		item, err := store.Get(itemID)
		if err != nil {
			return err
		}
		if item.ParentID != id {
			return httpError{status: 400, code: "invalid", message: "item " + itemID + " does not belong to convoy " + id}
		}
	}
	empty := ""
	for _, itemID := range items {
		if err := store.Update(itemID, beads.UpdateOpts{ParentID: &empty}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) convoyCheck(id string) (convoyCheckResponse, error) {
	store, _, err := s.findConvoyStore(id)
	if err != nil {
		return convoyCheckResponse{}, err
	}
	_ = store // used for find; check uses list
	children, err := store.List(beads.ListQuery{ParentID: id, IncludeClosed: true, Sort: beads.SortCreatedAsc})
	if err != nil {
		return convoyCheckResponse{}, err
	}
	total := len(children)
	closed := 0
	for _, c := range children {
		if c.Status == "closed" {
			closed++
		}
	}
	return convoyCheckResponse{ConvoyID: id, Total: total, Closed: closed, Complete: total > 0 && closed == total}, nil
}

func (s *Server) convoyClose(id string) error {
	store, _, err := s.findConvoyStore(id)
	if err != nil {
		return err
	}
	return store.Close(id)
}

func (s *Server) convoyDelete(id string) error {
	store, _, err := s.findConvoyStore(id)
	if err != nil {
		return err
	}
	return store.Close(id)
}
