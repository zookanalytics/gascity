package api

import (
	"encoding/json"
	"errors"

	"github.com/gastownhall/gascity/internal/beads"
)

func init() {
	RegisterAction("beads.list", ActionDef{
		Description:       "List beads",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server, payload socketBeadsListPayload) (listResponse, error) {
		items := s.listBeads(beads.ListQuery{
			Status:   payload.Status,
			Type:     payload.Type,
			Label:    payload.Label,
			Assignee: payload.Assignee,
		}, payload.Rig, nil)
		pp := socketPageParams(payload.Limit, payload.Cursor, 50)
		if !pp.IsPaging {
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return listResponse{Items: items, Total: len(items)}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []beads.Bead{}
		}
		return listResponse{Items: page, Total: total, NextCursor: nextCursor}, nil
	})

	RegisterVoidAction("beads.ready", ActionDef{
		Description:       "List ready beads",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server) (listResponse, error) {
		items, err := s.listReadyBeads()
		if err != nil {
			return listResponse{}, err
		}
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("beads.graph", ActionDef{
		Description:       "Get bead dependency graph",
		RequiresCityScope: true,
	}, func(s *Server, payload socketBeadGraphPayload) (beadGraphResponseJSON, error) {
		return s.getBeadGraph(payload.RootID)
	})

	RegisterAction("bead.get", ActionDef{
		Description:       "Get bead details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (beads.Bead, error) {
		bead, err := s.getBead(payload.ID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				return beads.Bead{}, httpError{status: 404, code: "not_found", message: "bead " + payload.ID + " not found"}
			}
			return beads.Bead{}, err
		}
		return bead, nil
	})

	RegisterAction("bead.deps", ActionDef{
		Description:       "Get bead dependencies",
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]any, error) {
		return s.getBeadDeps(payload.ID)
	})

	// bead.create uses idempotency — leave on legacy switch.
	RegisterMeta("bead.create", ActionDef{
		Description:       "Create a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	})

	RegisterAction("bead.close", ActionDef{
		Description:       "Close a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]string, error) {
		return s.closeBead(payload.ID)
	})

	RegisterAction("bead.update", ActionDef{
		Description:       "Update a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload json.RawMessage) (map[string]string, error) {
		// Extract the ID from the raw JSON payload.
		var idHolder socketIDPayload
		if err := json.Unmarshal(payload, &idHolder); err != nil {
			return nil, httpError{status: 400, code: "invalid", message: err.Error()}
		}
		return s.updateBead(idHolder.ID, payload)
	})

	RegisterAction("bead.reopen", ActionDef{
		Description:       "Reopen a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]string, error) {
		return s.reopenBead(payload.ID)
	})

	RegisterAction("bead.assign", ActionDef{
		Description:       "Assign a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketBeadAssignPayload) (map[string]string, error) {
		return s.assignBead(payload.ID, payload.Assignee)
	})

	RegisterAction("bead.delete", ActionDef{
		Description:       "Delete a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]string, error) {
		if err := s.deleteBead(payload.ID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted"}, nil
	})
}
