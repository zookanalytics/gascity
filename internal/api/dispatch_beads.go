package api

import (
	"context"
	"errors"

	"github.com/gastownhall/gascity/internal/beads"
)

type socketBeadsListPayload struct {
	Status   string `json:"status,omitempty"`
	Type     string `json:"type,omitempty"`
	Label    string `json:"label,omitempty"`
	Assignee string `json:"assignee,omitempty"`
	Rig      string `json:"rig,omitempty"`
	Limit    *int   `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}

type socketBeadAssignPayload struct {
	ID       string `json:"id"`
	Assignee string `json:"assignee"`
}

type socketBeadGraphPayload struct {
	RootID string `json:"root_id"`
}

func init() {
	RegisterAction("beads.list", ActionDef{
		Description:       "List beads",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(_ context.Context, s *Server, payload socketBeadsListPayload) (listResponse, error) {
		items := s.Beads.List(beads.ListQuery{
			Status:   payload.Status,
			Type:     payload.Type,
			Label:    payload.Label,
			Assignee: payload.Assignee,
		}, payload.Rig)
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
	}, func(_ context.Context, s *Server) (listResponse, error) {
		items, err := s.Beads.ListReady()
		if err != nil {
			return listResponse{}, err
		}
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("beads.graph", ActionDef{
		Description:       "Get bead dependency graph",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketBeadGraphPayload) (beadGraphResponseJSON, error) {
		return s.Beads.Graph(payload.RootID)
	})

	RegisterAction("bead.get", ActionDef{
		Description:       "Get bead details",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketIDPayload) (beads.Bead, error) {
		bead, err := s.Beads.Get(payload.ID)
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
	}, func(_ context.Context, s *Server, payload socketIDPayload) (beadDepsResponse, error) {
		return s.Beads.Deps(payload.ID)
	})

	RegisterAction("bead.create", ActionDef{
		Description:       "Create a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, p beadCreateRequest) (beads.Bead, error) {
		return s.Beads.Create(p)
	})

	RegisterAction("bead.close", ActionDef{
		Description:       "Close a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketIDPayload) (mutationStatusResponse, error) {
		return s.Beads.Close(payload.ID)
	})

	RegisterAction("bead.update", ActionDef{
		Description:       "Update a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload beadUpdateRequest) (mutationStatusResponse, error) {
		return s.Beads.Update(payload)
	})

	RegisterAction("bead.reopen", ActionDef{
		Description:       "Reopen a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketIDPayload) (mutationStatusResponse, error) {
		return s.Beads.Reopen(payload.ID)
	})

	RegisterAction("bead.assign", ActionDef{
		Description:       "Assign a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketBeadAssignPayload) (beadAssignResponse, error) {
		return s.Beads.Assign(payload.ID, payload.Assignee)
	})

	RegisterAction("bead.delete", ActionDef{
		Description:       "Delete a bead",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketIDPayload) (mutationStatusResponse, error) {
		if err := s.Beads.Delete(payload.ID); err != nil {
			return mutationStatusResponse{}, err
		}
		return mutationStatusResponse{Status: "deleted"}, nil
	})
}
