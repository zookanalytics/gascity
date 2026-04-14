package api

import (
	"context"
	"strings"
)

func init() {
	RegisterAction("sessions.list", ActionDef{
		Description:       "List sessions",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server, payload socketSessionsListPayload) (listResponse, error) {
		items, err := s.listSessionResponses(payload.State, payload.Template, payload.Peek)
		if err != nil {
			return listResponse{}, err
		}
		pp := socketPageParams(payload.Limit, payload.Cursor, maxPaginationLimit)
		if !pp.IsPaging {
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return listResponse{Items: items, Total: len(items)}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []sessionResponse{}
		}
		return listResponse{Items: page, Total: total, NextCursor: nextCursor}, nil
	})

	RegisterAction("session.get", ActionDef{
		Description:       "Get session details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (sessionResponse, error) {
		return s.getSessionResponse(payload.ID, payload.Peek)
	})

	// session.create is complex (internal round-trip + idempotency) — leave on legacy switch.
	RegisterMeta("session.create", ActionDef{
		Description:       "Create a session",
		IsMutation:        true,
		RequiresCityScope: true,
	})

	RegisterAction("session.suspend", ActionDef{
		Description:       "Suspend a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (map[string]string, error) {
		if err := s.suspendSessionTarget(payload.ID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok"}, nil
	})

	RegisterAction("session.close", ActionDef{
		Description:       "Close a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (map[string]string, error) {
		if err := s.closeSessionTarget(payload.ID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok"}, nil
	})

	RegisterAction("session.stop", ActionDef{
		Description:       "Stop a session turn",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (map[string]string, error) {
		store := s.state.CityBeadStore()
		if store == nil {
			return nil, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
		}
		id, err := s.resolveSessionIDWithConfig(store, payload.ID)
		if err != nil {
			return nil, err
		}
		mgr := s.sessionManager(store)
		if err := mgr.StopTurn(id); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok", "id": id}, nil
	})

	RegisterAction("session.wake", ActionDef{
		Description:       "Wake a suspended session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (map[string]string, error) {
		return s.wakeSessionTarget(context.Background(), payload.ID)
	})

	RegisterAction("session.rename", ActionDef{
		Description:       "Rename a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionRenamePayload) (sessionResponse, error) {
		return s.renameSessionTarget(payload.ID, payload.Title)
	})

	RegisterAction("session.respond", ActionDef{
		Description:       "Respond to a session prompt",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionRespondPayload) (map[string]string, error) {
		return s.respondSessionTarget(payload.ID, sessionRespondRequest{
			RequestID: payload.RequestID,
			Action:    payload.Action,
			Text:      payload.Text,
			Metadata:  payload.Metadata,
		})
	})

	RegisterAction("session.kill", ActionDef{
		Description:       "Kill a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (map[string]string, error) {
		return s.killSessionTarget(payload.ID)
	})

	RegisterAction("session.pending", ActionDef{
		Description:       "Get pending session requests",
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (sessionPendingResponse, error) {
		return s.getSessionPending(payload.ID)
	})

	// session.submit is complex — leave on legacy switch.
	RegisterMeta("session.submit", ActionDef{
		Description:       "Submit a message to a session",
		IsMutation:        true,
		RequiresCityScope: true,
	})

	RegisterAction("session.transcript", ActionDef{
		Description:       "Get session transcript",
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTranscriptPayload) (any, error) {
		return s.getSessionTranscript(payload.ID, sessionTranscriptQuery{
			Tail:   payload.Turns,
			Before: payload.Before,
			Raw:    payload.Format == "raw",
		})
	})

	RegisterAction("session.patch", ActionDef{
		Description:       "Patch session metadata",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionPatchPayload) (any, error) {
		return s.patchSession(payload.ID, payload.Title, payload.Alias)
	})

	RegisterAction("session.messages", ActionDef{
		Description:       "Send a user message to a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionMessagesPayload) (map[string]string, error) {
		if strings.TrimSpace(payload.Message) == "" {
			return nil, httpError{status: 400, code: "invalid", message: "message is required"}
		}
		store := s.state.CityBeadStore()
		if store == nil {
			return nil, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
		}
		id, err := s.resolveSessionIDMaterializingNamedWithContext(context.Background(), store, payload.ID)
		if err != nil {
			return nil, err
		}
		if err := s.sendUserMessageToSession(context.Background(), store, id, payload.Message); err != nil {
			return nil, err
		}
		return map[string]string{"status": "accepted", "id": id}, nil
	})

	RegisterAction("session.agents.list", ActionDef{
		Description:       "List agents in a session",
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionTargetPayload) (any, error) {
		return s.listSessionAgents(payload.ID)
	})

	RegisterAction("session.agent.get", ActionDef{
		Description:       "Get agent details in a session",
		RequiresCityScope: true,
	}, func(s *Server, payload socketSessionAgentGetPayload) (any, error) {
		return s.getSessionAgent(payload.ID, payload.AgentID)
	})
}
