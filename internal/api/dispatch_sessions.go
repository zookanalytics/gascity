package api

import (
	"context"
	"strings"

	"github.com/gastownhall/gascity/internal/session"
)

type socketSessionsListPayload struct {
	State    string `json:"state,omitempty"`
	Template string `json:"template,omitempty"`
	Peek     bool   `json:"peek,omitempty"`
	Limit    *int   `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}

type socketSessionTargetPayload struct {
	ID   string `json:"id"`
	Peek bool   `json:"peek,omitempty"`
}

type socketSessionSubmitPayload struct {
	ID      string               `json:"id"`
	Message string               `json:"message"`
	Intent  session.SubmitIntent `json:"intent,omitempty"`
}

type socketSessionTranscriptPayload struct {
	ID     string `json:"id"`
	Turns  int    `json:"turns,omitempty"` // most recent N turns (0=all)
	Before string `json:"before,omitempty"`
	Format string `json:"format,omitempty"`
}

type socketSessionRenamePayload struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type socketSessionRespondPayload struct {
	ID        string            `json:"id"`
	RequestID string            `json:"request_id,omitempty"`
	Action    string            `json:"action"`
	Text      string            `json:"text,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type socketSessionPatchPayload struct {
	ID    string  `json:"id"`
	Title *string `json:"title,omitempty"`
	Alias *string `json:"alias,omitempty"`
}

type socketSessionAgentGetPayload struct {
	ID      string `json:"id"`
	AgentID string `json:"agent_id"`
}

type socketSessionMessagesPayload struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

func init() {
	RegisterAction("sessions.list", ActionDef{
		Description:       "List sessions",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(_ context.Context, s *Server, payload socketSessionsListPayload) (listResponse, error) {
		items, err := s.Sessions.List(payload.State, payload.Template, payload.Peek)
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
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (sessionResponse, error) {
		return s.Sessions.Get(payload.ID, payload.Peek)
	})

	RegisterAction("session.create", ActionDef{
		Description:       "Create a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(ctx context.Context, s *Server, p sessionCreateRequest) (sessionResponse, error) {
		result, _, err := s.Sessions.Create(ctx, p, "")
		return result, err
	})

	RegisterAction("session.suspend", ActionDef{
		Description:       "Suspend a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (mutationStatusResponse, error) {
		if err := s.Sessions.Suspend(payload.ID); err != nil {
			return mutationStatusResponse{}, err
		}
		return mutationStatusResponse{Status: "ok"}, nil
	})

	RegisterAction("session.close", ActionDef{
		Description:       "Close a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (mutationStatusResponse, error) {
		if err := s.Sessions.Close(payload.ID); err != nil {
			return mutationStatusResponse{}, err
		}
		return mutationStatusResponse{Status: "ok"}, nil
	})

	RegisterAction("session.stop", ActionDef{
		Description:       "Stop a session turn",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (mutationStatusIDResponse, error) {
		store := s.state.CityBeadStore()
		if store == nil {
			return mutationStatusIDResponse{}, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
		}
		id, err := s.resolveSessionIDWithConfig(store, payload.ID)
		if err != nil {
			return mutationStatusIDResponse{}, err
		}
		mgr := s.sessionManager(store)
		if err := mgr.StopTurn(id); err != nil {
			return mutationStatusIDResponse{}, err
		}
		return mutationStatusIDResponse{Status: "ok", ID: id}, nil
	})

	RegisterAction("session.wake", ActionDef{
		Description:       "Wake a suspended session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(ctx context.Context, s *Server, payload socketSessionTargetPayload) (mutationStatusIDResponse, error) {
		return s.Sessions.Wake(ctx, payload.ID)
	})

	RegisterAction("session.rename", ActionDef{
		Description:       "Rename a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionRenamePayload) (sessionResponse, error) {
		return s.Sessions.Rename(payload.ID, payload.Title)
	})

	RegisterAction("session.respond", ActionDef{
		Description:       "Respond to a session prompt",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionRespondPayload) (mutationStatusIDResponse, error) {
		return s.Sessions.Respond(payload.ID, sessionRespondRequest{
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
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (mutationStatusIDResponse, error) {
		return s.Sessions.Kill(payload.ID)
	})

	RegisterAction("session.pending", ActionDef{
		Description:       "Get pending session requests",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (sessionPendingResponse, error) {
		return s.Sessions.Pending(payload.ID)
	})

	RegisterAction("session.submit", ActionDef{
		Description:       "Submit a message to a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(ctx context.Context, s *Server, p socketSessionSubmitPayload) (SessionSubmitResponse, error) {
		if p.Intent == "" {
			p.Intent = session.SubmitIntentDefault
		}
		return s.Sessions.Submit(ctx, p.ID, p.Message, p.Intent)
	})

	RegisterAction("session.transcript", ActionDef{
		Description:       "Get session transcript",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTranscriptPayload) (sessionTranscriptResult, error) {
		format, err := normalizeSessionTranscriptFormat(payload.Format)
		if err != nil {
			return sessionTranscriptResult{}, err
		}
		return s.Sessions.Transcript(payload.ID, sessionTranscriptQuery{
			Tail:   payload.Turns,
			Before: payload.Before,
			Raw:    format == "raw",
		})
	})

	RegisterAction("session.patch", ActionDef{
		Description:       "Patch session metadata",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionPatchPayload) (sessionResponse, error) {
		return s.Sessions.Patch(payload.ID, payload.Title, payload.Alias)
	})

	RegisterAction("session.messages", ActionDef{
		Description:       "Send a user message to a session",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(ctx context.Context, s *Server, payload socketSessionMessagesPayload) (mutationStatusIDResponse, error) {
		if strings.TrimSpace(payload.Message) == "" {
			return mutationStatusIDResponse{}, httpError{status: 400, code: "invalid", message: "message is required"}
		}
		store := s.state.CityBeadStore()
		if store == nil {
			return mutationStatusIDResponse{}, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
		}
		id, err := s.resolveSessionIDMaterializingNamedWithContext(ctx, store, payload.ID)
		if err != nil {
			return mutationStatusIDResponse{}, err
		}
		if err := s.sendUserMessageToSession(ctx, store, id, payload.Message); err != nil {
			return mutationStatusIDResponse{}, err
		}
		return mutationStatusIDResponse{Status: "accepted", ID: id}, nil
	})

	RegisterAction("session.agents.list", ActionDef{
		Description:       "List agents in a session",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionTargetPayload) (sessionAgentsResponse, error) {
		return s.Sessions.ListAgents(payload.ID)
	})

	RegisterAction("session.agent.get", ActionDef{
		Description:       "Get agent details in a session",
		RequiresCityScope: true,
	}, func(_ context.Context, s *Server, payload socketSessionAgentGetPayload) (sessionAgentDetailResponse, error) {
		return s.Sessions.GetAgent(payload.ID, payload.AgentID)
	})
}
