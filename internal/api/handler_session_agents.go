package api

import (
	"encoding/json"
	"errors"

	"github.com/gastownhall/gascity/internal/sessionlog"
)

// handleSessionAgentList returns subagent mappings for a session.
//
//	GET /v0/session/{id}/agents
//	Response: { "agents": [{ "agent_id": "...", "parent_tool_use_id": "..." }] }
// handleSessionAgentGet returns the transcript and status of a subagent.
//
//	GET /v0/session/{id}/agents/{agentId}
//	Response: { "messages": [...], "status": "completed|running|pending|failed" }
// --- Shared methods for WS dispatch ---

func (s *Server) listSessionAgents(target string) (sessionAgentsResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return sessionAgentsResponse{}, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDAllowClosedWithConfig(store, target)
	if err != nil {
		return sessionAgentsResponse{}, err
	}
	mgr := s.sessionManager(store)
	logPath, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		return sessionAgentsResponse{}, err
	}
	if logPath == "" {
		return sessionAgentsResponse{Agents: []sessionlog.AgentMapping{}}, nil
	}
	mappings, err := sessionlog.FindAgentMappings(logPath)
	if err != nil {
		return sessionAgentsResponse{}, httpError{status: 500, code: "internal", message: "failed to list agents"}
	}
	if mappings == nil {
		mappings = []sessionlog.AgentMapping{}
	}
	return sessionAgentsResponse{Agents: mappings}, nil
}

func (s *Server) getSessionAgent(target, agentID string) (sessionAgentDetailResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return sessionAgentDetailResponse{}, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDAllowClosedWithConfig(store, target)
	if err != nil {
		return sessionAgentDetailResponse{}, err
	}
	if agentID == "" {
		return sessionAgentDetailResponse{}, httpError{status: 400, code: "invalid", message: "agent_id is required"}
	}
	if err := sessionlog.ValidateAgentID(agentID); err != nil {
		return sessionAgentDetailResponse{}, httpError{status: 400, code: "invalid", message: err.Error()}
	}
	mgr := s.sessionManager(store)
	logPath, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		return sessionAgentDetailResponse{}, err
	}
	if logPath == "" {
		return sessionAgentDetailResponse{}, httpError{status: 404, code: "not_found", message: "no transcript found for session " + id}
	}
	agentSession, err := sessionlog.ReadAgentSession(logPath, agentID)
	if err != nil {
		if errors.Is(err, sessionlog.ErrAgentNotFound) {
			return sessionAgentDetailResponse{}, httpError{status: 404, code: "not_found", message: "agent not found"}
		}
		return sessionAgentDetailResponse{}, httpError{status: 500, code: "internal", message: "failed to read agent transcript"}
	}
	rawMessages := make([]json.RawMessage, 0, len(agentSession.Messages))
	for _, entry := range agentSession.Messages {
		if len(entry.Raw) > 0 {
			rawMessages = append(rawMessages, entry.Raw)
		}
	}
	return sessionAgentDetailResponse{Messages: rawMessages, Status: string(agentSession.Status)}, nil
}
