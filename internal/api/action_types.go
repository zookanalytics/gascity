package api

import (
	"encoding/json"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/sessionlog"
)

type mutationStatusResponse struct {
	Status string `json:"status"`
}

type mutationStatusIDResponse struct {
	Status string `json:"status"`
	ID     string `json:"id,omitempty"`
}

type beadAssignResponse struct {
	Status   string `json:"status"`
	Assignee string `json:"assignee,omitempty"`
}

type beadDepsResponse struct {
	Children []beads.Bead `json:"children"`
}

type convoyProgressResponse struct {
	Total  int `json:"total"`
	Closed int `json:"closed"`
}

type convoySnapshotResponse struct {
	Convoy   beads.Bead             `json:"convoy"`
	Children []beads.Bead           `json:"children"`
	Progress convoyProgressResponse `json:"progress"`
}

type convoyCheckResponse struct {
	ConvoyID string `json:"convoy_id"`
	Total    int    `json:"total"`
	Closed   int    `json:"closed"`
	Complete bool   `json:"complete"`
}

type sessionAgentsResponse struct {
	Agents []sessionlog.AgentMapping `json:"agents"`
}

type sessionAgentDetailResponse struct {
	Messages []json.RawMessage `json:"messages"`
	Status   string            `json:"status"`
}

// SessionSubmitResponse mirrors the session.submit WebSocket response.
type SessionSubmitResponse struct {
	Status string               `json:"status"`
	ID     string               `json:"id"`
	Queued bool                 `json:"queued"`
	Intent session.SubmitIntent `json:"intent"`
}
