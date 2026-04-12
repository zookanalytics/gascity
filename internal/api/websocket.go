package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/mail"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gorilla/websocket"
)

const wsProtocolVersion = "gc.v1alpha1"

type socketRequestEnvelope struct {
	Type           string          `json:"type"`
	ID             string          `json:"id"`
	Action         string          `json:"action"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Scope          *socketScope    `json:"scope,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type socketScope struct {
	City string `json:"city,omitempty"`
}

type socketHelloEnvelope struct {
	Type         string   `json:"type"`
	Protocol     string   `json:"protocol"`
	ServerRole   string   `json:"server_role"`
	ReadOnly     bool     `json:"read_only"`
	Capabilities []string `json:"capabilities"`
}

type socketResponseEnvelope struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Index  uint64 `json:"index,omitempty"`
	Result any    `json:"result,omitempty"`
}

type socketErrorEnvelope struct {
	Type    string       `json:"type"`
	ID      string       `json:"id,omitempty"`
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Details []FieldError `json:"details,omitempty"`
}

type socketActionResult struct {
	Index      uint64
	Result     any
	AfterWrite func()
}

type socketNamePayload struct {
	Name string `json:"name"`
}

type socketIDPayload struct {
	ID string `json:"id"`
}

type socketSessionsListPayload struct {
	State    string `json:"state,omitempty"`
	Template string `json:"template,omitempty"`
	Peek     bool   `json:"peek,omitempty"`
	Limit    *int   `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}

type socketAgentsListPayload struct {
	Pool    string `json:"pool,omitempty"`
	Rig     string `json:"rig,omitempty"`
	Running string `json:"running,omitempty"`
	Peek    bool   `json:"peek,omitempty"`
}

type socketBeadsListPayload struct {
	Status   string `json:"status,omitempty"`
	Type     string `json:"type,omitempty"`
	Label    string `json:"label,omitempty"`
	Assignee string `json:"assignee,omitempty"`
	Rig      string `json:"rig,omitempty"`
	Limit    *int   `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}

type socketMailListPayload struct {
	Agent  string `json:"agent,omitempty"`
	Status string `json:"status,omitempty"`
	Rig    string `json:"rig,omitempty"`
	Limit  *int   `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type socketMailGetPayload struct {
	ID  string `json:"id"`
	Rig string `json:"rig,omitempty"`
}

type socketMailCountPayload struct {
	Agent string `json:"agent,omitempty"`
	Rig   string `json:"rig,omitempty"`
}

type socketMailThreadPayload struct {
	ID  string `json:"id"`
	Rig string `json:"rig,omitempty"`
}

type socketEventsListPayload struct {
	Type  string `json:"type,omitempty"`
	Actor string `json:"actor,omitempty"`
	Since string `json:"since,omitempty"`
	Limit *int   `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type socketBeadAssignPayload struct {
	ID       string `json:"id"`
	Assignee string `json:"assignee"`
}

type socketBeadGraphPayload struct {
	RootID string `json:"root_id"`
}

type socketSessionTargetPayload struct {
	ID string `json:"id"`
}

type socketSessionSubmitPayload struct {
	ID      string               `json:"id"`
	Message string               `json:"message"`
	Intent  session.SubmitIntent `json:"intent,omitempty"`
}

type socketSessionTranscriptPayload struct {
	ID     string `json:"id"`
	Tail   int    `json:"tail,omitempty"`
	Before string `json:"before,omitempty"`
	Format string `json:"format,omitempty"`
}

type socketProvidersListPayload struct {
	View string `json:"view,omitempty"`
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

func socketPageParams(limit *int, cursor string, defaultLimit int) pageParams {
	pp := pageParams{
		Limit:    defaultLimit,
		IsPaging: strings.TrimSpace(cursor) != "",
	}
	if limit != nil {
		switch {
		case *limit == 0:
			pp.Limit = maxPaginationLimit
		case *limit > 0:
			pp.Limit = *limit
		}
	}
	if pp.Limit > maxPaginationLimit {
		pp.Limit = maxPaginationLimit
	}
	if cursor != "" {
		pp.Offset = decodeCursor(cursor)
	}
	return pp
}

type socketHandler interface {
	socketHello() socketHelloEnvelope
	handleSocketRequest(*socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope)
	startSocketSubscription(context.Context, *socketSession, *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope)
	stopSocketSubscription(*socketSession, *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope)
}

type socketConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type httpError struct {
	status  int
	code    string
	message string
	details []FieldError
}

func (e httpError) Error() string { return e.message }

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "" || isLocalhostOrigin(origin)
	},
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	serveWebSocket(w, r, s)
}

func (sm *SupervisorMux) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	serveWebSocket(w, r, sm)
}

func serveWebSocket(w http.ResponseWriter, r *http.Request, handler socketHandler) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("api: websocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	sc := &socketConn{conn: conn}
	ss := newSocketSession(r.Context(), sc)
	defer ss.close()

	if err := conn.SetReadDeadline(time.Now().Add(socketPongWait)); err != nil {
		return
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(socketPongWait))
	})
	go ss.runPingLoop()

	if err := sc.writeJSON(handler.socketHello()); err != nil {
		return
	}

	for {
		var req socketRequestEnvelope
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		if req.Type != "request" {
			if err := sc.writeJSON(newSocketError(req.ID, "invalid", "message type must be request")); err != nil {
				return
			}
			continue
		}
		if req.ID == "" || req.Action == "" {
			if err := sc.writeJSON(newSocketError(req.ID, "invalid", "request id and action are required")); err != nil {
				return
			}
			continue
		}

		var (
			result socketActionResult
			apiErr *socketErrorEnvelope
		)
		switch req.Action {
		case "subscription.start":
			result, apiErr = handler.startSocketSubscription(ss.ctx, ss, &req)
		case "subscription.stop":
			result, apiErr = handler.stopSocketSubscription(ss, &req)
		default:
			result, apiErr = handler.handleSocketRequest(&req)
		}
		if apiErr != nil {
			if err := sc.writeJSON(apiErr); err != nil {
				return
			}
			continue
		}
		if err := sc.writeJSON(socketResponseEnvelope{
			Type:   "response",
			ID:     req.ID,
			Index:  result.Index,
			Result: result.Result,
		}); err != nil {
			return
		}
		if result.AfterWrite != nil {
			result.AfterWrite()
		}
	}
}

func (sc *socketConn) writeJSON(v any) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteJSON(v)
}

func (s *Server) socketHello() socketHelloEnvelope {
	return socketHelloEnvelope{
		Type:       "hello",
		Protocol:   wsProtocolVersion,
		ServerRole: "city",
		ReadOnly:   s.readOnly,
		Capabilities: []string{
			"health.get",
			"status.get",
			"city.get",
			"city.patch",
			"config.get",
			"config.explain",
			"config.validate",
			"bead.create",
			"bead.close",
			"bead.update",
			"sessions.list",
			"beads.list",
			"beads.ready",
			"bead.get",
			"bead.deps",
			"mail.list",
			"mail.get",
			"mail.count",
			"mail.thread",
			"mail.read",
			"mail.mark_unread",
			"mail.archive",
			"mail.reply",
			"mail.send",
			"events.list",
			"rigs.list",
			"convoys.list",
			"convoy.get",
			"sling.run",
			"services.list",
			"service.get",
			"service.restart",
			"packs.list",
			"providers.list",
			"provider.get",
			"agents.list",
			"agent.get",
			"agent.suspend",
			"agent.resume",
			"rig.get",
			"rig.suspend",
			"rig.resume",
			"rig.restart",
			"session.get",
			"session.suspend",
			"session.close",
			"session.wake",
			"session.rename",
			"session.respond",
			"session.kill",
			"session.pending",
			"session.submit",
			"session.transcript",
		},
	}
}

func (sm *SupervisorMux) socketHello() socketHelloEnvelope {
	return socketHelloEnvelope{
		Type:       "hello",
		Protocol:   wsProtocolVersion,
		ServerRole: "supervisor",
		ReadOnly:   sm.readOnly,
		Capabilities: []string{
			"health.get",
			"cities.list",
			"city.get",
			"city.patch",
			"config.get",
			"config.explain",
			"config.validate",
			"bead.create",
			"bead.close",
			"bead.update",
			"status.get",
			"sessions.list",
			"beads.list",
			"beads.ready",
			"bead.get",
			"bead.deps",
			"mail.list",
			"mail.get",
			"mail.count",
			"mail.thread",
			"mail.read",
			"mail.mark_unread",
			"mail.archive",
			"mail.reply",
			"mail.send",
			"events.list",
			"rigs.list",
			"convoys.list",
			"convoy.get",
			"sling.run",
			"services.list",
			"service.get",
			"service.restart",
			"packs.list",
			"providers.list",
			"provider.get",
			"agents.list",
			"agent.get",
			"agent.suspend",
			"agent.resume",
			"rig.get",
			"rig.suspend",
			"rig.resume",
			"rig.restart",
			"session.get",
			"session.suspend",
			"session.close",
			"session.wake",
			"session.rename",
			"session.respond",
			"session.kill",
			"session.pending",
			"session.submit",
			"session.transcript",
		},
	}
}

func (s *Server) handleSocketRequest(req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	switch req.Action {
	case "health.get":
		return socketActionResult{Result: s.healthResponse()}, nil
	case "status.get":
		return socketActionResult{Result: s.statusSnapshot()}, nil
	case "city.get":
		return socketActionResult{Result: s.cityGet(), Index: s.latestIndex()}, nil
	case "city.patch":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var body cityPatchRequest
		if err := decodeSocketPayload(req.Payload, &body); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		if body.Suspended == nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", "no fields to update")
		}
		if err := s.patchCitySuspended(*body.Suspended); err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: map[string]string{"status": "ok"}}, nil
	case "config.get":
		return socketActionResult{Result: s.configGet(), Index: s.latestIndex()}, nil
	case "config.explain":
		return socketActionResult{Result: s.configExplain(), Index: s.latestIndex()}, nil
	case "config.validate":
		return socketActionResult{Result: s.configValidate(), Index: s.latestIndex()}, nil
	case "agents.list":
		var payload socketAgentsListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		items := s.listAgentResponses(payload.Pool, payload.Rig, payload.Running, payload.Peek)
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
	case "agent.get":
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		cfg := s.state.Config()
		agentCfg, ok := findAgent(cfg, payload.Name)
		if !ok {
			return socketActionResult{}, newSocketError(req.ID, "not_found", "agent "+payload.Name+" not found")
		}
		resp, _ := s.buildExpandedAgentResponse(agentCfg, expandedAgent{
			qualifiedName: payload.Name,
			rig:           agentCfg.Dir,
			suspended:     agentCfg.Suspended,
			provider:      agentCfg.Provider,
			description:   agentCfg.Description,
		}, false, "")
		return socketActionResult{Result: resp, Index: s.latestIndex()}, nil
	case "rig.get":
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		resp, ok := s.getRigResponse(payload.Name, false)
		if !ok {
			return socketActionResult{}, newSocketError(req.ID, "not_found", "rig "+payload.Name+" not found")
		}
		return socketActionResult{Result: resp, Index: s.latestIndex()}, nil
	case "sessions.list":
		var payload socketSessionsListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		items, err := s.listSessionResponses(payload.State, payload.Template, payload.Peek)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		pp := socketPageParams(payload.Limit, payload.Cursor, maxPaginationLimit)
		if !pp.IsPaging {
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []sessionResponse{}
		}
		return socketActionResult{Result: listResponse{Items: page, Total: total, NextCursor: nextCursor}, Index: s.latestIndex()}, nil
	case "beads.list":
		var payload socketBeadsListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
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
			return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []beads.Bead{}
		}
		return socketActionResult{Result: listResponse{Items: page, Total: total, NextCursor: nextCursor}, Index: s.latestIndex()}, nil
	case "beads.ready":
		items, err := s.listReadyBeads()
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
	case "bead.get":
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		bead, err := s.getBead(payload.ID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				return socketActionResult{}, newSocketError(req.ID, "not_found", "bead "+payload.ID+" not found")
			}
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: bead, Index: s.latestIndex()}, nil
	case "bead.deps":
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		resp, err := s.getBeadDeps(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: resp, Index: s.latestIndex()}, nil
	case "beads.graph":
		var payload socketBeadGraphPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		resp, err := s.getBeadGraph(payload.RootID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: resp, Index: s.latestIndex()}, nil
	case "bead.create":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload beadCreateRequest
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		bead, err := s.createBead(payload)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: bead}, nil
	case "bead.close":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.closeBead(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "bead.update":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.updateBead(payload.ID, req.Payload)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "bead.reopen":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.reopenBead(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "bead.assign":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketBeadAssignPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.assignBead(payload.ID, payload.Assignee)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "mail.list":
		var payload socketMailListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		items, err := s.listMailMessages(payload.Agent, payload.Status, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		pp := socketPageParams(payload.Limit, payload.Cursor, 50)
		if !pp.IsPaging {
			total := len(items)
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return socketActionResult{Result: listResponse{Items: items, Total: total}, Index: s.latestIndex()}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []mail.Message{}
		}
		return socketActionResult{Result: listResponse{Items: page, Total: total, NextCursor: nextCursor}, Index: s.latestIndex()}, nil
	case "mail.get":
		var payload socketMailGetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		msg, err := s.getMailMessage(payload.ID, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: msg, Index: s.latestIndex()}, nil
	case "mail.count":
		var payload socketMailCountPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.mailCount(payload.Agent, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result, Index: s.latestIndex()}, nil
	case "mail.thread":
		var payload socketMailThreadPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.listMailThread(payload.ID, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: listResponse{Items: result, Total: len(result)}, Index: s.latestIndex()}, nil
	case "mail.read":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketMailGetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.markMailRead(payload.ID, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "mail.mark_unread":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketMailGetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.markMailUnread(payload.ID, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "mail.archive":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketMailGetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.archiveMail(payload.ID, payload.Rig)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "mail.reply":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload struct {
			ID      string `json:"id"`
			Rig     string `json:"rig,omitempty"`
			From    string `json:"from"`
			Subject string `json:"subject"`
			Body    string `json:"body"`
		}
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.replyMail(payload.ID, payload.Rig, mailReplyRequest{
			From:    payload.From,
			Subject: payload.Subject,
			Body:    payload.Body,
		})
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "mail.send":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload mailSendRequest
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.sendMail(payload)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "events.list":
		var payload socketEventsListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		filter := events.Filter{Type: payload.Type, Actor: payload.Actor}
		if payload.Since != "" {
			d, err := time.ParseDuration(payload.Since)
			if err != nil {
				return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
			}
			filter.Since = time.Now().Add(-d)
		}
		items, err := s.listEvents(filter)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		pp := socketPageParams(payload.Limit, payload.Cursor, 100)
		if !pp.IsPaging {
			total := len(items)
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return socketActionResult{Result: listResponse{Items: items, Total: total}, Index: s.latestIndex()}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []events.Event{}
		}
		return socketActionResult{Result: listResponse{Items: page, Total: total, NextCursor: nextCursor}, Index: s.latestIndex()}, nil
	case "rigs.list":
		items := s.listRigResponses(false)
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
	case "convoys.list":
		items := s.listConvoys()
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
	case "convoy.get":
		var payload socketIDPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		snapshot, err := s.getConvoySnapshot(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: snapshot, Index: s.latestIndex()}, nil
	case "sling.run":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload slingBody
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, _, err := s.runSling(context.Background(), payload)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "services.list":
		items := s.listServices()
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}}, nil
	case "service.get":
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		service, err := s.getService(payload.Name)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: service}, nil
	case "service.restart":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		if err := s.restartService(payload.Name); err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: map[string]string{"status": "ok", "action": "restart", "service": payload.Name}}, nil
	case "packs.list":
		return socketActionResult{Result: map[string]any{"packs": s.listPacks()}, Index: s.latestIndex()}, nil
	case "providers.list":
		var payload socketProvidersListPayload
		if err := decodeOptionalSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		items := s.listProviders(payload.View == "public")
		return socketActionResult{Result: listResponse{Items: items, Total: len(items)}, Index: s.latestIndex()}, nil
	case "provider.get":
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		provider, err := s.getProvider(payload.Name)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: provider, Index: s.latestIndex()}, nil
	case "agent.suspend", "agent.resume":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		action := req.Action[len("agent."):]
		if err := s.applyAgentAction(payload.Name, action); err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: map[string]string{"status": "ok"}}, nil
	case "rig.suspend", "rig.resume", "rig.restart":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketNamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		action := req.Action[len("rig."):]
		result, err := s.applyRigAction(payload.Name, action)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.kill":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.killSessionTarget(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.get":
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.getSessionResponse(payload.ID, false)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.suspend":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		if err := s.suspendSessionTarget(payload.ID); err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: map[string]string{"status": "ok"}}, nil
	case "session.close":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		if err := s.closeSessionTarget(payload.ID); err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: map[string]string{"status": "ok"}}, nil
	case "session.wake":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.wakeSessionTarget(context.Background(), payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.rename":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionRenamePayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.renameSessionTarget(payload.ID, payload.Title)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.respond":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionRespondPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.respondSessionTarget(payload.ID, sessionRespondRequest{
			RequestID: payload.RequestID,
			Action:    payload.Action,
			Text:      payload.Text,
			Metadata:  payload.Metadata,
		})
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.pending":
		var payload socketSessionTargetPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.getSessionPending(payload.ID)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.submit":
		if s.readOnly {
			return socketActionResult{}, newSocketError(req.ID, "read_only", "mutations disabled: server bound to non-localhost address")
		}
		var payload socketSessionSubmitPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		if payload.Intent == "" {
			payload.Intent = session.SubmitIntentDefault
		}
		result, err := s.submitSessionTarget(context.Background(), payload.ID, payload.Message, payload.Intent)
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	case "session.transcript":
		var payload socketSessionTranscriptPayload
		if err := decodeSocketPayload(req.Payload, &payload); err != nil {
			return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
		}
		result, err := s.getSessionTranscript(payload.ID, sessionTranscriptQuery{
			Tail:   payload.Tail,
			Before: payload.Before,
			Raw:    payload.Format == "raw",
		})
		if err != nil {
			return socketActionResult{}, socketErrorFor(req.ID, err)
		}
		return socketActionResult{Result: result}, nil
	default:
		return socketActionResult{}, unknownSocketAction(req.ID, req.Action)
	}
}

func (sm *SupervisorMux) handleSocketRequest(req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	if socketActionRequiresCityScope(req.Action) {
		cityName, apiErr := sm.resolveSocketCityTarget(req.Scope)
		if apiErr != nil {
			apiErr.ID = req.ID
			return socketActionResult{}, apiErr
		}
		state := sm.resolver.CityState(cityName)
		if state == nil {
			return socketActionResult{}, newSocketError(req.ID, "not_found", "city not found or not running: "+cityName)
		}
		cityReq := *req
		cityReq.Scope = nil
		srv := sm.getCityServer(cityName, state)
		return srv.handleSocketRequest(&cityReq)
	}
	switch req.Action {
	case "health.get":
		return socketActionResult{Result: sm.healthResponse()}, nil
	case "cities.list":
		return socketActionResult{Result: sm.citiesList()}, nil
	default:
		return socketActionResult{}, unknownSocketAction(req.ID, req.Action)
	}
}

func socketActionRequiresCityScope(action string) bool {
	switch action {
	case "city.patch",
		"city.get",
		"config.get",
		"config.explain",
		"config.validate",
		"agents.list",
		"agent.get",
		"bead.create",
		"bead.close",
		"bead.update",
		"status.get",
		"sessions.list",
		"beads.list",
		"beads.ready",
		"bead.get",
		"bead.deps",
		"beads.graph",
		"bead.reopen",
		"bead.assign",
		"mail.list",
		"mail.get",
		"mail.count",
		"mail.thread",
		"mail.read",
		"mail.mark_unread",
		"mail.archive",
		"mail.reply",
		"mail.send",
		"events.list",
		"rigs.list",
		"convoys.list",
		"convoy.get",
		"sling.run",
		"services.list",
		"service.get",
		"service.restart",
		"packs.list",
		"providers.list",
		"provider.get",
		"agent.suspend",
		"agent.resume",
		"rig.get",
		"rig.suspend",
		"rig.resume",
		"rig.restart",
		"session.get",
		"session.suspend",
		"session.close",
		"session.wake",
		"session.rename",
		"session.respond",
		"session.kill",
		"session.pending",
		"session.submit":
		return true
	case "session.transcript":
		return true
	default:
		return false
	}
}

func (sm *SupervisorMux) resolveSocketCityTarget(scope *socketScope) (string, *socketErrorEnvelope) {
	if scope != nil && scope.City != "" {
		return scope.City, nil
	}
	cities := sm.resolver.ListCities()
	running := make([]CityInfo, 0, len(cities))
	for _, city := range cities {
		if city.Running {
			running = append(running, city)
		}
	}
	switch len(running) {
	case 0:
		return "", newSocketError("", "no_cities", "no cities running")
	case 1:
		return running[0].Name, nil
	default:
		return "", newSocketError("", "city_required", "multiple cities running; use scope.city to specify which city")
	}
}

func decodeSocketPayload(payload json.RawMessage, v any) error {
	if len(payload) == 0 {
		return errors.New("payload required")
	}
	return json.Unmarshal(payload, v)
}

func decodeOptionalSocketPayload(payload json.RawMessage, v any) error {
	if len(payload) == 0 {
		return nil
	}
	return json.Unmarshal(payload, v)
}

func socketErrorFor(id string, err error) *socketErrorEnvelope {
	var herr httpError
	if errors.As(err, &herr) {
		return newSocketErrorWithDetails(id, herr.code, herr.message, herr.details)
	}
	switch {
	case errors.Is(err, session.ErrAmbiguous), errors.Is(err, errConfiguredNamedSessionConflict):
		return newSocketError(id, "ambiguous", err.Error())
	case errors.Is(err, session.ErrSessionNotFound):
		return newSocketError(id, "not_found", err.Error())
	case errors.Is(err, session.ErrInvalidSessionName),
		errors.Is(err, session.ErrInvalidSessionAlias),
		errors.Is(err, session.ErrNotSession):
		return newSocketError(id, "invalid", err.Error())
	case errors.Is(err, session.ErrSessionNameExists),
		errors.Is(err, session.ErrSessionAliasExists),
		errors.Is(err, session.ErrPendingInteraction),
		errors.Is(err, session.ErrNoPendingInteraction),
		errors.Is(err, session.ErrInteractionMismatch),
		errors.Is(err, session.ErrSessionClosed),
		errors.Is(err, session.ErrResumeRequired):
		return newSocketError(id, "conflict", err.Error())
	case errors.Is(err, session.ErrInteractionUnsupported):
		return newSocketError(id, "unsupported", err.Error())
	}
	code := "internal"
	if errors.Is(err, websocket.ErrCloseSent) {
		code = "connection_closed"
	}
	return newSocketError(id, code, err.Error())
}

func newSocketError(id, code, message string) *socketErrorEnvelope {
	return newSocketErrorWithDetails(id, code, message, nil)
}

func newSocketErrorWithDetails(id, code, message string, details []FieldError) *socketErrorEnvelope {
	return &socketErrorEnvelope{
		Type:    "error",
		ID:      id,
		Code:    code,
		Message: message,
		Details: details,
	}
}

func unknownSocketAction(id, action string) *socketErrorEnvelope {
	return newSocketError(id, "not_found", "unknown action: "+action)
}
