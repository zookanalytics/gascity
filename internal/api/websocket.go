package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"

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
	Index  uint64
	Result any
}

type socketHandler interface {
	socketHello() socketHelloEnvelope
	handleSocketRequest(*socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope)
}

type socketConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type httpError struct {
	status  int
	code    string
	message string
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

		result, apiErr := handler.handleSocketRequest(&req)
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
	}
}

func (sc *socketConn) writeJSON(v any) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteJSON(v)
}

func (s *Server) socketHello() socketHelloEnvelope {
	return socketHelloEnvelope{
		Type:         "hello",
		Protocol:     wsProtocolVersion,
		ServerRole:   "city",
		ReadOnly:     s.readOnly,
		Capabilities: []string{"health.get", "city.patch"},
	}
}

func (sm *SupervisorMux) socketHello() socketHelloEnvelope {
	return socketHelloEnvelope{
		Type:         "hello",
		Protocol:     wsProtocolVersion,
		ServerRole:   "supervisor",
		ReadOnly:     sm.readOnly,
		Capabilities: []string{"health.get", "cities.list"},
	}
}

func (s *Server) handleSocketRequest(req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	switch req.Action {
	case "health.get":
		return socketActionResult{Result: s.healthResponse()}, nil
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
	default:
		return socketActionResult{}, unknownSocketAction(req.ID, req.Action)
	}
}

func (sm *SupervisorMux) handleSocketRequest(req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	switch req.Action {
	case "health.get":
		return socketActionResult{Result: sm.healthResponse()}, nil
	case "cities.list":
		return socketActionResult{Result: sm.citiesList()}, nil
	default:
		return socketActionResult{}, unknownSocketAction(req.ID, req.Action)
	}
}

func decodeSocketPayload(payload json.RawMessage, v any) error {
	if len(payload) == 0 {
		return errors.New("payload required")
	}
	return json.Unmarshal(payload, v)
}

func socketErrorFor(id string, err error) *socketErrorEnvelope {
	var herr httpError
	if errors.As(err, &herr) {
		return newSocketError(id, herr.code, herr.message)
	}
	code := "internal"
	if errors.Is(err, websocket.ErrCloseSent) {
		code = "connection_closed"
	}
	return newSocketError(id, code, err.Error())
}

func newSocketError(id, code, message string) *socketErrorEnvelope {
	return &socketErrorEnvelope{
		Type:    "error",
		ID:      id,
		Code:    code,
		Message: message,
	}
}

func unknownSocketAction(id, action string) *socketErrorEnvelope {
	return newSocketError(id, "not_found", "unknown action: "+action)
}
