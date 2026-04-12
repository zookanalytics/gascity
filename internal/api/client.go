package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/workspacesvc"
	"github.com/gorilla/websocket"
)

// connError wraps transport-level errors (connection refused, timeout, etc.)
// to distinguish them from API-level error responses.
type connError struct {
	err error
}

func (e *connError) Error() string { return e.err.Error() }
func (e *connError) Unwrap() error { return e.err }

// IsConnError reports whether err is a transport-level connection failure
// (e.g., connection refused, timeout) rather than an API-level error response.
func IsConnError(err error) bool {
	var ce *connError
	return errors.As(err, &ce)
}

// readOnlyError indicates the API server rejected a mutation because it's
// running in read-only mode (non-localhost bind).
type readOnlyError struct {
	msg string
}

func (e *readOnlyError) Error() string { return e.msg }

// ShouldFallback reports whether err indicates the CLI should fall back to
// direct file mutation. This is true for transport-level failures (connection
// refused, timeout) and for read-only API rejections (server bound to
// non-localhost, mutations disabled).
func ShouldFallback(err error) bool {
	if IsConnError(err) {
		return true
	}
	var ro *readOnlyError
	return errors.As(err, &ro)
}

// Client is an HTTP client for the Gas City API server.
// It wraps mutation endpoints so CLI commands can route writes
// through the API when a controller is running.
type Client struct {
	baseURL     string
	scopePrefix string
	socketScope *socketScope
	httpClient  *http.Client
	wsMu        sync.Mutex
	wsConn      *websocket.Conn
	wsDisabled  bool
	nextReqID   uint64
}

// SessionSubmitResponse mirrors POST /v0/session/{id}/submit.
type SessionSubmitResponse struct {
	Status string               `json:"status"`
	ID     string               `json:"id"`
	Queued bool                 `json:"queued"`
	Intent session.SubmitIntent `json:"intent"`
}

// NewClient creates a new API client targeting the given base URL
// (e.g., "http://127.0.0.1:8080").
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// NewCityScopedClient creates a client that routes requests through the
// supervisor's city-scoped API namespace for the given city name.
func NewCityScopedClient(baseURL, cityName string) *Client {
	c := NewClient(baseURL)
	c.scopePrefix = "/v0/city/" + escapeName(cityName)
	c.socketScope = &socketScope{City: cityName}
	return c
}

// ListCities fetches the current set of cities managed by the supervisor.
func (c *Client) ListCities() ([]CityInfo, error) {
	var resp struct {
		Items []CityInfo `json:"items"`
	}
	used, err := c.doSocketJSON("cities.list", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	if !used {
		if err := c.doGet("/v0/cities", &resp); err != nil {
			return nil, err
		}
	}
	return resp.Items, nil
}

// ListServices fetches the current workspace service statuses.
func (c *Client) ListServices() ([]workspacesvc.Status, error) {
	var resp struct {
		Items []workspacesvc.Status `json:"items"`
	}
	used, err := c.doSocketJSON("services.list", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	if !used {
		if err := c.doGet("/v0/services", &resp); err != nil {
			return nil, err
		}
	}
	return resp.Items, nil
}

// GetService fetches one current workspace service status.
func (c *Client) GetService(name string) (workspacesvc.Status, error) {
	var resp workspacesvc.Status
	used, err := c.doSocketJSON("service.get", nil, map[string]any{"name": name}, &resp)
	if err != nil {
		return workspacesvc.Status{}, err
	}
	if !used {
		if err := c.doGet("/v0/service/"+url.PathEscape(name), &resp); err != nil {
			return workspacesvc.Status{}, err
		}
	}
	return resp, nil
}

// RestartService restarts a service via POST /v0/service/{name}/restart.
func (c *Client) RestartService(name string) error {
	if used, err := c.doSocketJSON("service.restart", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/service/"+url.PathEscape(name)+"/restart", nil)
}

// SuspendCity suspends the city via PATCH /v0/city.
func (c *Client) SuspendCity() error {
	return c.patchCity(true)
}

// ResumeCity resumes the city via PATCH /v0/city.
func (c *Client) ResumeCity() error {
	return c.patchCity(false)
}

func (c *Client) patchCity(suspend bool) error {
	body := map[string]any{"suspended": suspend}
	if used, err := c.doSocketJSON("city.patch", nil, body, nil); used || err != nil {
		return err
	}
	return c.doMutation("PATCH", "/v0/city", body)
}

// SuspendAgent suspends an agent via POST /v0/agent/{name}/suspend.
// Name can be qualified (e.g., "myrig/worker") — the server route uses
// {name...} wildcard which captures slashes.
func (c *Client) SuspendAgent(name string) error {
	if used, err := c.doSocketJSON("agent.suspend", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/agent/"+escapeName(name)+"/suspend", nil)
}

// ResumeAgent resumes an agent via POST /v0/agent/{name}/resume.
func (c *Client) ResumeAgent(name string) error {
	if used, err := c.doSocketJSON("agent.resume", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/agent/"+escapeName(name)+"/resume", nil)
}

// SuspendRig suspends a rig via POST /v0/rig/{name}/suspend.
func (c *Client) SuspendRig(name string) error {
	if used, err := c.doSocketJSON("rig.suspend", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/suspend", nil)
}

// ResumeRig resumes a rig via POST /v0/rig/{name}/resume.
func (c *Client) ResumeRig(name string) error {
	if used, err := c.doSocketJSON("rig.resume", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/resume", nil)
}

// RestartRig restarts a rig via POST /v0/rig/{name}/restart.
// Kills all agents in the rig; the reconciler restarts them.
func (c *Client) RestartRig(name string) error {
	if used, err := c.doSocketJSON("rig.restart", nil, map[string]any{"name": name}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/restart", nil)
}

// KillSession force-kills a session via POST /v0/session/{id}/kill.
func (c *Client) KillSession(id string) error {
	if used, err := c.doSocketJSON("session.kill", nil, map[string]any{"id": id}, nil); used || err != nil {
		return err
	}
	return c.doMutation("POST", "/v0/session/"+url.PathEscape(id)+"/kill", nil)
}

// SubmitSession sends a semantic submit request to a session.
// The id may be either a bead ID or a resolvable session alias/name.
func (c *Client) SubmitSession(id, message string, intent session.SubmitIntent) (SessionSubmitResponse, error) {
	body := map[string]any{
		"message": message,
	}
	if intent != "" {
		body["intent"] = intent
	}
	var resp SessionSubmitResponse
	socketBody := map[string]any{
		"id":      id,
		"message": message,
	}
	if intent != "" {
		socketBody["intent"] = intent
	}
	used, err := c.doSocketJSON("session.submit", nil, socketBody, &resp)
	if err != nil {
		return SessionSubmitResponse{}, err
	}
	if !used {
		if err := c.doPostJSON("/v0/session/"+url.PathEscape(id)+"/submit", body, &resp); err != nil {
			return SessionSubmitResponse{}, err
		}
	}
	return resp, nil
}

// GetJSON fetches a JSON resource and returns the raw response body.
// For supported read routes, it prefers the websocket transport and falls
// back to HTTP automatically when websocket is unavailable.
func (c *Client) GetJSON(path string) ([]byte, error) {
	if action, payload, ok := socketGetAction(path); ok {
		raw, used, err := c.doSocketRaw(action, nil, payload)
		if err != nil {
			return nil, err
		}
		if used {
			return raw, nil
		}
	}
	return c.doGetRaw(path)
}

// PostJSON sends a JSON mutation request and returns the raw response body.
// For supported mutation routes, it prefers the websocket transport and falls
// back to HTTP automatically when websocket is unavailable.
func (c *Client) PostJSON(path string, payload any) ([]byte, error) {
	if action, socketPayload, ok, err := socketPostAction(path, payload); err != nil {
		return nil, err
	} else if ok {
		raw, used, err := c.doSocketRaw(action, nil, socketPayload)
		if err != nil {
			return nil, err
		}
		if used {
			return raw, nil
		}
	}
	return c.doPostRaw(path, payload)
}

// escapeName escapes each segment of a potentially qualified name (e.g.,
// "myrig/worker") for use in URL paths. Slashes are preserved as path
// separators; other URL metacharacters (#, ?, etc.) are percent-encoded.
func escapeName(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// doMutation sends a mutation request and checks for errors.
func (c *Client) doMutation(method, path string, body any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.urlForPath(path), bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-GC-Request", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &connError{err: fmt.Errorf("request failed: %w", err)}
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Parse error response.
	var apiErr struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		return fmt.Errorf("API returned %d", resp.StatusCode)
	}

	// Read-only rejection: server is bound non-localhost and rejects mutations.
	// CLI should fall back to direct file mutation.
	if apiErr.Error == "read_only" {
		msg := apiErr.Message
		if msg == "" {
			msg = "mutations disabled (read-only server)"
		}
		return &readOnlyError{msg: msg}
	}

	if apiErr.Message != "" {
		return fmt.Errorf("API error: %s", apiErr.Message)
	}
	if apiErr.Error != "" {
		return fmt.Errorf("API error: %s", apiErr.Error)
	}
	return fmt.Errorf("API returned %d", resp.StatusCode)
}

func (c *Client) doGet(path string, out any) error {
	body, err := c.doGetRaw(path)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doGetRaw(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, c.urlForPath(path), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &connError{err: fmt.Errorf("request failed: %w", err)}
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}

	var apiErr struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &apiErr); err != nil {
		return nil, fmt.Errorf("API returned %d", resp.StatusCode)
	}
	if apiErr.Message != "" {
		return nil, fmt.Errorf("API error: %s", apiErr.Message)
	}
	if apiErr.Error != "" {
		return nil, fmt.Errorf("API error: %s", apiErr.Error)
	}
	return nil, fmt.Errorf("API returned %d", resp.StatusCode)
}

func (c *Client) doPostJSON(path string, body any, out any) error {
	raw, err := c.doPostRaw(path, body)
	if err != nil {
		return err
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doPostRaw(path string, body any) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodPost, c.urlForPath(path), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-GC-Request", "true")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &connError{err: fmt.Errorf("request failed: %w", err)}
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort

	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return raw, nil
	}

	var apiErr struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &apiErr); err != nil {
		return nil, fmt.Errorf("API returned %d", resp.StatusCode)
	}

	if apiErr.Error == "read_only" {
		msg := apiErr.Message
		if msg == "" {
			msg = "mutations disabled (read-only server)"
		}
		return nil, &readOnlyError{msg: msg}
	}
	if apiErr.Message != "" {
		return nil, fmt.Errorf("API error: %s", apiErr.Message)
	}
	if apiErr.Error != "" {
		return nil, fmt.Errorf("API error: %s", apiErr.Error)
	}
	return nil, fmt.Errorf("API returned %d", resp.StatusCode)
}

func (c *Client) doSocketJSON(action string, scope *socketScope, payload any, out any) (bool, error) {
	resp, handled, err := c.doSocketRequest(action, c.effectiveSocketScope(scope), payload)
	if !handled || err != nil {
		return handled, err
	}
	if out == nil || len(resp.Result) == 0 {
		return true, nil
	}
	if err := json.Unmarshal(resp.Result, out); err != nil {
		return true, fmt.Errorf("decode websocket response: %w", err)
	}
	return true, nil
}

func (c *Client) doSocketRaw(action string, scope *socketScope, payload any) ([]byte, bool, error) {
	resp, handled, err := c.doSocketRequest(action, c.effectiveSocketScope(scope), payload)
	if !handled || err != nil {
		return nil, handled, err
	}
	return append([]byte(nil), resp.Result...), true, nil
}

type socketClientResponseEnvelope struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Index  uint64          `json:"index,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

func (c *Client) doSocketRequest(action string, scope *socketScope, payload any) (socketClientResponseEnvelope, bool, error) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	if c.wsDisabled {
		return socketClientResponseEnvelope{}, false, nil
	}
	if err := c.ensureWSConnLocked(); err != nil {
		c.wsDisabled = true
		return socketClientResponseEnvelope{}, false, nil
	}

	c.nextReqID++
	req := socketRequestEnvelope{
		Type:   "request",
		ID:     fmt.Sprintf("cli-%d", c.nextReqID),
		Action: action,
		Scope:  scope,
	}
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return socketClientResponseEnvelope{}, true, fmt.Errorf("marshal websocket payload: %w", err)
		}
		req.Payload = data
	}

	if err := c.wsConn.WriteJSON(req); err != nil {
		_ = c.wsConn.Close()
		c.wsConn = nil
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket request failed: %w", err)}
	}

	var raw map[string]json.RawMessage
	if err := c.wsConn.ReadJSON(&raw); err != nil {
		_ = c.wsConn.Close()
		c.wsConn = nil
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket read failed: %w", err)}
	}

	var msgType string
	if err := json.Unmarshal(raw["type"], &msgType); err != nil {
		return socketClientResponseEnvelope{}, true, fmt.Errorf("decode websocket type: %w", err)
	}
	switch msgType {
	case "response":
		var resp socketClientResponseEnvelope
		if err := decodeRawMessage(raw, &resp); err != nil {
			return socketClientResponseEnvelope{}, true, err
		}
		return resp, true, nil
	case "error":
		var resp socketErrorEnvelope
		if err := decodeRawMessage(raw, &resp); err != nil {
			return socketClientResponseEnvelope{}, true, err
		}
		if resp.Code == "read_only" {
			msg := resp.Message
			if msg == "" {
				msg = "mutations disabled (read-only server)"
			}
			return socketClientResponseEnvelope{}, true, &readOnlyError{msg: msg}
		}
		if resp.Message != "" {
			return socketClientResponseEnvelope{}, true, fmt.Errorf("API error: %s", resp.Message)
		}
		if resp.Code != "" {
			return socketClientResponseEnvelope{}, true, fmt.Errorf("API error: %s", resp.Code)
		}
		return socketClientResponseEnvelope{}, true, fmt.Errorf("API error")
	default:
		return socketClientResponseEnvelope{}, true, fmt.Errorf("unexpected websocket message type: %s", msgType)
	}
}

func (c *Client) ensureWSConnLocked() error {
	if c.wsConn != nil {
		return nil
	}
	wsURL, err := websocketURLForBase(c.baseURL)
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("Origin", "http://localhost")
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("websocket handshake failed: %s", resp.Status)
		}
		return err
	}
	var hello socketHelloEnvelope
	if err := conn.ReadJSON(&hello); err != nil {
		_ = conn.Close()
		return err
	}
	if hello.Type != "hello" {
		_ = conn.Close()
		return fmt.Errorf("unexpected websocket hello type: %s", hello.Type)
	}
	c.wsConn = conn
	return nil
}

func websocketURLForBase(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse base url: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported base url scheme: %s", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v0/ws"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func decodeRawMessage(raw map[string]json.RawMessage, out any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal websocket envelope: %w", err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode websocket envelope: %w", err)
	}
	return nil
}

func (c *Client) urlForPath(path string) string {
	if c.scopePrefix != "" && strings.HasPrefix(path, "/v0/") {
		return c.baseURL + c.scopePrefix + strings.TrimPrefix(path, "/v0")
	}
	return c.baseURL + path
}

func (c *Client) effectiveSocketScope(scope *socketScope) *socketScope {
	if scope != nil {
		return scope
	}
	return c.socketScope
}

func socketGetAction(path string) (string, any, bool) {
	u, err := url.Parse(path)
	if err != nil {
		return "", nil, false
	}
	switch {
	case u.Path == "/v0/city":
		return "city.get", nil, true
	case u.Path == "/v0/config":
		return "config.get", nil, true
	case u.Path == "/v0/config/explain":
		return "config.explain", nil, true
	case u.Path == "/v0/config/validate":
		return "config.validate", nil, true
	case u.Path == "/v0/packs":
		return "packs.list", nil, true
	case u.Path == "/v0/providers":
		payload := map[string]any{}
		if v := strings.TrimSpace(u.Query().Get("view")); v != "" {
			payload["view"] = v
		}
		return "providers.list", payload, true
	case strings.HasPrefix(u.Path, "/v0/provider/"):
		name := strings.TrimPrefix(u.Path, "/v0/provider/")
		if name == "" {
			return "", nil, false
		}
		return "provider.get", map[string]any{"name": name}, true
	case u.Path == "/v0/status":
		return "status.get", nil, true
	case u.Path == "/v0/agents":
		payload := map[string]any{}
		for _, key := range []string{"pool", "rig", "running"} {
			if v := strings.TrimSpace(u.Query().Get(key)); v != "" {
				payload[key] = v
			}
		}
		if u.Query().Get("peek") == "true" {
			payload["peek"] = true
		}
		return "agents.list", payload, true
	case strings.HasPrefix(u.Path, "/v0/agent/"):
		name := strings.TrimPrefix(u.Path, "/v0/agent/")
		if name == "" {
			return "", nil, false
		}
		return "agent.get", map[string]any{"name": name}, true
	case strings.HasPrefix(u.Path, "/v0/rig/"):
		name := strings.TrimPrefix(u.Path, "/v0/rig/")
		if name == "" {
			return "", nil, false
		}
		return "rig.get", map[string]any{"name": name}, true
	case u.Path == "/v0/services":
		return "services.list", nil, true
	case strings.HasPrefix(u.Path, "/v0/service/"):
		name := strings.TrimPrefix(u.Path, "/v0/service/")
		if name == "" {
			return "", nil, false
		}
		return "service.get", map[string]any{"name": name}, true
	case u.Path == "/v0/sessions":
		payload := map[string]any{}
		if v := strings.TrimSpace(u.Query().Get("state")); v != "" {
			payload["state"] = v
		}
		if v := strings.TrimSpace(u.Query().Get("template")); v != "" {
			payload["template"] = v
		}
		if u.Query().Get("peek") == "true" {
			payload["peek"] = true
		}
		applySocketPaginationQuery(payload, u.Query())
		return "sessions.list", payload, true
	case u.Path == "/v0/beads":
		payload := map[string]any{}
		for _, key := range []string{"status", "type", "label", "assignee", "rig"} {
			if v := strings.TrimSpace(u.Query().Get(key)); v != "" {
				payload[key] = v
			}
		}
		applySocketPaginationQuery(payload, u.Query())
		return "beads.list", payload, true
	case u.Path == "/v0/beads/ready":
		return "beads.ready", nil, true
	case strings.HasPrefix(u.Path, "/v0/beads/graph/"):
		rootID := strings.TrimPrefix(u.Path, "/v0/beads/graph/")
		if rootID == "" {
			return "", nil, false
		}
		return "beads.graph", map[string]any{"root_id": rootID}, true
	case strings.HasSuffix(u.Path, "/deps") && strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/bead/"), "/deps")
		if id == "" {
			return "", nil, false
		}
		return "bead.deps", map[string]any{"id": id}, true
	case strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimPrefix(u.Path, "/v0/bead/")
		if id == "" {
			return "", nil, false
		}
		return "bead.get", map[string]any{"id": id}, true
	case u.Path == "/v0/mail":
		payload := map[string]any{}
		for _, key := range []string{"agent", "status", "rig"} {
			if v := strings.TrimSpace(u.Query().Get(key)); v != "" {
				payload[key] = v
			}
		}
		applySocketPaginationQuery(payload, u.Query())
		return "mail.list", payload, true
	case u.Path == "/v0/mail/count":
		payload := map[string]any{}
		for _, key := range []string{"agent", "rig"} {
			if v := strings.TrimSpace(u.Query().Get(key)); v != "" {
				payload[key] = v
			}
		}
		return "mail.count", payload, true
	case strings.HasPrefix(u.Path, "/v0/mail/thread/"):
		id := strings.TrimPrefix(u.Path, "/v0/mail/thread/")
		if id == "" {
			return "", nil, false
		}
		payload := map[string]any{"id": id}
		if v := strings.TrimSpace(u.Query().Get("rig")); v != "" {
			payload["rig"] = v
		}
		return "mail.thread", payload, true
	case strings.HasPrefix(u.Path, "/v0/mail/"):
		id := strings.TrimPrefix(u.Path, "/v0/mail/")
		if id == "" {
			return "", nil, false
		}
		payload := map[string]any{"id": id}
		if v := strings.TrimSpace(u.Query().Get("rig")); v != "" {
			payload["rig"] = v
		}
		return "mail.get", payload, true
	case u.Path == "/v0/events":
		payload := map[string]any{}
		for _, key := range []string{"type", "actor", "since"} {
			if v := strings.TrimSpace(u.Query().Get(key)); v != "" {
				payload[key] = v
			}
		}
		applySocketPaginationQuery(payload, u.Query())
		return "events.list", payload, true
	case u.Path == "/v0/rigs":
		payload := map[string]any{}
		if u.Query().Get("git") == "true" {
			payload["git"] = true
		}
		return "rigs.list", payload, true
	case u.Path == "/v0/convoys":
		return "convoys.list", nil, true
	case strings.HasPrefix(u.Path, "/v0/convoy/"):
		id := strings.TrimPrefix(u.Path, "/v0/convoy/")
		if id == "" {
			return "", nil, false
		}
		return "convoy.get", map[string]any{"id": id}, true
	case strings.HasSuffix(u.Path, "/pending") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/pending")
		if id == "" {
			return "", nil, false
		}
		return "session.pending", map[string]any{"id": id}, true
	case strings.HasSuffix(u.Path, "/transcript") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/transcript")
		if id == "" {
			return "", nil, false
		}
		payload := map[string]any{"id": id}
		if v := strings.TrimSpace(u.Query().Get("before")); v != "" {
			payload["before"] = v
		}
		if v := strings.TrimSpace(u.Query().Get("format")); v != "" {
			payload["format"] = v
		}
		if v := strings.TrimSpace(u.Query().Get("tail")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return "", nil, false
			}
			payload["tail"] = n
		}
		return "session.transcript", payload, true
	case strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimPrefix(u.Path, "/v0/session/")
		if id == "" || strings.Contains(id, "/") {
			return "", nil, false
		}
		payload := map[string]any{"id": id}
		if u.Query().Get("peek") == "true" {
			payload["peek"] = true
		}
		return "session.get", payload, true
	default:
		return "", nil, false
	}
}

func applySocketPaginationQuery(payload map[string]any, q url.Values) {
	if payload == nil {
		return
	}
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			payload["limit"] = n
		}
	}
	if v := strings.TrimSpace(q.Get("cursor")); v != "" {
		payload["cursor"] = v
	}
}

func socketPostAction(path string, payload any) (string, any, bool, error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", nil, false, nil
	}
	switch {
	case u.Path == "/v0/beads":
		merged, err := mergeSocketPayload(nil, payload)
		return "bead.create", merged, true, err
	case strings.HasSuffix(u.Path, "/close") && strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/bead/"), "/close")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "bead.close", merged, true, err
	case strings.HasSuffix(u.Path, "/update") && strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/bead/"), "/update")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "bead.update", merged, true, err
	case u.Path == "/v0/mail":
		merged, err := mergeSocketPayload(nil, payload)
		return "mail.send", merged, true, err
	case strings.HasSuffix(u.Path, "/read") && strings.HasPrefix(u.Path, "/v0/mail/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/mail/"), "/read")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "mail.read", merged, true, err
	case strings.HasSuffix(u.Path, "/reply") && strings.HasPrefix(u.Path, "/v0/mail/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/mail/"), "/reply")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "mail.reply", merged, true, err
	case strings.HasSuffix(u.Path, "/mark-unread") && strings.HasPrefix(u.Path, "/v0/mail/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/mail/"), "/mark-unread")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "mail.mark_unread", merged, true, err
	case strings.HasSuffix(u.Path, "/archive") && strings.HasPrefix(u.Path, "/v0/mail/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/mail/"), "/archive")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "mail.archive", merged, true, err
	case u.Path == "/v0/sling":
		merged, err := mergeSocketPayload(nil, payload)
		return "sling.run", merged, true, err
	case strings.HasSuffix(u.Path, "/reopen") && strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/bead/"), "/reopen")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "bead.reopen", merged, true, err
	case strings.HasSuffix(u.Path, "/assign") && strings.HasPrefix(u.Path, "/v0/bead/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/bead/"), "/assign")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "bead.assign", merged, true, err
	case strings.HasSuffix(u.Path, "/wake") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/wake")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "session.wake", merged, true, err
	case strings.HasSuffix(u.Path, "/rename") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/rename")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "session.rename", merged, true, err
	case strings.HasSuffix(u.Path, "/respond") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/respond")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "session.respond", merged, true, err
	case strings.HasSuffix(u.Path, "/suspend") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/suspend")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "session.suspend", merged, true, err
	case strings.HasSuffix(u.Path, "/close") && strings.HasPrefix(u.Path, "/v0/session/"):
		id := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/v0/session/"), "/close")
		if id == "" {
			return "", nil, false, nil
		}
		merged, err := mergeSocketPayload(map[string]any{"id": id}, payload)
		return "session.close", merged, true, err
	default:
		return "", nil, false, nil
	}
}

func mergeSocketPayload(base map[string]any, payload any) (map[string]any, error) {
	if base == nil {
		base = map[string]any{}
	}
	if payload == nil {
		return base, nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal socket payload: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return base, nil
	}
	var extra map[string]any
	if err := json.Unmarshal(data, &extra); err != nil {
		return nil, fmt.Errorf("decode socket payload: %w", err)
	}
	for k, v := range extra {
		base[k] = v
	}
	return base, nil
}
