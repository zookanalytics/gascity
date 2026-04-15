package api

import (
	"context"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/workspacesvc"
	"github.com/gorilla/websocket"
)

// Client is a WebSocket client for the Gas City API server.
// All API operations go through the persistent WebSocket connection.
// The client auto-reconnects with exponential backoff on failure.
type Client struct {
	baseURL     string
	socketScope *socketScope
	wsMu        sync.Mutex
	wsConn      *websocket.Conn
	wsFailCount int
	wsBackoff   time.Time // don't attempt WS before this time
	wsClosed    bool
	nextReqID   uint64

	wsReaderDone chan struct{}
	pending      sync.Map // map[string]chan wsClientResult

	subMu           sync.Mutex
	subs            map[string]*clientSubscription // stable client ID -> subscription state
	subServerIndex  map[string]string              // server subscription ID -> stable client ID
	eventBuf        map[string][]SubscriptionEvent // buffered by server subscription ID
	reconnectActive bool
}

type clientSubscription struct {
	id         string
	serverID   string
	scope      *socketScope
	payload    SubscriptionStartPayload
	callback   func(SubscriptionEvent)
	ctx        context.Context
	lastIndex  uint64
	lastCursor string
}

// NewClient creates a new API client targeting the given base URL
// (e.g., "http://127.0.0.1:8080").
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:        baseURL,
		subs:           make(map[string]*clientSubscription),
		subServerIndex: make(map[string]string),
		eventBuf:       make(map[string][]SubscriptionEvent),
	}
}

// NewCityScopedClient creates a client that attaches the given city scope to
// all requests sent through the supervisor WebSocket endpoint.
func NewCityScopedClient(baseURL, cityName string) *Client {
	c := NewClient(baseURL)
	c.socketScope = &socketScope{City: cityName}
	return c
}

// ListCities fetches the current set of cities managed by the supervisor.
func (c *Client) ListCities() ([]CityInfo, error) {
	var resp struct {
		Items []CityInfo `json:"items"`
	}
	if _, err := c.doSocketJSON("cities.list", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Items, nil
}

// ListServices fetches the current workspace service statuses.
func (c *Client) ListServices() ([]workspacesvc.Status, error) {
	var resp struct {
		Items []workspacesvc.Status `json:"items"`
	}
	if _, err := c.doSocketJSON("services.list", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Items, nil
}

// GetService fetches one current workspace service status.
func (c *Client) GetService(name string) (workspacesvc.Status, error) {
	var resp workspacesvc.Status
	if _, err := c.doSocketJSON("service.get", nil, socketNamePayload{Name: name}, &resp); err != nil {
		return workspacesvc.Status{}, err
	}
	return resp, nil
}

// RestartService restarts a service.
func (c *Client) RestartService(name string) error {
	_, err := c.doSocketJSON("service.restart", nil, socketNamePayload{Name: name}, nil)
	return err
}

// SuspendCity suspends the city via city.patch.
func (c *Client) SuspendCity() error {
	return c.patchCity(true)
}

// ResumeCity resumes the city via city.patch.
func (c *Client) ResumeCity() error {
	return c.patchCity(false)
}

func (c *Client) patchCity(suspend bool) error {
	_, err := c.doSocketJSON("city.patch", nil, cityPatchRequest{Suspended: &suspend}, nil)
	return err
}

// SuspendAgent suspends an agent.
func (c *Client) SuspendAgent(name string) error {
	_, err := c.doSocketJSON("agent.suspend", nil, socketNamePayload{Name: name}, nil)
	return err
}

// ResumeAgent resumes a suspended agent.
func (c *Client) ResumeAgent(name string) error {
	_, err := c.doSocketJSON("agent.resume", nil, socketNamePayload{Name: name}, nil)
	return err
}

// SuspendRig suspends a rig.
func (c *Client) SuspendRig(name string) error {
	_, err := c.doSocketJSON("rig.suspend", nil, socketNamePayload{Name: name}, nil)
	return err
}

// ResumeRig resumes a suspended rig.
func (c *Client) ResumeRig(name string) error {
	_, err := c.doSocketJSON("rig.resume", nil, socketNamePayload{Name: name}, nil)
	return err
}

// RestartRig restarts a rig. Kills all agents; the reconciler restarts them.
func (c *Client) RestartRig(name string) error {
	_, err := c.doSocketJSON("rig.restart", nil, socketNamePayload{Name: name}, nil)
	return err
}

// KillSession force-kills a session.
func (c *Client) KillSession(id string) error {
	_, err := c.doSocketJSON("session.kill", nil, socketSessionTargetPayload{ID: id}, nil)
	return err
}

// SubmitSession sends a semantic submit request to a session.
// The id may be either a bead ID or a resolvable session alias/name.
func (c *Client) SubmitSession(id, message string, intent session.SubmitIntent) (SessionSubmitResponse, error) {
	payload := socketSessionSubmitPayload{
		ID:      id,
		Message: message,
		Intent:  intent,
	}
	if payload.Intent == "" {
		payload.Intent = session.SubmitIntentDefault
	}
	var resp SessionSubmitResponse
	if _, err := c.doSocketJSON("session.submit", nil, payload, &resp); err != nil {
		return SessionSubmitResponse{}, err
	}
	return resp, nil
}
