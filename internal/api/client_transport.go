package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

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

// wsClientResult carries either a response or an error from the background
// reader to the waiting request goroutine.
type wsClientResult struct {
	resp socketClientResponseEnvelope
	err  error
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

// wsBackoffDuration returns the backoff duration for the given failure count.
func wsBackoffDuration(failCount int) time.Duration {
	d := time.Second
	for i := 1; i < failCount && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// Close shuts down the WebSocket connection and waits for the reader to exit.
func (c *Client) Close() {
	c.wsMu.Lock()
	conn := c.wsConn
	done := c.wsReaderDone
	c.wsConn = nil
	c.wsClosed = true
	c.wsMu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	if done != nil {
		<-done
	}
}

func (c *Client) doSocketRequest(action string, scope *socketScope, payload any) (socketClientResponseEnvelope, bool, error) {
	c.wsMu.Lock()
	if c.wsClosed {
		c.wsMu.Unlock()
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket client closed")}
	}

	if !c.wsBackoff.IsZero() && time.Now().Before(c.wsBackoff) {
		c.wsMu.Unlock()
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket in backoff (next retry in %s)", time.Until(c.wsBackoff).Truncate(time.Millisecond))}
	}

	if err := c.ensureWSConnLocked(); err != nil {
		c.wsFailCount++
		c.wsBackoff = time.Now().Add(wsBackoffDuration(c.wsFailCount))
		c.wsMu.Unlock()
		log.Printf("api: ws connect failed (attempt %d, backoff %s): %v", c.wsFailCount, wsBackoffDuration(c.wsFailCount), err)
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket connect failed: %w", err)}
	}
	c.wsFailCount = 0
	c.wsBackoff = time.Time{}

	c.nextReqID++
	reqID := fmt.Sprintf("cli-%d", c.nextReqID)
	req := socketRequestEnvelope{
		Type:   "request",
		ID:     reqID,
		Action: action,
		Scope:  scope,
	}
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			c.wsMu.Unlock()
			return socketClientResponseEnvelope{}, true, fmt.Errorf("marshal websocket payload: %w", err)
		}
		req.Payload = data
	}

	ch := make(chan wsClientResult, 1)
	c.pending.Store(reqID, ch)

	if err := c.wsConn.WriteJSON(req); err != nil {
		c.pending.Delete(reqID)
		_ = c.wsConn.Close()
		c.wsConn = nil
		c.wsFailCount++
		c.wsBackoff = time.Now().Add(wsBackoffDuration(c.wsFailCount))
		c.wsMu.Unlock()
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket write failed: %w", err)}
	}

	c.wsMu.Unlock()

	select {
	case result := <-ch:
		if result.err != nil {
			return socketClientResponseEnvelope{}, true, result.err
		}
		return result.resp, true, nil
	case <-time.After(30 * time.Second):
		c.pending.Delete(reqID)
		return socketClientResponseEnvelope{}, true, &connError{err: fmt.Errorf("websocket request timeout")}
	}
}

// wsReadLoop is the background reader goroutine. It reads all incoming
// messages and dispatches responses/errors to the appropriate pending
// request channel by ID. The conn parameter is captured at launch time
// so the loop is safe from concurrent Close() setting c.wsConn to nil.
func (c *Client) wsReadLoop(conn *websocket.Conn) {
	defer close(c.wsReaderDone)
	for {
		_, rawBytes, err := conn.ReadMessage()
		if err != nil {
			connErr := &connError{err: fmt.Errorf("websocket read failed: %w", err)}
			c.pending.Range(func(key, val any) bool {
				ch := val.(chan wsClientResult)
				select {
				case ch <- wsClientResult{err: connErr}:
				default:
				}
				return true
			})
			c.wsMu.Lock()
			if c.wsConn == conn {
				c.wsConn = nil
				c.wsFailCount++
				c.wsBackoff = time.Now().Add(wsBackoffDuration(c.wsFailCount))
			}
			c.wsMu.Unlock()
			c.handleDisconnectedSubscriptions()
			return
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(rawBytes, &envelope); err != nil {
			log.Printf("api: ws client: invalid envelope: %v", err)
			continue
		}

		switch envelope.Type {
		case "response":
			var resp socketClientResponseEnvelope
			if err := json.Unmarshal(rawBytes, &resp); err != nil {
				log.Printf("api: ws client: invalid response frame: %v", err)
				continue
			}
			if val, ok := c.pending.LoadAndDelete(resp.ID); ok {
				val.(chan wsClientResult) <- wsClientResult{resp: resp}
			}
		case "error":
			var resp socketErrorEnvelope
			if err := json.Unmarshal(rawBytes, &resp); err != nil {
				log.Printf("api: ws client: invalid error frame: %v", err)
				continue
			}
			goErr := wsSocketErrorToGoError(resp)
			if val, ok := c.pending.LoadAndDelete(resp.ID); ok {
				val.(chan wsClientResult) <- wsClientResult{err: goErr}
			}
		case "event":
			var evt SubscriptionEvent
			if err := json.Unmarshal(rawBytes, &evt); err != nil {
				log.Printf("api: ws client: invalid event frame: %v", err)
				continue
			}
			c.routeSubscriptionEvent(evt)
		default:
			// Ignore unknown message types.
		}
	}
}

// wsSocketErrorToGoError converts a WebSocket error envelope to a Go error.
func wsSocketErrorToGoError(resp socketErrorEnvelope) error {
	if resp.Code == "read_only" {
		msg := resp.Message
		if msg == "" {
			msg = "mutations disabled (read-only server)"
		}
		return &readOnlyError{msg: msg}
	}
	if resp.Message != "" {
		return fmt.Errorf("API error: %s", resp.Message)
	}
	if resp.Code != "" {
		return fmt.Errorf("API error: %s", resp.Code)
	}
	return fmt.Errorf("API error")
}

func (c *Client) ensureWSConnLocked() error {
	if c.wsClosed {
		return fmt.Errorf("websocket client closed")
	}
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
	c.wsReaderDone = make(chan struct{})
	go c.wsReadLoop(conn)
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

func (c *Client) effectiveSocketScope(scope *socketScope) *socketScope {
	if scope != nil {
		return scope
	}
	return c.socketScope
}
