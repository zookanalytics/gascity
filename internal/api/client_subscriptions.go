package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// SubscriptionEvent represents an event received via a WebSocket subscription.
type SubscriptionEvent struct {
	SubscriptionID string          `json:"subscription_id"`
	EventType      string          `json:"event_type"`
	Index          uint64          `json:"index,omitempty"`
	Cursor         string          `json:"cursor,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

// SubscribeEvents starts an event subscription and delivers events to the
// callback until ctx is cancelled or Unsubscribe is called. Returns the
// subscription ID assigned by the server.
func (c *Client) SubscribeEvents(ctx context.Context, afterSeq uint64, callback func(SubscriptionEvent)) (string, error) {
	payload := SubscriptionStartPayload{Kind: "events"}
	if afterSeq > 0 {
		payload.AfterSeq = afterSeq
	}
	return c.startSubscription(ctx, payload, callback)
}

// SubscribeSessionStream starts a session stream subscription and delivers
// events to the callback. The target identifies the session (bead ID or name).
// Format is optional ("text", "jsonl", etc.). Turns controls how many recent
// turns to replay (0 = all).
func (c *Client) SubscribeSessionStream(ctx context.Context, target, format string, turns int, callback func(SubscriptionEvent)) (string, error) {
	payload := SubscriptionStartPayload{
		Kind:   "session.stream",
		Target: target,
		Format: format,
		Turns:  turns,
	}
	return c.startSubscription(ctx, payload, callback)
}

func (c *Client) startSubscription(ctx context.Context, payload SubscriptionStartPayload, callback func(SubscriptionEvent)) (string, error) {
	serverID, used, err := c.startSubscriptionOnSocket(c.effectiveSocketScope(nil), payload)
	if err != nil {
		return "", err
	}
	if !used {
		return "", fmt.Errorf("websocket not available for subscriptions")
	}
	sub := &clientSubscription{
		id:       serverID,
		serverID: serverID,
		scope:    cloneSocketScope(c.effectiveSocketScope(nil)),
		payload:  cloneSubscriptionStartPayload(payload),
		callback: callback,
		ctx:      ctx,
	}
	buffered := c.registerSubscription(sub)

	go func() {
		<-ctx.Done()
		_ = c.unsubscribeSubscription(sub.id, true)
	}()

	for _, evt := range buffered {
		callback(evt)
	}

	return sub.id, nil
}

// Unsubscribe stops a subscription by ID.
func (c *Client) Unsubscribe(subscriptionID string) error {
	return c.unsubscribeSubscription(subscriptionID, false)
}

func (c *Client) unsubscribeSubscription(subscriptionID string, bestEffort bool) error {
	sub, serverID := c.removeSubscription(subscriptionID)
	if sub == nil {
		if bestEffort {
			return nil
		}
		return fmt.Errorf("subscription not found: %s", subscriptionID)
	}
	if serverID == "" {
		return nil
	}
	_, err := c.stopSubscriptionOnSocket(serverID)
	if bestEffort && IsConnError(err) {
		return nil
	}
	return err
}

func (c *Client) registerSubscription(sub *clientSubscription) []SubscriptionEvent {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.subs[sub.id] = sub
	if sub.serverID != "" {
		c.subServerIndex[sub.serverID] = sub.id
	}
	buffered := c.takeBufferedEventsLocked(sub.serverID)
	for i := range buffered {
		buffered[i].SubscriptionID = sub.id
		c.updateSubscriptionCursorLocked(sub, buffered[i])
	}
	return buffered
}

func (c *Client) removeSubscription(subscriptionID string) (*clientSubscription, string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	sub := c.subs[subscriptionID]
	if sub == nil {
		return nil, ""
	}
	delete(c.subs, subscriptionID)
	serverID := sub.serverID
	if serverID != "" {
		delete(c.subServerIndex, serverID)
		delete(c.eventBuf, serverID)
	}
	sub.serverID = ""
	return sub, serverID
}

func (c *Client) startSubscriptionOnSocket(scope *socketScope, payload SubscriptionStartPayload) (string, bool, error) {
	var resp struct {
		SubscriptionID string `json:"subscription_id"`
	}
	used, err := c.doSocketJSON("subscription.start", scope, payload, &resp)
	if err != nil {
		return "", used, err
	}
	if resp.SubscriptionID == "" {
		return "", used, fmt.Errorf("server returned empty subscription_id")
	}
	return resp.SubscriptionID, used, nil
}

func (c *Client) stopSubscriptionOnSocket(subscriptionID string) (bool, error) {
	return c.doSocketJSON("subscription.stop", nil, SubscriptionStopPayload{
		SubscriptionID: subscriptionID,
	}, nil)
}

func (c *Client) routeSubscriptionEvent(evt SubscriptionEvent) {
	cb, routed, shouldBuffer := c.routeSubscriptionEventLocked(evt)
	if shouldBuffer || cb == nil {
		return
	}
	cb(routed)
}

func (c *Client) routeSubscriptionEventLocked(evt SubscriptionEvent) (func(SubscriptionEvent), SubscriptionEvent, bool) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	stableID, ok := c.subServerIndex[evt.SubscriptionID]
	if !ok {
		const maxEventBufPerSubscription = 128
		buf := append(c.eventBuf[evt.SubscriptionID], evt)
		if len(buf) > maxEventBufPerSubscription {
			buf = buf[len(buf)-maxEventBufPerSubscription:]
		}
		c.eventBuf[evt.SubscriptionID] = buf
		return nil, SubscriptionEvent{}, true
	}

	sub := c.subs[stableID]
	if sub == nil {
		delete(c.subServerIndex, evt.SubscriptionID)
		return nil, SubscriptionEvent{}, true
	}
	c.updateSubscriptionCursorLocked(sub, evt)
	evt.SubscriptionID = stableID
	return sub.callback, evt, false
}

func (c *Client) updateSubscriptionCursorLocked(sub *clientSubscription, evt SubscriptionEvent) {
	if evt.Index > 0 {
		sub.lastIndex = evt.Index
	}
	if evt.Cursor != "" {
		sub.lastCursor = evt.Cursor
	}
}

func (c *Client) takeBufferedEventsLocked(serverID string) []SubscriptionEvent {
	if serverID == "" {
		return nil
	}
	buffered := append([]SubscriptionEvent(nil), c.eventBuf[serverID]...)
	delete(c.eventBuf, serverID)
	return buffered
}

func (c *Client) handleDisconnectedSubscriptions() {
	c.subMu.Lock()
	if len(c.subs) == 0 || c.wsClosed {
		c.subServerIndex = make(map[string]string)
		c.eventBuf = make(map[string][]SubscriptionEvent)
		c.subMu.Unlock()
		return
	}
	for _, sub := range c.subs {
		sub.serverID = ""
	}
	c.subServerIndex = make(map[string]string)
	c.eventBuf = make(map[string][]SubscriptionEvent)
	if c.reconnectActive {
		c.subMu.Unlock()
		return
	}
	c.reconnectActive = true
	c.subMu.Unlock()

	go c.reconnectSubscriptionsLoop()
}

func (c *Client) reconnectSubscriptionsLoop() {
	defer func() {
		c.subMu.Lock()
		c.reconnectActive = false
		c.subMu.Unlock()
	}()

	for {
		if c.isClosed() {
			return
		}
		snapshot := c.subscriptionSnapshot()
		if len(snapshot) == 0 {
			return
		}
		if !c.waitForReconnectBackoff() {
			return
		}
		if err := c.reconnectWS(); err != nil {
			continue
		}
		if c.resubscribeAll(snapshot) {
			return
		}
	}
}

func (c *Client) isClosed() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.wsClosed
}

func (c *Client) waitForReconnectBackoff() bool {
	for {
		c.wsMu.Lock()
		if c.wsClosed {
			c.wsMu.Unlock()
			return false
		}
		wait := time.Until(c.wsBackoff)
		c.wsMu.Unlock()
		if wait <= 0 {
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		}
	}
}

func (c *Client) reconnectWS() error {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	if c.wsClosed {
		return fmt.Errorf("websocket client closed")
	}
	if c.wsConn != nil {
		return nil
	}
	if err := c.ensureWSConnLocked(); err != nil {
		c.wsFailCount++
		c.wsBackoff = time.Now().Add(wsBackoffDuration(c.wsFailCount))
		log.Printf("api: ws reconnect failed (attempt %d, backoff %s): %v", c.wsFailCount, wsBackoffDuration(c.wsFailCount), err)
		return err
	}
	c.wsFailCount = 0
	c.wsBackoff = time.Time{}
	return nil
}

func (c *Client) resubscribeAll(snapshot []*clientSubscription) bool {
	for _, sub := range snapshot {
		if !c.subscriptionStillActive(sub.id, sub.ctx) {
			continue
		}
		payload := c.resumePayload(sub)
		serverID, _, err := c.startSubscriptionOnSocket(sub.scope, payload)
		if err != nil {
			if IsConnError(err) {
				return false
			}
			log.Printf("api: ws resubscribe failed id=%s kind=%v: %v", sub.id, payload.Kind, err)
			return false
		}
		buffered, current := c.remapSubscription(sub.id, serverID)
		if !current {
			_, _ = c.stopSubscriptionOnSocket(serverID)
			continue
		}
		for _, evt := range buffered {
			sub.callback(evt)
		}
	}
	return true
}

func (c *Client) subscriptionSnapshot() []*clientSubscription {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	snapshot := make([]*clientSubscription, 0, len(c.subs))
	for _, sub := range c.subs {
		snapshot = append(snapshot, &clientSubscription{
			id:         sub.id,
			serverID:   sub.serverID,
			scope:      cloneSocketScope(sub.scope),
			payload:    cloneSubscriptionStartPayload(sub.payload),
			callback:   sub.callback,
			ctx:        sub.ctx,
			lastIndex:  sub.lastIndex,
			lastCursor: sub.lastCursor,
		})
	}
	return snapshot
}

func (c *Client) subscriptionStillActive(subscriptionID string, ctx context.Context) bool {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return false
		default:
		}
	}
	c.subMu.Lock()
	defer c.subMu.Unlock()
	_, ok := c.subs[subscriptionID]
	return ok
}

func (c *Client) resumePayload(sub *clientSubscription) SubscriptionStartPayload {
	payload := cloneSubscriptionStartPayload(sub.payload)
	switch payload.Kind {
	case "events":
		if sub.lastCursor != "" {
			payload.AfterCursor = sub.lastCursor
			payload.AfterSeq = 0
		} else if sub.lastIndex > 0 {
			payload.AfterSeq = sub.lastIndex
		}
	case "session.stream":
		if sub.lastCursor != "" {
			payload.AfterCursor = sub.lastCursor
			payload.AfterSeq = 0
		}
	}
	return payload
}

func (c *Client) remapSubscription(subscriptionID, serverID string) ([]SubscriptionEvent, bool) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	sub := c.subs[subscriptionID]
	if sub == nil {
		return nil, false
	}
	if sub.ctx != nil {
		select {
		case <-sub.ctx.Done():
			return nil, false
		default:
		}
	}
	if sub.serverID != "" {
		delete(c.subServerIndex, sub.serverID)
	}
	sub.serverID = serverID
	c.subServerIndex[serverID] = subscriptionID
	buffered := c.takeBufferedEventsLocked(serverID)
	for i := range buffered {
		buffered[i].SubscriptionID = subscriptionID
		c.updateSubscriptionCursorLocked(sub, buffered[i])
	}
	return buffered, true
}

func cloneSubscriptionStartPayload(src SubscriptionStartPayload) SubscriptionStartPayload {
	return src
}

func cloneSocketScope(scope *socketScope) *socketScope {
	if scope == nil {
		return nil
	}
	clone := *scope
	return &clone
}
