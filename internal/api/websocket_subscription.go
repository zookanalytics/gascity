package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gorilla/websocket"
)

const (
	socketPingInterval = 15 * time.Second
	socketPongWait     = 45 * time.Second
)

type socketSubscriptionStartPayload struct {
	Kind        string `json:"kind"`
	AfterSeq    uint64 `json:"after_seq,omitempty"`
	AfterCursor string `json:"after_cursor,omitempty"`
	Target      string `json:"target,omitempty"`
	Format      string `json:"format,omitempty"`
}

type socketSubscriptionStopPayload struct {
	SubscriptionID string `json:"subscription_id"`
}

type socketEventEnvelope struct {
	Type           string `json:"type"`
	SubscriptionID string `json:"subscription_id"`
	EventType      string `json:"event_type"`
	Index          uint64 `json:"index,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
	Payload        any    `json:"payload,omitempty"`
}

type socketSession struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conn      *socketConn
	mu        sync.Mutex
	nextSubID uint64
	subs      map[string]context.CancelFunc
}

func newSocketSession(parent context.Context, conn *socketConn) *socketSession {
	ctx, cancel := context.WithCancel(parent)
	return &socketSession{
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		subs:   make(map[string]context.CancelFunc),
	}
}

func (s *socketSession) close() {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, cancel := range s.subs {
		cancel()
		delete(s.subs, id)
	}
}

func (s *socketSession) runPingLoop() {
	ticker := time.NewTicker(socketPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.conn.writePing(); err != nil {
				s.cancel()
				return
			}
		}
	}
}

func (s *socketSession) newSubscriptionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSubID++
	return fmt.Sprintf("sub-%d", s.nextSubID)
}

func (s *socketSession) registerSubscription(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[id] = cancel
}

func (s *socketSession) unregisterSubscription(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, id)
}

func (s *socketSession) stopSubscription(id string) bool {
	s.mu.Lock()
	cancel, ok := s.subs[id]
	if ok {
		delete(s.subs, id)
	}
	s.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

func (sc *socketConn) writePing() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
}

func (s *Server) startSocketSubscription(ctx context.Context, sess *socketSession, req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	var payload socketSubscriptionStartPayload
	if err := decodeSocketPayload(req.Payload, &payload); err != nil {
		return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
	}
	switch payload.Kind {
	case "events":
		return s.startEventSubscription(ctx, sess, req, payload)
	case "session.stream":
		return s.startSessionStreamSubscription(ctx, sess, req, payload)
	default:
		return socketActionResult{}, newSocketError(req.ID, "not_found", "unknown subscription kind: "+payload.Kind)
	}
}

func (s *Server) stopSocketSubscription(sess *socketSession, req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	var payload socketSubscriptionStopPayload
	if err := decodeSocketPayload(req.Payload, &payload); err != nil {
		return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
	}
	if payload.SubscriptionID == "" {
		return socketActionResult{}, newSocketError(req.ID, "invalid", "subscription_id is required")
	}
	if !sess.stopSubscription(payload.SubscriptionID) {
		return socketActionResult{}, newSocketError(req.ID, "not_found", "subscription not found: "+payload.SubscriptionID)
	}
	return socketActionResult{Result: map[string]string{"status": "ok", "subscription_id": payload.SubscriptionID}}, nil
}

func (sm *SupervisorMux) startSocketSubscription(ctx context.Context, sess *socketSession, req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	var payload socketSubscriptionStartPayload
	if err := decodeSocketPayload(req.Payload, &payload); err != nil {
		return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
	}
	switch payload.Kind {
	case "events":
		if req.Scope != nil && req.Scope.City != "" {
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
			return srv.startSocketSubscription(ctx, sess, &cityReq)
		}
		return sm.startGlobalEventSubscription(ctx, sess, req, payload)
	case "session.stream":
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
		return srv.startSocketSubscription(ctx, sess, &cityReq)
	default:
		return socketActionResult{}, newSocketError(req.ID, "not_found", "unknown subscription kind: "+payload.Kind)
	}
}

func (sm *SupervisorMux) stopSocketSubscription(sess *socketSession, req *socketRequestEnvelope) (socketActionResult, *socketErrorEnvelope) {
	var payload socketSubscriptionStopPayload
	if err := decodeSocketPayload(req.Payload, &payload); err != nil {
		return socketActionResult{}, newSocketError(req.ID, "invalid", err.Error())
	}
	if payload.SubscriptionID == "" {
		return socketActionResult{}, newSocketError(req.ID, "invalid", "subscription_id is required")
	}
	if !sess.stopSubscription(payload.SubscriptionID) {
		return socketActionResult{}, newSocketError(req.ID, "not_found", "subscription not found: "+payload.SubscriptionID)
	}
	return socketActionResult{Result: map[string]string{"status": "ok", "subscription_id": payload.SubscriptionID}}, nil
}

func (s *Server) startEventSubscription(parent context.Context, sess *socketSession, req *socketRequestEnvelope, payload socketSubscriptionStartPayload) (socketActionResult, *socketErrorEnvelope) {
	ep := s.state.EventProvider()
	if ep == nil {
		return socketActionResult{}, newSocketError(req.ID, "unavailable", "events not enabled")
	}
	subID := sess.newSubscriptionID()
	subCtx, cancel := context.WithCancel(parent)
	watcher, err := ep.Watch(subCtx, payload.AfterSeq)
	if err != nil {
		cancel()
		return socketActionResult{}, newSocketError(req.ID, "internal", "failed to start event watcher: "+err.Error())
	}
	sess.registerSubscription(subID, cancel)
	return socketActionResult{
		Result: map[string]string{"subscription_id": subID, "kind": payload.Kind},
		AfterWrite: func() {
			go func() {
				defer watcher.Close() //nolint:errcheck
				defer cancel()
				defer sess.unregisterSubscription(subID)
				for {
					event, err := watcher.Next()
					if err != nil {
						return
					}
					envelope := socketEventEnvelope{
						Type:           "event",
						SubscriptionID: subID,
						EventType:      event.Type,
						Index:          event.Seq,
						Payload: eventStreamEnvelope{
							Event:    event,
							Workflow: projectWorkflowEvent(s.state, event),
						},
					}
					if err := sess.conn.writeJSON(envelope); err != nil {
						return
					}
				}
			}()
		},
	}, nil
}

func (sm *SupervisorMux) startGlobalEventSubscription(parent context.Context, sess *socketSession, req *socketRequestEnvelope, payload socketSubscriptionStartPayload) (socketActionResult, *socketErrorEnvelope) {
	subID := sess.newSubscriptionID()
	subCtx, cancel := context.WithCancel(parent)
	mw, err := sm.buildMultiplexer().Watch(subCtx, events.ParseCursor(payload.AfterCursor))
	if err != nil {
		cancel()
		return socketActionResult{}, newSocketError(req.ID, "internal", "failed to start global event watcher: "+err.Error())
	}
	sess.registerSubscription(subID, cancel)
	cursors := events.ParseCursor(payload.AfterCursor)
	if cursors == nil {
		cursors = make(map[string]uint64)
	}
	return socketActionResult{
		Result: map[string]string{"subscription_id": subID, "kind": payload.Kind},
		AfterWrite: func() {
			go func() {
				defer mw.Close() //nolint:errcheck
				defer cancel()
				defer sess.unregisterSubscription(subID)
				for {
					tagged, err := mw.Next()
					if err != nil {
						return
					}
					cursors[tagged.City] = tagged.Seq
					var workflow *workflowEventProjection
					if state := sm.resolver.CityState(tagged.City); state != nil {
						workflow = projectWorkflowEvent(state, tagged.Event)
					}
					envelope := socketEventEnvelope{
						Type:           "event",
						SubscriptionID: subID,
						EventType:      tagged.Type,
						Cursor:         events.FormatCursor(cursors),
						Payload: taggedEventStreamEnvelope{
							TaggedEvent: tagged,
							Workflow:    workflow,
						},
					}
					if err := sess.conn.writeJSON(envelope); err != nil {
						return
					}
				}
			}()
		},
	}, nil
}

func (s *Server) startSessionStreamSubscription(parent context.Context, sess *socketSession, req *socketRequestEnvelope, payload socketSubscriptionStartPayload) (socketActionResult, *socketErrorEnvelope) {
	store := s.state.CityBeadStore()
	if store == nil {
		return socketActionResult{}, newSocketError(req.ID, "unavailable", "no bead store configured")
	}
	if payload.Target == "" {
		return socketActionResult{}, newSocketError(req.ID, "invalid", "target is required")
	}
	id, err := s.resolveSessionIDAllowClosedWithConfig(store, payload.Target)
	if err != nil {
		return socketActionResult{}, newSocketError(req.ID, "not_found", err.Error())
	}

	mgr := s.sessionManager(store)
	info, err := mgr.Get(id)
	if err != nil {
		return socketActionResult{}, socketErrorFor(req.ID, err)
	}
	path, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		return socketActionResult{}, socketErrorFor(req.ID, err)
	}
	sp := s.state.SessionProvider()
	running := info.State == session.StateActive && sp.IsRunning(info.SessionName)
	if path == "" && !running {
		return socketActionResult{}, newSocketError(req.ID, "not_found", "session "+id+" has no live output")
	}

	format := payload.Format
	subID := sess.newSubscriptionID()
	subCtx, cancel := context.WithCancel(parent)
	sess.registerSubscription(subID, cancel)

	start := func() {
		go func() {
			defer cancel()
			defer sess.unregisterSubscription(subID)
			emitter := newSocketSessionStreamEmitter(sess, subID)
			if info.Closed {
				if format == "raw" {
					s.emitClosedSessionSnapshotRawWithEmitter(emitter, info, path)
				} else {
					s.emitClosedSessionSnapshotWithEmitter(emitter, info, path)
				}
				return
			}
			switch {
			case path != "":
				if format == "raw" {
					s.streamSessionTranscriptLogRawWithEmitter(subCtx, emitter, info, path)
				} else {
					s.streamSessionTranscriptLogWithEmitter(subCtx, emitter, info, path)
				}
			case format == "raw":
				if running {
					s.streamSessionPeekRawWithEmitter(subCtx, emitter, info)
				} else {
					data, _ := json.Marshal(sessionRawTranscriptResponse{
						ID:       info.ID,
						Template: info.Template,
						Format:   "raw",
						Messages: []json.RawMessage{},
					})
					_ = emitter.emit("message", 1, data)
				}
			default:
				s.streamSessionPeekWithEmitter(subCtx, emitter, info)
			}
		}()
	}

	return socketActionResult{
		Result: map[string]string{"subscription_id": subID, "kind": payload.Kind},
		AfterWrite: start,
	}, nil
}

func encodeSocketJSON(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
