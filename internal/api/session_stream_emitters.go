package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/sessionlog"
)

type sessionStreamEmitter struct {
	event     func(eventType string, id uint64, data []byte) error
	keepalive func() error
}

func (e sessionStreamEmitter) emit(eventType string, id uint64, data []byte) error {
	if e.event == nil {
		return nil
	}
	return e.event(eventType, id, data)
}

func (e sessionStreamEmitter) comment() error {
	if e.keepalive == nil {
		return nil
	}
	return e.keepalive()
}

func newSSESessionStreamEmitter(w http.ResponseWriter) sessionStreamEmitter {
	return sessionStreamEmitter{
		event: func(eventType string, id uint64, data []byte) error {
			return writeSSEEvent(w, eventType, id, data)
		},
		keepalive: func() error {
			return writeSSECommentLine(w)
		},
	}
}

func newSocketSessionStreamEmitter(sess *socketSession, subscriptionID string) sessionStreamEmitter {
	return sessionStreamEmitter{
		event: func(eventType string, id uint64, data []byte) error {
			payload := json.RawMessage(append([]byte(nil), data...))
			return sess.conn.writeJSON(socketEventEnvelope{
				Type:           "event",
				SubscriptionID: subscriptionID,
				EventType:      eventType,
				Index:          id,
				Payload:        payload,
			})
		},
	}
}

func (s *Server) emitClosedSessionSnapshotWithEmitter(emitter sessionStreamEmitter, info session.Info, logPath string) {
	if logPath == "" {
		return
	}
	sess, err := sessionlog.ReadProviderFile(info.Provider, logPath, 0)
	if err != nil {
		return
	}

	turns := make([]outputTurn, 0, len(sess.Messages))
	for _, entry := range sess.Messages {
		turn := entryToTurn(entry)
		if turn.Text == "" {
			continue
		}
		turns = append(turns, turn)
	}
	if len(turns) == 0 {
		return
	}

	data, err := json.Marshal(sessionTranscriptResponse{
		ID:       info.ID,
		Template: info.Template,
		Format:   "conversation",
		Turns:    turns,
	})
	if err != nil {
		return
	}
	if err := emitter.emit("turn", 1, data); err != nil {
		return
	}
	actData, _ := json.Marshal(map[string]string{"activity": "idle"})
	_ = emitter.emit("activity", 2, actData)
}

func (s *Server) emitClosedSessionSnapshotRawWithEmitter(emitter sessionStreamEmitter, info session.Info, logPath string) {
	if logPath == "" {
		return
	}
	sess, err := sessionlog.ReadProviderFileRaw(info.Provider, logPath, 0)
	if err != nil {
		return
	}

	rawMessages := make([]json.RawMessage, 0, len(sess.Messages))
	for _, entry := range sess.Messages {
		if len(entry.Raw) == 0 {
			continue
		}
		rawMessages = append(rawMessages, entry.Raw)
	}
	if len(rawMessages) == 0 {
		return
	}

	data, err := json.Marshal(sessionRawTranscriptResponse{
		ID:       info.ID,
		Template: info.Template,
		Format:   "raw",
		Messages: rawMessages,
	})
	if err != nil {
		return
	}
	if err := emitter.emit("message", 1, data); err != nil {
		return
	}
	actData, _ := json.Marshal(map[string]string{"activity": "idle"})
	_ = emitter.emit("activity", 2, actData)
}

func (s *Server) streamSessionTranscriptLogRawWithEmitter(ctx context.Context, emitter sessionStreamEmitter, info session.Info, logPath string) {
	lw := newLogFileWatcher(logPath)
	defer lw.Close()

	var lastSize int64
	var lastSentUUID string
	var seq uint64
	var lastActivity string
	sentUUIDs := make(map[string]struct{})
	lw.onReset = func() { lastSize = 0; lastActivity = "" }

	readAndEmit := func() {
		stat, err := os.Stat(logPath)
		if err != nil {
			return
		}
		if stat.Size() == lastSize {
			return
		}

		sess, err := sessionlog.ReadProviderFileRaw(info.Provider, logPath, 1)
		if err != nil {
			return
		}
		lastSize = stat.Size()
		activity := sessionlog.InferActivityFromEntries(sess.Messages)

		rawMessages := make([]json.RawMessage, 0, len(sess.Messages))
		uuids := make([]string, 0, len(sess.Messages))
		for _, entry := range sess.Messages {
			if len(entry.Raw) == 0 {
				continue
			}
			rawMessages = append(rawMessages, entry.Raw)
			uuids = append(uuids, entry.UUID)
		}

		if len(rawMessages) > 0 {
			var toSend []json.RawMessage

			if lastSentUUID == "" {
				toSend = rawMessages
			} else {
				found := false
				for i, uuid := range uuids {
					if uuid == lastSentUUID {
						toSend = rawMessages[i+1:]
						found = true
						break
					}
				}
				if !found {
					log.Printf("session stream raw: cursor %s lost, emitting only new messages", lastSentUUID)
					for i, uuid := range uuids {
						if _, seen := sentUUIDs[uuid]; !seen {
							toSend = append(toSend, rawMessages[i])
						}
					}
				}
			}

			if len(toSend) > 0 {
				seq++
				data, err := json.Marshal(sessionRawTranscriptResponse{
					ID:       info.ID,
					Template: info.Template,
					Format:   "raw",
					Messages: toSend,
				})
				if err == nil && emitter.emit("message", seq, data) != nil {
					return
				}
			}

			lastSentUUID = uuids[len(uuids)-1]
			for _, uuid := range uuids {
				sentUUIDs[uuid] = struct{}{}
			}
		}

		if activity != "" && activity != lastActivity {
			lastActivity = activity
			seq++
			actData, _ := json.Marshal(map[string]string{"activity": activity})
			if emitter.emit("activity", seq, actData) != nil {
				return
			}
		}
	}

	var lastPendingID string
	onStall := func() {
		sp := s.state.SessionProvider()
		ip, ok := sp.(runtime.InteractionProvider)
		if !ok {
			return
		}
		pending, err := ip.Pending(info.SessionName)
		if err != nil || pending == nil {
			if lastPendingID != "" {
				lastPendingID = ""
				seq++
				actData, _ := json.Marshal(map[string]string{"activity": "in-turn"})
				_ = emitter.emit("activity", seq, actData)
			}
			return
		}
		if pending.RequestID == lastPendingID {
			return
		}
		lastPendingID = pending.RequestID
		seq++
		pendingData, _ := json.Marshal(pending)
		_ = emitter.emit("pending", seq, pendingData)
	}

	lw.Run(ctx, readAndEmit, func() { _ = emitter.comment() }, RunOpts{
		OnStall:      onStall,
		StallTimeout: 5 * time.Second,
	})
}

func (s *Server) streamSessionTranscriptLogWithEmitter(ctx context.Context, emitter sessionStreamEmitter, info session.Info, logPath string) {
	lw := newLogFileWatcher(logPath)
	defer lw.Close()

	var lastSize int64
	var lastSentUUID string
	var seq uint64
	var lastActivity string
	sentUUIDs := make(map[string]struct{})
	lw.onReset = func() { lastSize = 0; lastActivity = "" }

	readAndEmit := func() {
		stat, err := os.Stat(logPath)
		if err != nil {
			return
		}
		if stat.Size() == lastSize {
			return
		}

		sess, err := sessionlog.ReadProviderFile(info.Provider, logPath, 0)
		if err != nil {
			return
		}
		lastSize = stat.Size()
		activity := sessionlog.InferActivityFromEntries(sess.Messages)

		turns := make([]outputTurn, 0, len(sess.Messages))
		uuids := make([]string, 0, len(sess.Messages))
		for _, entry := range sess.Messages {
			turn := entryToTurn(entry)
			if turn.Text == "" {
				continue
			}
			turns = append(turns, turn)
			uuids = append(uuids, entry.UUID)
		}

		if len(turns) > 0 {
			var toSend []outputTurn

			if lastSentUUID == "" {
				toSend = turns
			} else {
				found := false
				for i, uuid := range uuids {
					if uuid == lastSentUUID {
						toSend = turns[i+1:]
						found = true
						break
					}
				}
				if !found {
					log.Printf("session stream: cursor %s lost, emitting only new turns", lastSentUUID)
					for i, uuid := range uuids {
						if _, seen := sentUUIDs[uuid]; !seen {
							toSend = append(toSend, turns[i])
						}
					}
				}
			}

			if len(toSend) > 0 {
				seq++
				data, err := json.Marshal(sessionTranscriptResponse{
					ID:       info.ID,
					Template: info.Template,
					Format:   "conversation",
					Turns:    toSend,
				})
				if err == nil && emitter.emit("turn", seq, data) != nil {
					return
				}
			}

			lastSentUUID = uuids[len(uuids)-1]
			for _, uuid := range uuids {
				sentUUIDs[uuid] = struct{}{}
			}
		}

		if activity != "" && activity != lastActivity {
			lastActivity = activity
			seq++
			actData, _ := json.Marshal(map[string]string{"activity": activity})
			if emitter.emit("activity", seq, actData) != nil {
				return
			}
		}
	}

	lw.Run(ctx, readAndEmit, func() { _ = emitter.comment() })
}

func (s *Server) streamSessionPeekRawWithEmitter(ctx context.Context, emitter sessionStreamEmitter, info session.Info) {
	sp := s.state.SessionProvider()
	poll := time.NewTicker(outputStreamPollInterval)
	defer poll.Stop()
	keepalive := time.NewTicker(sseKeepalive)
	defer keepalive.Stop()

	var lastOutput string
	var seq uint64
	var lastPeekPendingID string

	emitPeek := func() {
		if !sp.IsRunning(info.SessionName) {
			return
		}
		output, err := sp.Peek(info.SessionName, 100)
		if err != nil || output == lastOutput {
			return
		}
		lastOutput = output
		seq++

		if output == "" {
			return
		}

		fakeMsg, _ := json.Marshal(map[string]interface{}{
			"role": "assistant",
			"content": []map[string]string{
				{"type": "text", "text": output},
			},
		})
		data, err := json.Marshal(sessionRawTranscriptResponse{
			ID:       info.ID,
			Template: info.Template,
			Format:   "raw",
			Messages: []json.RawMessage{fakeMsg},
		})
		if err != nil {
			return
		}
		if emitter.emit("message", seq, data) != nil {
			return
		}

		if ip, ok := sp.(runtime.InteractionProvider); ok {
			pending, pErr := ip.Pending(info.SessionName)
			if pErr == nil && pending != nil && pending.RequestID != lastPeekPendingID {
				lastPeekPendingID = pending.RequestID
				seq++
				pendingData, _ := json.Marshal(pending)
				_ = emitter.emit("pending", seq, pendingData)
			} else if pending == nil && lastPeekPendingID != "" {
				lastPeekPendingID = ""
			}
		}
	}

	emitPeek()

	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			emitPeek()
		case <-keepalive.C:
			if emitter.comment() != nil {
				return
			}
		}
	}
}

func (s *Server) streamSessionPeekWithEmitter(ctx context.Context, emitter sessionStreamEmitter, info session.Info) {
	sp := s.state.SessionProvider()
	poll := time.NewTicker(outputStreamPollInterval)
	defer poll.Stop()
	keepalive := time.NewTicker(sseKeepalive)
	defer keepalive.Stop()

	var lastOutput string
	var seq uint64

	emitPeek := func() {
		if !sp.IsRunning(info.SessionName) {
			return
		}
		output, err := sp.Peek(info.SessionName, 100)
		if err != nil || output == lastOutput {
			return
		}
		lastOutput = output
		seq++

		turns := []outputTurn{}
		if output != "" {
			turns = append(turns, outputTurn{Role: "output", Text: output})
		}
		data, err := json.Marshal(sessionTranscriptResponse{
			ID:       info.ID,
			Template: info.Template,
			Format:   "text",
			Turns:    turns,
		})
		if err != nil {
			return
		}
		_ = emitter.emit("turn", seq, data)
	}

	emitPeek()

	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			emitPeek()
		case <-keepalive.C:
			if emitter.comment() != nil {
				return
			}
		}
	}
}
