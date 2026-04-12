package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandleSSEProxyUsesWebSocketSubscription(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{
			"type": "hello",
		}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		var req struct {
			Type    string         `json:"type"`
			ID      string         `json:"id"`
			Action  string         `json:"action"`
			Scope   map[string]any `json:"scope"`
			Payload map[string]any `json:"payload"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if req.Type != "request" || req.Action != "subscription.start" {
			t.Fatalf("request = %#v, want subscription.start request", req)
		}
		if got := req.Scope["city"]; got != "bright-lights" {
			t.Fatalf("scope.city = %#v, want bright-lights", got)
		}
		if got := req.Payload["kind"]; got != "events" {
			t.Fatalf("payload.kind = %#v, want events", got)
		}
		if got := req.Payload["after_seq"]; got != "12" {
			t.Fatalf("payload.after_seq = %#v, want 12", got)
		}

		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   req.ID,
			"result": map[string]any{
				"subscription_id": "sub-1",
				"kind":            "events",
			},
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
		if err := conn.WriteJSON(map[string]any{
			"type":            "event",
			"subscription_id": "sub-1",
			"event_type":      "session.woke",
			"index":           21,
			"payload": map[string]any{
				"type": "session.woke",
				"seq":  21,
			},
		}); err != nil {
			t.Fatalf("write event: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")
	req := httptest.NewRequest(http.MethodGet, "/api/events?city=bright-lights&after_seq=12", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: connected") {
		t.Fatalf("body missing connected event:\n%s", body)
	}
	if !strings.Contains(body, "event: gc-event") {
		t.Fatalf("body missing gc-event:\n%s", body)
	}
	if !strings.Contains(body, "id: 21") {
		t.Fatalf("body missing event id 21:\n%s", body)
	}
	if !strings.Contains(body, `"type":"session.woke"`) {
		t.Fatalf("body missing event payload:\n%s", body)
	}
}

func TestHandleAgentOutputStreamUsesWebSocketSessionStream(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{
			"type": "hello",
		}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		var req struct {
			Type    string         `json:"type"`
			ID      string         `json:"id"`
			Action  string         `json:"action"`
			Scope   map[string]any `json:"scope"`
			Payload map[string]any `json:"payload"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if req.Type != "request" || req.Action != "subscription.start" {
			t.Fatalf("request = %#v, want subscription.start request", req)
		}
		if got := req.Scope["city"]; got != "bright-lights" {
			t.Fatalf("scope.city = %#v, want bright-lights", got)
		}
		if got := req.Payload["kind"]; got != "session.stream" {
			t.Fatalf("payload.kind = %#v, want session.stream", got)
		}
		if got := req.Payload["target"]; got != "mayor" {
			t.Fatalf("payload.target = %#v, want mayor", got)
		}

		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   req.ID,
			"result": map[string]any{
				"subscription_id": "sub-stream-1",
				"kind":            "session.stream",
			},
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
		if err := conn.WriteJSON(map[string]any{
			"type":            "event",
			"subscription_id": "sub-stream-1",
			"event_type":      "turn",
			"index":           7,
			"payload": map[string]any{
				"id":       "mayor",
				"template": "mayor",
				"format":   "text",
				"turns": []map[string]any{
					{"role": "output", "text": "current city status"},
				},
			},
		}); err != nil {
			t.Fatalf("write event: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")
	req := httptest.NewRequest(http.MethodGet, "/api/agent/output/stream?city=bright-lights&name=mayor", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: turn") {
		t.Fatalf("body missing turn event:\n%s", body)
	}
	if !strings.Contains(body, "id: 7") {
		t.Fatalf("body missing event id 7:\n%s", body)
	}
	if !strings.Contains(body, `"current city status"`) {
		t.Fatalf("body missing streamed session payload:\n%s", body)
	}
}

func TestAPIGetUsesWebSocketForSupportedRoutes(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{
			"type": "hello",
		}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		var first struct {
			Type    string         `json:"type"`
			ID      string         `json:"id"`
			Action  string         `json:"action"`
			Payload map[string]any `json:"payload"`
		}
		if err := conn.ReadJSON(&first); err != nil {
			t.Fatalf("read first request: %v", err)
		}
		if first.Action != "status.get" {
			t.Fatalf("first action = %q, want status.get", first.Action)
		}
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   first.ID,
			"result": map[string]any{
				"name": "test-city",
			},
		}); err != nil {
			t.Fatalf("write first response: %v", err)
		}

		var second struct {
			Type    string         `json:"type"`
			ID      string         `json:"id"`
			Action  string         `json:"action"`
			Payload map[string]any `json:"payload"`
		}
		if err := conn.ReadJSON(&second); err != nil {
			t.Fatalf("read second request: %v", err)
		}
		if second.Action != "beads.list" {
			t.Fatalf("second action = %q, want beads.list", second.Action)
		}
		if got := second.Payload["status"]; got != "in_progress" {
			t.Fatalf("second payload.status = %#v, want in_progress", got)
		}
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   second.ID,
			"result": map[string]any{
				"items": []map[string]any{{"id": "gc-1", "status": "in_progress"}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write second response: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")

	statusBody, err := h.apiGet("/v0/status")
	if err != nil {
		t.Fatalf("apiGet status: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal(statusBody, &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if status["name"] != "test-city" {
		t.Fatalf("status = %#v, want test-city", status["name"])
	}

	beadsBody, err := h.apiGet("/v0/beads?status=in_progress")
	if err != nil {
		t.Fatalf("apiGet beads: %v", err)
	}
	if !strings.Contains(string(beadsBody), `"in_progress"`) {
		t.Fatalf("beads body = %s, want in_progress payload", beadsBody)
	}
}

func TestAPIGetUsesWebSocketForMailAndSessionReads(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "mail.get", map[string]any{"id": "mail-1"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"id":      "mail-1",
				"subject": "Review needed",
				"body":    "Please review",
			},
		}); err != nil {
			t.Fatalf("write mail response: %v", err)
		}

		expectRequestAction(t, conn, "session.pending", map[string]any{"id": "sess-1"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-2",
			"result": map[string]any{
				"supported": true,
				"pending": map[string]any{
					"request_id": "req-1",
					"kind":       "approval",
					"prompt":     "approve?",
				},
			},
		}); err != nil {
			t.Fatalf("write pending response: %v", err)
		}

		expectRequestAction(t, conn, "session.transcript", map[string]any{"id": "sess-1", "tail": float64(1)})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-3",
			"result": map[string]any{
				"id":       "sess-1",
				"template": "mayor",
				"format":   "text",
				"turns": []map[string]any{
					{"role": "output", "text": "current city status"},
				},
			},
		}); err != nil {
			t.Fatalf("write transcript response: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")

	mailBody, err := h.apiGet("/v0/mail/mail-1")
	if err != nil {
		t.Fatalf("apiGet mail: %v", err)
	}
	if !strings.Contains(string(mailBody), `"Review needed"`) {
		t.Fatalf("mail body = %s, want Review needed", mailBody)
	}

	pendingBody, err := h.apiGet("/v0/session/sess-1/pending")
	if err != nil {
		t.Fatalf("apiGet pending: %v", err)
	}
	if !strings.Contains(string(pendingBody), `"request_id":"req-1"`) {
		t.Fatalf("pending body = %s, want req-1 payload", pendingBody)
	}

	transcriptBody, err := h.apiGet("/v0/session/sess-1/transcript?tail=1")
	if err != nil {
		t.Fatalf("apiGet transcript: %v", err)
	}
	if !strings.Contains(string(transcriptBody), `"current city status"`) {
		t.Fatalf("transcript body = %s, want transcript payload", transcriptBody)
	}
}

func TestAPIHandlerMailMutationsUseWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "mail.get", map[string]any{"id": "mail-1"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"id":         "mail-1",
				"from":       "mayor",
				"to":         "worker",
				"subject":    "Review needed",
				"body":       "Please review",
				"created_at": time.Now().Format(time.RFC3339),
			},
		}); err != nil {
			t.Fatalf("write mail.get response: %v", err)
		}

		expectRequestAction(t, conn, "mail.read", map[string]any{"id": "mail-1"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-2",
			"result": map[string]any{
				"status": "read",
			},
		}); err != nil {
			t.Fatalf("write mail.read response: %v", err)
		}

		expectRequestAction(t, conn, "mail.send", map[string]any{
			"from":    "dashboard",
			"to":      "worker",
			"subject": "New review",
			"body":    "Please review this too",
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-3",
			"result": map[string]any{
				"id":      "mail-2",
				"subject": "New review",
			},
		}); err != nil {
			t.Fatalf("write mail.send response: %v", err)
		}

		expectRequestAction(t, conn, "mail.reply", map[string]any{
			"id":      "mail-1",
			"from":    "dashboard",
			"subject": "Re: Review needed",
			"body":    "On it",
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-4",
			"result": map[string]any{
				"id":      "mail-3",
				"subject": "Re: Review needed",
			},
		}); err != nil {
			t.Fatalf("write mail.reply response: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")

	readReq := httptest.NewRequest(http.MethodGet, "/api/mail/read?id=mail-1", nil)
	readRec := httptest.NewRecorder()
	h.handleMailRead(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("handleMailRead status = %d, want 200; body=%s", readRec.Code, readRec.Body.String())
	}
	if !strings.Contains(readRec.Body.String(), `"Review needed"`) {
		t.Fatalf("handleMailRead body = %s, want websocket-backed message", readRec.Body.String())
	}

	sendReq := httptest.NewRequest(http.MethodPost, "/api/mail/send", strings.NewReader(`{"to":"worker","subject":"New review","body":"Please review this too"}`))
	sendReq.Header.Set("Content-Type", "application/json")
	sendRec := httptest.NewRecorder()
	h.handleMailSend(sendRec, sendReq)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("handleMailSend status = %d, want 200; body=%s", sendRec.Code, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), `"success":true`) {
		t.Fatalf("handleMailSend body = %s, want success", sendRec.Body.String())
	}

	replyReq := httptest.NewRequest(http.MethodPost, "/api/mail/send", strings.NewReader(`{"to":"worker","reply_to":"mail-1","subject":"Re: Review needed","body":"On it"}`))
	replyReq.Header.Set("Content-Type", "application/json")
	replyRec := httptest.NewRecorder()
	h.handleMailSend(replyRec, replyReq)
	if replyRec.Code != http.StatusOK {
		t.Fatalf("handleMailReply status = %d, want 200; body=%s", replyRec.Code, replyRec.Body.String())
	}
	if !strings.Contains(replyRec.Body.String(), `"success":true`) {
		t.Fatalf("handleMailReply body = %s, want success", replyRec.Body.String())
	}
}

func TestAPIHandlerAgentOutputUsesWebSocketTranscript(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "session.transcript", map[string]any{
			"id":     "mayor",
			"tail":   float64(1),
			"before": "cursor-1",
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"turns": []map[string]any{{"text": "current city status"}},
			},
		}); err != nil {
			t.Fatalf("write transcript response: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")
	req := httptest.NewRequest(http.MethodGet, "/api/agent/output?name=mayor&tail=1&before=cursor-1", nil)
	rec := httptest.NewRecorder()
	h.handleAgentOutput(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleAgentOutput status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"current city status"`) {
		t.Fatalf("handleAgentOutput body = %s, want websocket transcript payload", rec.Body.String())
	}
}

func TestAPIHandlerIssueMutationsAndSlingUseWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "bead.create", map[string]any{
			"title":       "New issue",
			"description": "body",
			"rig":         "myrig",
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"id": "gc-1",
			},
		}); err != nil {
			t.Fatalf("write bead.create response: %v", err)
		}

		expectRequestAction(t, conn, "bead.close", map[string]any{
			"id": "gc-1",
		})
		if err := conn.WriteJSON(map[string]any{
			"type":   "response",
			"id":     "cli-2",
			"result": map[string]any{"status": "closed"},
		}); err != nil {
			t.Fatalf("write bead.close response: %v", err)
		}

		expectRequestAction(t, conn, "bead.update", map[string]any{
			"id":       "gc-1",
			"assignee": "worker",
		})
		if err := conn.WriteJSON(map[string]any{
			"type":   "response",
			"id":     "cli-3",
			"result": map[string]any{"status": "updated"},
		}); err != nil {
			t.Fatalf("write bead.update response: %v", err)
		}

		expectRequestAction(t, conn, "sling.run", map[string]any{
			"target": "myrig/worker",
			"bead":   "gc-1",
			"rig":    "myrig",
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-4",
			"result": map[string]any{
				"status": "slung",
				"target": "myrig/worker",
				"bead":   "gc-1",
			},
		}); err != nil {
			t.Fatalf("write sling.run response: %v", err)
		}
	}))
	defer upstream.Close()

	h := NewAPIHandler("/tmp/city", "test-city", upstream.URL, "", 5*time.Second, 10*time.Second, "csrf-token")

	createReq := httptest.NewRequest(http.MethodPost, "/api/issues/create", strings.NewReader(`{"title":"New issue","description":"body","rig":"myrig"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	h.handleIssueCreate(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("handleIssueCreate status = %d, want 200; body=%s", createRec.Code, createRec.Body.String())
	}
	if !strings.Contains(createRec.Body.String(), `"id":"gc-1"`) {
		t.Fatalf("handleIssueCreate body = %s, want websocket result id", createRec.Body.String())
	}

	closeReq := httptest.NewRequest(http.MethodPost, "/api/issues/close", strings.NewReader(`{"id":"gc-1"}`))
	closeReq.Header.Set("Content-Type", "application/json")
	closeRec := httptest.NewRecorder()
	h.handleIssueClose(closeRec, closeReq)
	if closeRec.Code != http.StatusOK {
		t.Fatalf("handleIssueClose status = %d, want 200; body=%s", closeRec.Code, closeRec.Body.String())
	}
	if !strings.Contains(closeRec.Body.String(), `"success":true`) {
		t.Fatalf("handleIssueClose body = %s, want success", closeRec.Body.String())
	}

	updateReq := httptest.NewRequest(http.MethodPost, "/api/issues/update", strings.NewReader(`{"id":"gc-1","assignee":"worker"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	h.handleIssueUpdate(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("handleIssueUpdate status = %d, want 200; body=%s", updateRec.Code, updateRec.Body.String())
	}
	if !strings.Contains(updateRec.Body.String(), `"success":true`) {
		t.Fatalf("handleIssueUpdate body = %s, want success", updateRec.Body.String())
	}

	output, ok := h.runViaAPI("sling gc-1 myrig/worker --rig=myrig")
	if !ok {
		t.Fatal("runViaAPI did not handle sling command")
	}
	if !strings.Contains(output, `"status": "slung"`) && !strings.Contains(output, `"status":"slung"`) {
		t.Fatalf("runViaAPI output = %s, want websocket-backed sling response", output)
	}
}

func TestAPIFetcherFetchHealthUsesWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "status.get", nil)
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"name": "test-city",
			},
		}); err != nil {
			t.Fatalf("write status response: %v", err)
		}

		expectRequestAction(t, conn, "sessions.list", map[string]any{
			"state": "active",
			"peek":  true,
		})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-2",
			"result": map[string]any{
				"items": []map[string]any{
					{"template": "myrig/worker", "running": true, "rig": "myrig", "pool": "worker"},
					{"template": "mayor", "running": false, "rig": "", "pool": ""},
				},
				"total": 2,
			},
		}); err != nil {
			t.Fatalf("write sessions response: %v", err)
		}
	}))
	defer upstream.Close()

	fetcher := NewAPIFetcher(upstream.URL, "/tmp/city", "test-city")
	health, err := fetcher.FetchHealth()
	if err != nil {
		t.Fatalf("FetchHealth: %v", err)
	}
	if !health.HeartbeatFresh || health.DeaconHeartbeat != "active" {
		t.Fatalf("health = %#v, want active heartbeat", health)
	}
	if health.HealthyAgents != 1 || health.UnhealthyAgents != 1 {
		t.Fatalf("health counts = %#v, want 1 healthy and 1 unhealthy", health)
	}
}

func TestAPIFetcherFetchIssuesUsesWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "rigs.list", nil)
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"items": []map[string]any{{"name": "myrig"}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write rigs response: %v", err)
		}

		expectRequestAction(t, conn, "beads.list", map[string]any{"status": "open", "rig": "myrig"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-2",
			"result": map[string]any{
				"items": []map[string]any{{"id": "gc-1", "title": "Open issue", "status": "open", "type": "task", "created_at": time.Now().Format(time.RFC3339)}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write open beads response: %v", err)
		}

		expectRequestAction(t, conn, "beads.list", map[string]any{"status": "in_progress", "rig": "myrig"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-3",
			"result": map[string]any{
				"items": []map[string]any{{"id": "gc-2", "title": "Working issue", "status": "in_progress", "type": "task", "created_at": time.Now().Format(time.RFC3339)}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write in-progress beads response: %v", err)
		}
	}))
	defer upstream.Close()

	fetcher := NewAPIFetcher(upstream.URL, "/tmp/city", "test-city")
	issues, err := fetcher.FetchIssues()
	if err != nil {
		t.Fatalf("FetchIssues: %v", err)
	}
	if len(issues) != 2 {
		t.Fatalf("len(issues) = %d, want 2", len(issues))
	}
	gotTitles := map[string]bool{}
	for _, issue := range issues {
		gotTitles[issue.Title] = true
		if issue.Rig != "myrig" {
			t.Fatalf("issue rig = %q, want myrig", issue.Rig)
		}
	}
	if !gotTitles["Open issue"] || !gotTitles["Working issue"] {
		t.Fatalf("issues = %#v, want both websocket-backed backlog rows", issues)
	}
}

func TestAPIFetcherFetchMailActivityAndServicesUseWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			t.Fatalf("unexpected HTTP path %s; expected websocket-only transport", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
			t.Fatalf("write hello: %v", err)
		}

		expectRequestAction(t, conn, "mail.list", nil)
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"items": []map[string]any{{
					"id":         "gc-1",
					"from":       "mayor",
					"to":         "worker",
					"subject":    "Review needed",
					"body":       "Please review",
					"created_at": time.Now().Format(time.RFC3339),
					"read":       false,
				}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write mail response: %v", err)
		}

		expectRequestAction(t, conn, "events.list", map[string]any{"since": "1h"})
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-2",
			"result": map[string]any{
				"items": []map[string]any{{
					"seq":   1,
					"type":  "session.woke",
					"ts":    time.Now().Format(time.RFC3339),
					"actor": "gc",
				}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write events response: %v", err)
		}

		expectRequestAction(t, conn, "services.list", nil)
		if err := conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-3",
			"result": map[string]any{
				"items": []map[string]any{{
					"service_name": "review-intake",
					"state":        "running",
					"local_state":  "running",
				}},
				"total": 1,
			},
		}); err != nil {
			t.Fatalf("write services response: %v", err)
		}
	}))
	defer upstream.Close()

	fetcher := NewAPIFetcher(upstream.URL, "/tmp/city", "test-city")

	mailRows, err := fetcher.FetchMail()
	if err != nil {
		t.Fatalf("FetchMail: %v", err)
	}
	if len(mailRows) != 1 || mailRows[0].Subject != "Review needed" {
		t.Fatalf("mailRows = %#v, want websocket-backed mail row", mailRows)
	}

	activityRows, err := fetcher.FetchActivity()
	if err != nil {
		t.Fatalf("FetchActivity: %v", err)
	}
	if len(activityRows) != 1 || activityRows[0].Type != "session.woke" {
		t.Fatalf("activityRows = %#v, want websocket-backed activity row", activityRows)
	}

	serviceRows, err := fetcher.FetchServices()
	if err != nil {
		t.Fatalf("FetchServices: %v", err)
	}
	if len(serviceRows) != 1 || serviceRows[0].Name != "review-intake" {
		t.Fatalf("serviceRows = %#v, want websocket-backed service row", serviceRows)
	}
}

func expectRequestAction(t *testing.T, conn *websocket.Conn, wantAction string, wantPayload map[string]any) {
	t.Helper()
	var req struct {
		Type    string         `json:"type"`
		ID      string         `json:"id"`
		Action  string         `json:"action"`
		Scope   *gcapiScope    `json:"scope"`
		Payload map[string]any `json:"payload"`
	}
	if err := conn.ReadJSON(&req); err != nil {
		t.Fatalf("read request: %v", err)
	}
	if req.Type != "request" {
		t.Fatalf("request type = %q, want request", req.Type)
	}
	if req.Action != wantAction {
		t.Fatalf("request action = %q, want %q", req.Action, wantAction)
	}
	for key, want := range wantPayload {
		if got := req.Payload[key]; got != want {
			t.Fatalf("payload[%q] = %#v, want %#v", key, got, want)
		}
	}
}

type gcapiScope struct {
	City string `json:"city,omitempty"`
}
