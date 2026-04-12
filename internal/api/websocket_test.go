package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type wsHelloEnvelope struct {
	Type         string   `json:"type"`
	Protocol     string   `json:"protocol"`
	ServerRole   string   `json:"server_role"`
	ReadOnly     bool     `json:"read_only"`
	Capabilities []string `json:"capabilities"`
}

type wsRequestEnvelope struct {
	Type    string      `json:"type"`
	ID      string      `json:"id"`
	Action  string      `json:"action"`
	Scope   *wsScope    `json:"scope,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

type wsScope struct {
	City string `json:"city,omitempty"`
}

type wsResponseEnvelope struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Index  uint64          `json:"index,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

type wsErrorEnvelope struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func TestServerWebSocketHello(t *testing.T) {
	state := newFakeState(t)
	srv := New(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()

	var hello wsHelloEnvelope
	readWSJSON(t, conn, &hello)
	if hello.Type != "hello" {
		t.Fatalf("hello type = %q, want hello", hello.Type)
	}
	if hello.Protocol == "" {
		t.Fatal("hello protocol empty")
	}
	if hello.ServerRole != "city" {
		t.Fatalf("hello server_role = %q, want city", hello.ServerRole)
	}
	if hello.ReadOnly {
		t.Fatal("hello read_only = true, want false")
	}
	if len(hello.Capabilities) == 0 {
		t.Fatal("hello capabilities empty")
	}
}

func TestServerWebSocketHealthGet(t *testing.T) {
	state := newFakeState(t)
	srv := New(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()
	drainWSHello(t, conn)

	writeWSJSON(t, conn, wsRequestEnvelope{
		Type:   "request",
		ID:     "req-1",
		Action: "health.get",
	})

	var resp wsResponseEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "response" {
		t.Fatalf("response type = %q, want response", resp.Type)
	}
	if resp.ID != "req-1" {
		t.Fatalf("response id = %q, want req-1", resp.ID)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(resp.Result, &body); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status = %#v, want ok", body["status"])
	}
	if body["city"] != "test-city" {
		t.Fatalf("city = %#v, want test-city", body["city"])
	}
}

func TestReadOnlyServerWebSocketRejectsCityPatch(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := NewReadOnly(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()
	drainWSHello(t, conn)

	writeWSJSON(t, conn, wsRequestEnvelope{
		Type:   "request",
		ID:     "req-2",
		Action: "city.patch",
		Payload: map[string]any{
			"suspended": true,
		},
	})

	var resp wsErrorEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "error" {
		t.Fatalf("error type = %q, want error", resp.Type)
	}
	if resp.ID != "req-2" {
		t.Fatalf("error id = %q, want req-2", resp.ID)
	}
	if resp.Code != "read_only" {
		t.Fatalf("error code = %q, want read_only", resp.Code)
	}
	if !strings.Contains(resp.Message, "mutations disabled") {
		t.Fatalf("error message = %q, want mutations disabled", resp.Message)
	}
}

func TestSupervisorWebSocketHelloAndCitiesList(t *testing.T) {
	s1 := newFakeState(t)
	s1.cityName = "alpha"
	s2 := newFakeState(t)
	s2.cityName = "beta"

	sm := newTestSupervisorMux(t, map[string]*fakeState{
		"alpha": s1,
		"beta":  s2,
	})
	ts := httptest.NewServer(sm.Handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()

	var hello wsHelloEnvelope
	readWSJSON(t, conn, &hello)
	if hello.Type != "hello" {
		t.Fatalf("hello type = %q, want hello", hello.Type)
	}
	if hello.ServerRole != "supervisor" {
		t.Fatalf("hello server_role = %q, want supervisor", hello.ServerRole)
	}

	writeWSJSON(t, conn, wsRequestEnvelope{
		Type:   "request",
		ID:     "req-3",
		Action: "cities.list",
	})

	var resp wsResponseEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "response" {
		t.Fatalf("response type = %q, want response", resp.Type)
	}
	if resp.ID != "req-3" {
		t.Fatalf("response id = %q, want req-3", resp.ID)
	}

	var body struct {
		Items []CityInfo `json:"items"`
		Total int        `json:"total"`
	}
	if err := json.Unmarshal(resp.Result, &body); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if body.Total != 2 {
		t.Fatalf("total = %d, want 2", body.Total)
	}
	if len(body.Items) != 2 || body.Items[0].Name != "alpha" || body.Items[1].Name != "beta" {
		t.Fatalf("items = %#v, want alpha then beta", body.Items)
	}
}

func TestServerWebSocketUnknownActionReturnsCorrelatedError(t *testing.T) {
	state := newFakeState(t)
	srv := New(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()
	drainWSHello(t, conn)

	writeWSJSON(t, conn, wsRequestEnvelope{
		Type:   "request",
		ID:     "req-unknown",
		Action: "totally.unknown",
	})

	var resp wsErrorEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "error" {
		t.Fatalf("error type = %q, want error", resp.Type)
	}
	if resp.ID != "req-unknown" {
		t.Fatalf("error id = %q, want req-unknown", resp.ID)
	}
	if resp.Code != "not_found" {
		t.Fatalf("error code = %q, want not_found", resp.Code)
	}
	if !strings.Contains(resp.Message, "unknown action") {
		t.Fatalf("error message = %q, want unknown action", resp.Message)
	}
}

func TestServerWebSocketRejectsMalformedRequestEnvelope(t *testing.T) {
	state := newFakeState(t)
	srv := New(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()
	drainWSHello(t, conn)

	writeWSJSON(t, conn, map[string]any{
		"type":   "request",
		"id":     "req-bad",
		"action": "",
	})

	var resp wsErrorEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "error" {
		t.Fatalf("error type = %q, want error", resp.Type)
	}
	if resp.ID != "req-bad" {
		t.Fatalf("error id = %q, want req-bad", resp.ID)
	}
	if resp.Code != "invalid" {
		t.Fatalf("error code = %q, want invalid", resp.Code)
	}
	if !strings.Contains(resp.Message, "required") {
		t.Fatalf("error message = %q, want request id and action are required", resp.Message)
	}
}

func TestServerWebSocketRejectsNonRequestMessageType(t *testing.T) {
	state := newFakeState(t)
	srv := New(state)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	conn := dialWebSocket(t, ts.URL+"/v0/ws")
	defer conn.Close()
	drainWSHello(t, conn)

	writeWSJSON(t, conn, map[string]any{
		"type":   "event",
		"id":     "req-wrong-type",
		"action": "health.get",
	})

	var resp wsErrorEnvelope
	readWSJSON(t, conn, &resp)
	if resp.Type != "error" {
		t.Fatalf("error type = %q, want error", resp.Type)
	}
	if resp.ID != "req-wrong-type" {
		t.Fatalf("error id = %q, want req-wrong-type", resp.ID)
	}
	if resp.Code != "invalid" {
		t.Fatalf("error code = %q, want invalid", resp.Code)
	}
	if !strings.Contains(resp.Message, "message type must be request") {
		t.Fatalf("error message = %q, want message type must be request", resp.Message)
	}
}

func drainWSHello(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	var hello wsHelloEnvelope
	readWSJSON(t, conn, &hello)
	if hello.Type != "hello" {
		t.Fatalf("drain hello type = %q, want hello", hello.Type)
	}
}

func dialWebSocket(t *testing.T, httpURL string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(httpURL, "http")
	header := http.Header{}
	header.Set("Origin", "http://localhost:3000")
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial websocket: %v (status %s)", err, resp.Status)
		}
		t.Fatalf("dial websocket: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	return conn
}

func writeWSJSON(t *testing.T, conn *websocket.Conn, v interface{}) {
	t.Helper()
	if err := conn.WriteJSON(v); err != nil {
		t.Fatalf("write ws json: %v", err)
	}
}

func readWSJSON(t *testing.T, conn *websocket.Conn, v interface{}) {
	t.Helper()
	if err := conn.ReadJSON(v); err != nil {
		t.Fatalf("read ws json: %v", err)
	}
}
