package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const sseKeepalive = 15 * time.Second

// writeSSE writes a single SSE event to w and flushes.
func writeSSE(w http.ResponseWriter, eventType string, id uint64, data []byte) {
	_ = writeSSEEvent(w, eventType, id, data)
}

func writeSSEWithStringID(w http.ResponseWriter, eventType, id string, data []byte) {
	_ = writeSSEStringIDEvent(w, eventType, id, data)
}

// writeSSEComment writes a keepalive comment line and flushes.
func writeSSEComment(w http.ResponseWriter) {
	_ = writeSSECommentLine(w)
}

func writeSSEEvent(w http.ResponseWriter, eventType string, id uint64, data []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n", eventType, id, data); err != nil {
		return err
	}
	return flushSSEWriter(w)
}

func writeSSEStringIDEvent(w http.ResponseWriter, eventType, id string, data []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", eventType, id, data); err != nil {
		return err
	}
	return flushSSEWriter(w)
}

func writeSSECommentLine(w http.ResponseWriter) error {
	if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
		return err
	}
	return flushSSEWriter(w)
}

func flushSSEWriter(w http.ResponseWriter) error {
	if err := http.NewResponseController(w).Flush(); err != nil {
		return err
	}
	return nil
}

// parseAfterSeq reads the reconnect position from Last-Event-ID or ?after_seq.
func parseAfterSeq(r *http.Request) uint64 {
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	if v := r.URL.Query().Get("after_seq"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}
