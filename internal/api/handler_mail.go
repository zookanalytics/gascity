package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/mail"
)

type mailSendRequest struct {
	Rig     string `json:"rig"`
	From    string `json:"from"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type mailReplyRequest struct {
	From    string `json:"from"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func (s *Server) handleMailList(w http.ResponseWriter, r *http.Request) {
	bp := parseBlockingParams(r)
	if bp.isBlocking() {
		waitForChange(r.Context(), s.state.EventProvider(), bp)
	}

	q := r.URL.Query()
	agent := q.Get("agent")
	status := q.Get("status")
	rig := q.Get("rig")
	msgs, err := s.listMailMessages(agent, status, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	pp := parsePagination(r, 50)
	if !pp.IsPaging {
		total := len(msgs)
		if pp.Limit < len(msgs) {
			msgs = msgs[:pp.Limit]
		}
		writeListJSON(w, s.latestIndex(), msgs, total)
		return
	}
	page, total, nextCursor := paginate(msgs, pp)
	if page == nil {
		page = []mail.Message{}
	}
	writePagedJSON(w, s.latestIndex(), page, total, nextCursor)
}

func (s *Server) listMailMessages(agent, status, rig string) ([]mail.Message, error) {
	switch status {
	case "", "unread":
		if rig != "" {
			mp := s.state.MailProvider(rig)
			if mp == nil {
				return []mail.Message{}, nil
			}
			msgs, err := mp.Inbox(agent)
			if err != nil {
				return nil, err
			}
			if msgs == nil {
				msgs = []mail.Message{}
			}
			msgs = tagRig(msgs, rig)
			return msgs, nil
		}

		providers := s.state.MailProviders()
		var allMsgs []mail.Message
		for _, name := range sortedProviderNames(providers) {
			msgs, err := providers[name].Inbox(agent)
			if err != nil {
				return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "mail provider " + name + ": " + err.Error()}
			}
			allMsgs = append(allMsgs, tagRig(msgs, name)...)
		}
		if allMsgs == nil {
			allMsgs = []mail.Message{}
		}
		return allMsgs, nil
	case "all":
		if rig != "" {
			mp := s.state.MailProvider(rig)
			if mp == nil {
				return []mail.Message{}, nil
			}
			msgs, err := mp.All(agent)
			if err != nil {
				return nil, err
			}
			if msgs == nil {
				msgs = []mail.Message{}
			}
			msgs = tagRig(msgs, rig)
			return msgs, nil
		}

		providers := s.state.MailProviders()
		var allMsgs []mail.Message
		for _, name := range sortedProviderNames(providers) {
			msgs, err := providers[name].All(agent)
			if err != nil {
				return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "mail provider " + name + ": " + err.Error()}
			}
			allMsgs = append(allMsgs, tagRig(msgs, name)...)
		}
		if allMsgs == nil {
			allMsgs = []mail.Message{}
		}
		return allMsgs, nil
	default:
		return nil, httpError{status: http.StatusBadRequest, code: "invalid", message: "unsupported status filter: " + status + "; supported: unread, all"}
	}
	return []mail.Message{}, nil
}

func (s *Server) handleMailGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	msg, err := s.getMailMessage(id, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeIndexJSON(w, s.latestIndex(), msg)
}

func (s *Server) getMailMessage(id, rig string) (mail.Message, error) {
	mp, resolvedRig, err := s.findMailProviderForMessage(id, rig)
	if err != nil {
		return mail.Message{}, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	if mp == nil {
		return mail.Message{}, httpError{status: http.StatusNotFound, code: "not_found", message: "message " + id + " not found"}
	}

	msg, err := mp.Get(id)
	if err != nil {
		if errors.Is(err, mail.ErrNotFound) {
			return mail.Message{}, httpError{status: http.StatusNotFound, code: "not_found", message: err.Error()}
		}
		return mail.Message{}, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	msg.Rig = resolvedRig
	return msg, nil
}

func (s *Server) handleMailSend(w http.ResponseWriter, r *http.Request) {
	var body mailSendRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	// Idempotency check — key is scoped by method+path to prevent cross-endpoint collisions.
	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}

	msg, err := s.sendMail(body)
	if err != nil {
		s.idem.unreserve(idemKey)
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeStructuredError(w, herr.status, herr.code, herr.message, herr.details)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	if idemKey != "" {
		s.idem.storeResponse(idemKey, bodyHash, http.StatusCreated, msg)
	}
	writeJSON(w, http.StatusCreated, msg)
}

func (s *Server) sendMail(body mailSendRequest) (mail.Message, error) {
	var errs []FieldError
	if body.To == "" {
		errs = append(errs, FieldError{Field: "to", Message: "required"})
	}
	if body.Subject == "" {
		errs = append(errs, FieldError{Field: "subject", Message: "required"})
	}
	if len(errs) > 0 {
		return mail.Message{}, httpError{
			status:  http.StatusBadRequest,
			code:    "invalid",
			message: "invalid mail request",
			details: errs,
		}
	}

	// Resolve recipient against configured agents.
	resolved, resolveErr := mail.ResolveRecipient(body.To, agentEntries(s.state.Config()))
	if resolveErr != nil {
		return mail.Message{}, httpError{status: http.StatusBadRequest, code: "invalid", message: resolveErr.Error()}
	}

	mp := s.findMailProvider(body.Rig)
	if mp == nil {
		return mail.Message{}, httpError{status: http.StatusBadRequest, code: "invalid", message: "no mail provider available"}
	}

	msg, err := mp.Send(body.From, resolved, body.Subject, body.Body)
	if err != nil {
		return mail.Message{}, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	msg.Rig = body.Rig
	s.recordMailEvent(events.MailSent, body.From, msg.ID, body.Rig, &msg)
	return msg, nil
}

func (s *Server) handleMailRead(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	resp, err := s.markMailRead(id, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMailMarkUnread(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	resp, err := s.markMailUnread(id, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMailArchive(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	resp, err := s.archiveMail(id, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMailReply(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	var body mailReplyRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	msg, err := s.replyMail(id, rig, body)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, msg)
}

func (s *Server) markMailRead(id, rig string) (map[string]string, error) {
	mp, resolvedRig, err := s.findMailProviderForMessage(id, rig)
	if err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	if mp == nil {
		return nil, httpError{status: http.StatusNotFound, code: "not_found", message: "message " + id + " not found"}
	}
	if err := mp.MarkRead(id); err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	s.recordMailEvent(events.MailMarkedRead, "api", id, resolvedRig, nil)
	return map[string]string{"status": "read"}, nil
}

func (s *Server) markMailUnread(id, rig string) (map[string]string, error) {
	mp, resolvedRig, err := s.findMailProviderForMessage(id, rig)
	if err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	if mp == nil {
		return nil, httpError{status: http.StatusNotFound, code: "not_found", message: "message " + id + " not found"}
	}
	if err := mp.MarkUnread(id); err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	s.recordMailEvent(events.MailMarkedUnread, "api", id, resolvedRig, nil)
	return map[string]string{"status": "unread"}, nil
}

func (s *Server) archiveMail(id, rig string) (map[string]string, error) {
	mp, resolvedRig, err := s.findMailProviderForMessage(id, rig)
	if err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	if mp == nil {
		return nil, httpError{status: http.StatusNotFound, code: "not_found", message: "message " + id + " not found"}
	}
	if err := mp.Archive(id); err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	s.recordMailEvent(events.MailArchived, "api", id, resolvedRig, nil)
	return map[string]string{"status": "archived"}, nil
}

func (s *Server) replyMail(id, rig string, body mailReplyRequest) (mail.Message, error) {
	mp, resolvedRig, mpErr := s.findMailProviderForMessage(id, rig)
	if mpErr != nil {
		return mail.Message{}, httpError{status: http.StatusInternalServerError, code: "internal", message: mpErr.Error()}
	}
	if mp == nil {
		return mail.Message{}, httpError{status: http.StatusNotFound, code: "not_found", message: "message " + id + " not found"}
	}

	msg, err := mp.Reply(id, body.From, body.Subject, body.Body)
	if err != nil {
		return mail.Message{}, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
	}
	msg.Rig = resolvedRig
	s.recordMailEvent(events.MailReplied, body.From, msg.ID, resolvedRig, &msg)
	return msg, nil
}

func (s *Server) handleMailDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	mp, resolvedRig, err := s.findMailProviderForMessage(id, rig)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if mp == nil {
		writeError(w, http.StatusNotFound, "not_found", "message "+id+" not found")
		return
	}
	if err := mp.Delete(id); err != nil {
		if errors.Is(err, mail.ErrNotFound) || errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "message "+id+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.recordMailEvent(events.MailDeleted, "api", id, resolvedRig, nil)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleMailThread(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("id")
	rig := r.URL.Query().Get("rig")
	msgs, err := s.listMailThread(threadID, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeListJSON(w, s.latestIndex(), msgs, len(msgs))
}

func (s *Server) handleMailCount(w http.ResponseWriter, r *http.Request) {
	agentName := r.URL.Query().Get("agent")
	rig := r.URL.Query().Get("rig")
	counts, err := s.mailCount(agentName, rig)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, counts)
}

func (s *Server) listMailThread(threadID, rig string) ([]mail.Message, error) {
	if rig != "" {
		mp := s.state.MailProvider(rig)
		if mp == nil {
			return nil, httpError{status: http.StatusNotFound, code: "not_found", message: "rig " + rig + " not found"}
		}
		msgs, err := mp.Thread(threadID)
		if err != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
		}
		if msgs == nil {
			msgs = []mail.Message{}
		}
		return tagRig(msgs, rig), nil
	}

	providers := s.state.MailProviders()
	var allMsgs []mail.Message
	for _, name := range sortedProviderNames(providers) {
		msgs, err := providers[name].Thread(threadID)
		if err != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "mail provider " + name + ": " + err.Error()}
		}
		allMsgs = append(allMsgs, tagRig(msgs, name)...)
	}
	if allMsgs == nil {
		allMsgs = []mail.Message{}
	}
	return allMsgs, nil
}

func (s *Server) mailCount(agentName, rig string) (map[string]int, error) {
	if rig != "" {
		mp := s.state.MailProvider(rig)
		if mp == nil {
			return map[string]int{"total": 0, "unread": 0}, nil
		}
		total, unread, err := mp.Count(agentName)
		if err != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: err.Error()}
		}
		return map[string]int{"total": total, "unread": unread}, nil
	}

	providers := s.state.MailProviders()
	var totalAll, unreadAll int
	for _, name := range sortedProviderNames(providers) {
		total, unread, err := providers[name].Count(agentName)
		if err != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "mail provider " + name + ": " + err.Error()}
		}
		totalAll += total
		unreadAll += unread
	}
	return map[string]int{"total": totalAll, "unread": unreadAll}, nil
}

// findMailProvider returns the mail provider for a rig, or the first available
// (deterministically by sorted rig name).
func (s *Server) findMailProvider(rig string) mail.Provider {
	if rig != "" {
		return s.state.MailProvider(rig)
	}
	providers := s.state.MailProviders()
	names := sortedProviderNames(providers)
	if len(names) == 0 {
		return nil
	}
	return providers[names[0]]
}

// findMailProviderForMessage locates the mail provider and rig that own `id`.
// When `rigHint` is non-empty, it checks that provider first for an O(1)
// lookup instead of scanning all providers. Falls back to brute-force
// search if the hint misses (message moved/deleted from that rig).
func (s *Server) findMailProviderForMessage(id, rigHint string) (mail.Provider, string, error) {
	if rigHint != "" {
		if mp := s.state.MailProvider(rigHint); mp != nil {
			if _, err := mp.Get(id); err == nil {
				return mp, rigHint, nil
			} else if !errors.Is(err, mail.ErrNotFound) && !errors.Is(err, beads.ErrNotFound) {
				return nil, "", err
			}
		}
		// Hint missed — fall through to full scan.
	}
	return s.findMailProviderByID(id)
}

// findMailProviderByID searches all mail providers for one that contains the given message ID.
// Returns the provider and rig that own the message, or nil/""
// with an error if a provider failed.
// Returns (nil, "", nil) only when all providers definitively return ErrNotFound.
func (s *Server) findMailProviderByID(id string) (mail.Provider, string, error) {
	providers := s.state.MailProviders()
	var firstErr error
	for _, name := range sortedProviderNames(providers) {
		mp := providers[name]
		if _, err := mp.Get(id); err == nil {
			return mp, name, nil
		} else if !errors.Is(err, mail.ErrNotFound) && !errors.Is(err, beads.ErrNotFound) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return nil, "", firstErr
}

// agentEntries converts city config agents to mail.AgentEntry for recipient resolution.
func agentEntries(cfg *config.City) []mail.AgentEntry {
	if cfg == nil {
		return nil
	}
	entries := make([]mail.AgentEntry, len(cfg.Agents))
	for i, a := range cfg.Agents {
		entries[i] = mail.AgentEntry{Dir: a.Dir, Name: a.Name}
	}
	return entries
}

// sortedProviderNames returns provider names in sorted order, deduplicating
// providers that share the same underlying instance (e.g. file provider mode).
func sortedProviderNames(providers map[string]mail.Provider) []string {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	seen := make(map[mail.Provider]bool, len(names))
	deduped := names[:0]
	for _, name := range names {
		p := providers[name]
		if seen[p] {
			continue
		}
		seen[p] = true
		deduped = append(deduped, name)
	}
	return deduped
}

// recordMailEvent emits a mail SSE event so WebSocket/SSE consumers receive
// real-time updates for API-initiated operations (not just CLI-initiated ones).
// Best-effort: silently skips if no event provider is configured.
func (s *Server) recordMailEvent(eventType, actor, subject, rig string, msg *mail.Message) {
	ep := s.state.EventProvider()
	if ep == nil {
		return
	}
	payload := map[string]any{"rig": rig}
	if msg != nil {
		payload["message"] = msg
	}
	b, _ := json.Marshal(payload)
	ep.Record(events.Event{
		Type:    eventType,
		Actor:   actor,
		Subject: subject,
		Payload: b,
	})
}

// tagRig stamps every message with the provider/rig name so API consumers
// can distinguish messages from different rigs in aggregated responses.
func tagRig(msgs []mail.Message, rig string) []mail.Message {
	for i := range msgs {
		msgs[i].Rig = rig
	}
	return msgs
}
