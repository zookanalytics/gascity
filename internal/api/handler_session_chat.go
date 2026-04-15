package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/sessionlog"
	"github.com/gastownhall/gascity/internal/shellquote"
	workdirutil "github.com/gastownhall/gascity/internal/workdir"
)

var errSessionTemplateNotFound = errors.New("session template not found")

type sessionCreateRequest struct {
	// Kind discriminates the session target: "agent" or "provider".
	Kind              string            `json:"kind,omitempty"`
	Name              string            `json:"name,omitempty"`
	Alias             string            `json:"alias,omitempty"`
	LegacySessionName *string           `json:"session_name,omitempty"`
	Message           string            `json:"message,omitempty"`
	Async             bool              `json:"async,omitempty"`
	Options           map[string]string `json:"options,omitempty"`
	// ProjectID is an opaque identifier for the MC project context.
	// Stored in bead metadata for session-to-project association.
	ProjectID string `json:"project_id,omitempty"`
	Title     string `json:"title,omitempty"`
}

type sessionMessageRequest struct {
	Message string `json:"message"`
}

type sessionSubmitRequest struct {
	Message string               `json:"message"`
	Intent  session.SubmitIntent `json:"intent,omitempty"`
}

type sessionPendingResponse struct {
	Supported bool                        `json:"supported"`
	Pending   *runtime.PendingInteraction `json:"pending,omitempty"`
}

type sessionRespondRequest struct {
	RequestID string            `json:"request_id,omitempty"`
	Action    string            `json:"action"`
	Text      string            `json:"text,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type sessionTranscriptResult struct {
	ID         string                     `json:"id"`
	Template   string                     `json:"template"`
	Format     string                     `json:"format"`
	Turns      []outputTurn               `json:"turns,omitempty"`
	Messages   []sessionRawMessage        `json:"messages,omitempty"`
	Pagination *sessionlog.PaginationInfo `json:"pagination,omitempty"`
}

type sessionTranscriptResponse = sessionTranscriptResult

type sessionRawTranscriptResponse = sessionTranscriptResult

type sessionTranscriptQuery struct {
	Tail   int
	Before string
	Raw    bool
}

type sessionRawMessage map[string]json.RawMessage

func normalizeSessionTranscriptFormat(format string) (string, error) {
	switch format {
	case "", "text":
		return "text", nil
	case "raw", "jsonl":
		return "raw", nil
	default:
		return "", httpError{status: http.StatusBadRequest, code: "invalid", message: "format must be one of: text, raw, jsonl"}
	}
}

func sliceTranscriptEntries(entries []*sessionlog.Entry, before string, tail int) ([]*sessionlog.Entry, *sessionlog.PaginationInfo) {
	totalCount := len(entries)
	working := entries
	if before != "" {
		for i, entry := range entries {
			if entry.UUID == before {
				working = entries[:i]
				break
			}
		}
	}

	sliced := working
	if tail > 0 {
		sliced = tailSlice(working, tail)
	}

	if before == "" && tail <= 0 {
		return sliced, nil
	}

	info := &sessionlog.PaginationInfo{
		HasOlderMessages:     len(sliced) < len(working),
		TotalMessageCount:    totalCount,
		ReturnedMessageCount: len(sliced),
	}
	if len(sliced) > 0 && len(sliced) < len(working) {
		info.TruncatedBeforeMessage = sliced[0].UUID
	}
	return sliced, info
}

func decodeSessionRawMessage(raw json.RawMessage) (sessionRawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var msg sessionRawMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "decoding raw session message: " + err.Error()}
	}
	return msg, nil
}

func (s *Server) sessionLogPaths() []string {
	if s.sessionLogSearchPaths != nil {
		return s.sessionLogSearchPaths
	}
	cfg := s.state.Config()
	if cfg == nil {
		return sessionlog.DefaultSearchPaths()
	}
	return sessionlog.MergeSearchPaths(cfg.Daemon.ObservePaths)
}

func sessionCreateHints(resolved *config.ResolvedProvider) runtime.Config {
	return runtime.Config{
		ReadyPromptPrefix:      resolved.ReadyPromptPrefix,
		ReadyDelayMs:           resolved.ReadyDelayMs,
		ProcessNames:           resolved.ProcessNames,
		EmitsPermissionWarning: resolved.EmitsPermissionWarning,
	}
}

func sessionResumeHints(resolved *config.ResolvedProvider, workDir string) runtime.Config {
	return runtime.Config{
		WorkDir:                workDir,
		ReadyPromptPrefix:      resolved.ReadyPromptPrefix,
		ReadyDelayMs:           resolved.ReadyDelayMs,
		ProcessNames:           resolved.ProcessNames,
		EmitsPermissionWarning: resolved.EmitsPermissionWarning,
		Env:                    resolved.Env,
	}
}

func (s *Server) resolveSessionTemplate(template string) (*config.ResolvedProvider, string, string, string, error) {
	cfg := s.state.Config()
	if cfg == nil {
		return nil, "", "", "", errors.New("no city config loaded")
	}
	agentCfg, ok := resolveSessionTemplateAgent(cfg, template)
	if !ok {
		return nil, "", "", "", errSessionTemplateNotFound
	}
	resolved, err := config.ResolveProvider(&agentCfg, &cfg.Workspace, cfg.Providers, exec.LookPath)
	if err != nil {
		return nil, "", "", "", err
	}
	workDir, err := workdirutil.ResolveWorkDirPathStrict(
		s.state.CityPath(),
		workdirutil.CityName(s.state.CityPath(), cfg),
		agentCfg.QualifiedName(),
		agentCfg,
		cfg.Rigs,
	)
	if err != nil {
		return nil, "", "", "", err
	}
	if workDir == "" {
		workDir = s.state.CityPath()
	}
	return resolved, workDir, agentCfg.Session, agentCfg.QualifiedName(), nil
}

func (s *Server) buildSessionResume(info session.Info) (string, runtime.Config) {
	cmd := session.BuildResumeCommand(info)

	buildResolved := func(resolved *config.ResolvedProvider, workDir string) (string, runtime.Config) {
		if resolved == nil {
			return cmd, runtime.Config{WorkDir: workDir}
		}
		resolvedInfo := info
		resolvedInfo.Command = resolved.CommandString()
		resolvedInfo.Provider = resolved.Name
		resolvedInfo.ResumeFlag = resolved.ResumeFlag
		resolvedInfo.ResumeStyle = resolved.ResumeStyle
		resolvedInfo.ResumeCommand = resolved.ResumeCommand
		return session.BuildResumeCommand(resolvedInfo), sessionResumeHints(resolved, workDir)
	}

	// Check persisted kind to avoid agent/provider name collisions.
	// If kind is "provider", skip the agent template lookup entirely.
	kind := s.sessionKind(info.ID)

	if kind != "provider" {
		resolved, workDir, _, _, err := s.resolveSessionTemplate(info.Template)
		if err == nil {
			if info.WorkDir != "" {
				workDir = info.WorkDir
			}
			return buildResolved(resolved, workDir)
		}
	}

	// Provider path (explicit kind=provider, or agent template not found).
	resolved, err := s.resolveBareProvider(info.Template)
	if err != nil {
		return cmd, runtime.Config{WorkDir: info.WorkDir}
	}
	workDir := info.WorkDir
	if workDir == "" {
		workDir = s.state.CityPath()
	}
	return buildResolved(resolved, workDir)
}

// sessionKind reads the persisted mc_session_kind from bead metadata.
func (s *Server) sessionKind(sessionID string) string {
	store := s.state.CityBeadStore()
	if store == nil {
		return ""
	}
	b, err := store.Get(sessionID)
	if err != nil {
		return ""
	}
	return b.Metadata["mc_session_kind"]
}

// resolveBareProvider resolves a provider by name without an agent template.
func (s *Server) resolveBareProvider(providerName string) (*config.ResolvedProvider, error) {
	cfg := s.state.Config()
	if cfg == nil {
		return nil, errors.New("no city config loaded")
	}
	return config.ResolveProvider(
		&config.Agent{Provider: providerName},
		&cfg.Workspace,
		cfg.Providers,
		exec.LookPath,
	)
}

func writeSessionManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, session.ErrInvalidSessionName):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, session.ErrSessionNameExists):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, session.ErrInvalidSessionAlias):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, session.ErrSessionAliasExists):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, session.ErrInteractionUnsupported):
		writeError(w, http.StatusNotImplemented, "unsupported", err.Error())
	case errors.Is(err, session.ErrPendingInteraction):
		writeError(w, http.StatusConflict, "pending_interaction", err.Error())
	case errors.Is(err, session.ErrNoPendingInteraction):
		writeError(w, http.StatusConflict, "no_pending", err.Error())
	case errors.Is(err, session.ErrInteractionMismatch):
		writeError(w, http.StatusConflict, "invalid_interaction", err.Error())
	case errors.Is(err, session.ErrSessionClosed), errors.Is(err, session.ErrResumeRequired):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, session.ErrNotSession):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	default:
		writeStoreError(w, err)
	}
}

// createProviderSession handles the "provider" kind session creation.
// Resolves a bare provider (not an agent template) and creates a session.
func (s *Server) createProviderSession(w http.ResponseWriter, r *http.Request, store beads.Store, body sessionCreateRequest, providerName, idemKey, bodyHash string) {
	cfg := s.state.Config()
	if cfg == nil {
		s.idem.unreserve(idemKey)
		writeError(w, http.StatusServiceUnavailable, "unavailable", "city config not loaded yet")
		return
	}
	resolved, err := config.ResolveProvider(
		&config.Agent{Provider: providerName},
		&cfg.Workspace,
		cfg.Providers,
		exec.LookPath,
	)
	if err != nil {
		s.idem.unreserve(idemKey)
		if errors.Is(err, config.ErrProviderNotInPATH) {
			writeError(w, http.StatusServiceUnavailable, "provider_unavailable", err.Error())
			return
		}
		if errors.Is(err, config.ErrProviderNotFound) {
			writeError(w, http.StatusNotFound, "provider_not_found", "provider '"+providerName+"' not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	// Resolve options against the provider's schema.
	var extraArgs []string
	var optMeta map[string]string
	if len(body.Options) > 0 && len(resolved.OptionsSchema) == 0 {
		s.idem.unreserve(idemKey)
		writeError(w, http.StatusBadRequest, "unknown_option", "provider '"+providerName+"' does not accept options")
		return
	}
	if len(resolved.OptionsSchema) > 0 {
		var optErr error
		extraArgs, optMeta, optErr = config.ResolveOptions(resolved.OptionsSchema, body.Options, resolved.EffectiveDefaults)
		if optErr != nil {
			s.idem.unreserve(idemKey)
			if errors.Is(optErr, config.ErrUnknownOption) {
				writeError(w, http.StatusBadRequest, "unknown_option", optErr.Error())
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_option_value", optErr.Error())
			return
		}
	}

	template := providerName
	title := body.Title
	if title == "" {
		title = resolved.Name
	}
	if body.Async && strings.TrimSpace(body.Message) != "" {
		s.idem.unreserve(idemKey)
		writeError(w, http.StatusBadRequest, "invalid", "message is not supported with async session creation; create the session, then POST /v0/session/{id}/messages")
		return
	}
	if body.Async {
		s.idem.unreserve(idemKey)
		writeError(w, http.StatusBadRequest, "invalid", "async session creation is only supported for configured agent templates")
		return
	}

	workDir := s.state.CityPath()

	resume := session.ProviderResume{
		ResumeFlag:    resolved.ResumeFlag,
		ResumeStyle:   resolved.ResumeStyle,
		ResumeCommand: resolved.ResumeCommand,
		SessionIDFlag: resolved.SessionIDFlag,
	}
	alias, err := session.ValidateAlias(body.Alias)
	if err != nil {
		s.idem.unreserve(idemKey)
		writeSessionManagerError(w, err)
		return
	}

	command := resolved.CommandString()
	if len(extraArgs) > 0 {
		command = command + " " + shellquote.Join(extraArgs)
	}

	mgr := s.sessionManager(store)
	hints := sessionCreateHints(resolved)
	var info session.Info
	err = session.WithCitySessionAliasLock(s.state.CityPath(), alias, func() error {
		if err := session.EnsureAliasAvailableWithConfig(store, s.state.Config(), alias, ""); err != nil {
			return err
		}
		var createErr error
		info, createErr = mgr.CreateAliasedNamedWithTransport(
			r.Context(),
			alias,
			"",
			template,
			title,
			command,
			workDir,
			resolved.Name,
			"",
			resolved.Env,
			resume,
			hints,
		)
		return createErr
	})
	if err != nil {
		s.idem.unreserve(idemKey)
		writeSessionManagerError(w, err)
		return
	}

	// Persist kind, option metadata, and project_id on the bead.
	s.persistSessionMeta(store, info.ID, "provider", body.ProjectID, optMeta)
	if body.Async {
		s.state.Poke()
	}

	// Auto-generate a title from the user's message if no explicit title was provided.
	titleProvider := s.resolveTitleProvider()
	MaybeGenerateTitleAsync(store, info.ID, body.Title, body.Message, titleProvider, info.WorkDir, func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "session %s: "+format+"\n", append([]any{info.ID}, args...)...)
	})

	// Deliver initial message if provided.
	if msg := strings.TrimSpace(body.Message); msg != "" {
		if _, sendErr := s.submitMessageToSession(r.Context(), store, info.ID, msg, session.SubmitIntentDefault); sendErr != nil {
			log.Printf("session %s: initial message delivery failed: %v", info.ID, sendErr)
			s.idem.unreserve(idemKey)
			writeError(w, http.StatusInternalServerError, "message_delivery_failed",
				fmt.Sprintf("session created but initial message failed: %v", sendErr))
			return
		}
	}

	resp := sessionToResponse(info, s.state.Config())
	resp.Kind = "provider"
	if caps, capErr := s.sessionManager(store).SubmissionCapabilities(info.ID); capErr == nil {
		resp.SubmissionCapabilities = caps
	}
	s.enrichSessionResponse(&resp, info, s.state.Config(), s.state.SessionProvider(), false)
	statusCode := http.StatusCreated
	s.idem.storeResponse(idemKey, bodyHash, statusCode, resp)
	writeJSON(w, statusCode, resp)
}

// persistSessionMeta writes kind, option metadata, and project_id to the session bead.
func (s *Server) persistSessionMeta(store beads.Store, sessionID, kind, projectID string, optMeta map[string]string) {
	batch := make(map[string]string)
	for k, v := range optMeta {
		batch[k] = v
	}
	if kind != "" {
		batch["mc_session_kind"] = kind
	}
	if projectID != "" {
		batch["mc_project_id"] = projectID
	}
	if len(batch) > 0 {
		if err := store.SetMetadataBatch(sessionID, batch); err != nil {
			log.Printf("persistSessionMeta: session %s: %v", sessionID, err)
		}
	}
}

func (s *Server) submitSessionTarget(ctx context.Context, target, message string, intent session.SubmitIntent) (SessionSubmitResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return SessionSubmitResponse{}, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDMaterializingNamedWithContext(ctx, store, target)
	if err != nil {
		return SessionSubmitResponse{}, err
	}
	outcome, err := s.submitMessageToSession(ctx, store, id, message, intent)
	if err != nil {
		return SessionSubmitResponse{}, err
	}
	return SessionSubmitResponse{
		Status: "accepted",
		ID:     id,
		Queued: outcome.Queued,
		Intent: intent,
	}, nil
}

func (s *Server) killSessionTarget(target string) (mutationStatusIDResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return mutationStatusIDResponse{}, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDWithConfig(store, target)
	if err != nil {
		return mutationStatusIDResponse{}, err
	}
	mgr := s.sessionManager(store)
	if err := mgr.Kill(id); err != nil {
		return mutationStatusIDResponse{}, err
	}
	return mutationStatusIDResponse{Status: "ok", ID: id}, nil
}

func writeSocketCompatibleSessionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, session.ErrAmbiguous), errors.Is(err, errConfiguredNamedSessionConflict):
		writeError(w, http.StatusConflict, "ambiguous", err.Error())
	case errors.Is(err, session.ErrSessionNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case herrStatus(err) != 0:
		herr := asHTTPError(err)
		writeError(w, herr.status, herr.code, herr.message)
	default:
		writeSessionManagerError(w, err)
	}
}

func asHTTPError(err error) httpError {
	var herr httpError
	if errors.As(err, &herr) {
		return herr
	}
	return httpError{}
}

func herrStatus(err error) int {
	var herr httpError
	if errors.As(err, &herr) {
		return herr.status
	}
	return 0
}

func (s *Server) getSessionPending(target string) (sessionPendingResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return sessionPendingResponse{}, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}

	id, err := s.resolveSessionIDWithConfig(store, target)
	if err != nil {
		return sessionPendingResponse{}, err
	}

	mgr := s.sessionManager(store)
	pending, supported, err := mgr.Pending(id)
	if err != nil {
		return sessionPendingResponse{}, err
	}
	return sessionPendingResponse{Supported: supported, Pending: pending}, nil
}

func (s *Server) getSessionTranscript(target string, query sessionTranscriptQuery) (sessionTranscriptResult, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return sessionTranscriptResult{}, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}

	id, err := s.resolveSessionIDAllowClosedWithConfig(store, target)
	if err != nil {
		return sessionTranscriptResult{}, err
	}

	mgr := s.sessionManager(store)
	info, err := mgr.Get(id)
	if err != nil {
		return sessionTranscriptResult{}, err
	}

	path, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		return sessionTranscriptResult{}, err
	}

	if path != "" {
		if query.Raw {
			rawSess, err := sessionlog.ReadProviderFileRaw(info.Provider, path, 0)
			if err != nil {
				return sessionTranscriptResult{}, httpError{status: http.StatusInternalServerError, code: "internal", message: "reading session log: " + err.Error()}
			}
			entries, pagination := sliceTranscriptEntries(rawSess.Messages, query.Before, query.Tail)
			msgs := make([]sessionRawMessage, 0, len(entries))
			for _, entry := range entries {
				if len(entry.Raw) > 0 {
					msg, err := decodeSessionRawMessage(entry.Raw)
					if err != nil {
						return sessionTranscriptResult{}, err
					}
					msgs = append(msgs, msg)
				}
			}
			return sessionRawTranscriptResponse{
				ID:         info.ID,
				Template:   info.Template,
				Format:     "raw",
				Messages:   msgs,
				Pagination: pagination,
			}, nil
		}

		sess, err := sessionlog.ReadProviderFile(info.Provider, path, 0)
		if err != nil {
			return sessionTranscriptResult{}, httpError{status: http.StatusInternalServerError, code: "internal", message: "reading session log: " + err.Error()}
		}

		entries, pagination := sliceTranscriptEntries(sess.Messages, query.Before, query.Tail)
		turns := make([]outputTurn, 0, len(entries))
		for _, entry := range entries {
			turn := entryToTurn(entry)
			if turn.Text == "" {
				continue
			}
			turns = append(turns, turn)
		}
		return sessionTranscriptResponse{
			ID:         info.ID,
			Template:   info.Template,
			Format:     "conversation",
			Turns:      turns,
			Pagination: pagination,
		}, nil
	}

	if query.Raw {
		return sessionRawTranscriptResponse{
			ID:       info.ID,
			Template: info.Template,
			Format:   "raw",
			Messages: []sessionRawMessage{},
		}, nil
	}

	if info.State == session.StateActive && s.state.SessionProvider().IsRunning(info.SessionName) {
		output, peekErr := s.state.SessionProvider().Peek(info.SessionName, 100)
		if peekErr != nil {
			return sessionTranscriptResult{}, httpError{status: http.StatusInternalServerError, code: "internal", message: peekErr.Error()}
		}
		turns := []outputTurn{}
		if output != "" {
			turns = append(turns, outputTurn{Role: "output", Text: output})
		}
		return sessionTranscriptResponse{
			ID:       info.ID,
			Template: info.Template,
			Format:   "text",
			Turns:    turns,
		}, nil
	}

	return sessionTranscriptResponse{
		ID:       info.ID,
		Template: info.Template,
		Format:   "conversation",
		Turns:    []outputTurn{},
	}, nil
}

func (s *Server) respondSessionTarget(identifier string, body sessionRespondRequest) (mutationStatusIDResponse, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return mutationStatusIDResponse{}, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	if body.Action == "" {
		return mutationStatusIDResponse{}, httpError{status: http.StatusBadRequest, code: "invalid", message: "action is required"}
	}
	id, err := s.resolveSessionIDWithConfig(store, identifier)
	if err != nil {
		return mutationStatusIDResponse{}, err
	}
	mgr := s.sessionManager(store)
	if err := mgr.Respond(id, runtime.InteractionResponse{
		RequestID: body.RequestID,
		Action:    body.Action,
		Text:      body.Text,
		Metadata:  body.Metadata,
	}); err != nil {
		return mutationStatusIDResponse{}, err
	}
	return mutationStatusIDResponse{Status: "accepted", ID: id}, nil
}

// createSessionInternal implements session creation for the WebSocket transport.
// Returns the session response or an error. Handles agent and provider kinds.
func (s *Server) createSessionInternal(ctx context.Context, body sessionCreateRequest, idemKey string) (sessionResponse, int, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return sessionResponse{}, 0, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
	}
	if body.LegacySessionName != nil {
		return sessionResponse{}, 0, httpError{status: 400, code: "invalid", message: "session_name is no longer accepted; use alias"}
	}

	// WS idempotency is handled at the dispatch layer; we use an empty
	// idemKey here so the internal helpers don't touch the HTTP cache.
	_ = idemKey

	alias, err := session.ValidateAlias(body.Alias)
	if err != nil {
		return sessionResponse{}, 0, err
	}

	switch body.Kind {
	case "agent":
		return s.createAgentSessionInternal(ctx, store, body, alias)
	case "provider":
		return s.createProviderSessionInternal(ctx, store, body, alias)
	default:
		return sessionResponse{}, 0, httpError{status: 400, code: "invalid_kind", message: "kind must be 'agent' or 'provider'"}
	}
}

func (s *Server) createAgentSessionInternal(ctx context.Context, store beads.Store, body sessionCreateRequest, alias string) (sessionResponse, int, error) {
	resolved, workDir, transport, template, err := s.resolveSessionTemplate(body.Name)
	if err != nil {
		if errors.Is(err, errSessionTemplateNotFound) {
			return sessionResponse{}, 0, httpError{status: 404, code: "agent_not_found", message: "agent '" + body.Name + "' not found"}
		}
		return sessionResponse{}, 0, err
	}

	if len(body.Options) > 0 {
		if len(resolved.OptionsSchema) == 0 {
			return sessionResponse{}, 0, httpError{status: 400, code: "unknown_option", message: "agent '" + body.Name + "' does not accept options"}
		}
		if _, err := config.ResolveExplicitOptions(resolved.OptionsSchema, body.Options); err != nil {
			if errors.Is(err, config.ErrUnknownOption) {
				return sessionResponse{}, 0, httpError{status: 400, code: "unknown_option", message: err.Error()}
			}
			return sessionResponse{}, 0, httpError{status: 400, code: "invalid_option_value", message: err.Error()}
		}
	}

	title := body.Title
	if title == "" {
		title = template
	}

	command := resolved.CommandString()
	if len(resolved.OptionsSchema) > 0 {
		mergedOptions := make(map[string]string)
		for k, v := range resolved.EffectiveDefaults {
			mergedOptions[k] = v
		}
		for k, v := range body.Options {
			mergedOptions[k] = v
		}
		if mergedArgs, mergeErr := config.ResolveExplicitOptions(resolved.OptionsSchema, mergedOptions); mergeErr == nil && len(mergedArgs) > 0 {
			command = config.ReplaceSchemaFlags(command, resolved.OptionsSchema, mergedArgs)
		}
	}

	allOverrides := make(map[string]string)
	for k, v := range body.Options {
		allOverrides[k] = v
	}
	if msg := strings.TrimSpace(body.Message); msg != "" {
		allOverrides["initial_message"] = msg
	}
	var extraMeta map[string]string
	if len(allOverrides) > 0 {
		if overridesJSON, jsonErr := json.Marshal(allOverrides); jsonErr == nil {
			extraMeta = map[string]string{"template_overrides": string(overridesJSON)}
		}
	}

	resume := session.ProviderResume{
		ResumeFlag:    resolved.ResumeFlag,
		ResumeStyle:   resolved.ResumeStyle,
		ResumeCommand: resolved.ResumeCommand,
		SessionIDFlag: resolved.SessionIDFlag,
	}

	mgr := s.sessionManager(store)
	var info session.Info
	err = session.WithCitySessionAliasLock(s.state.CityPath(), alias, func() error {
		if err := session.EnsureAliasAvailableWithConfig(store, s.state.Config(), alias, ""); err != nil {
			return err
		}
		var createErr error
		info, createErr = mgr.CreateAliasedBeadOnlyNamedWithMetadata(
			alias, "", template, title, command, workDir,
			resolved.Name, transport, resume, extraMeta,
		)
		return createErr
	})
	if err != nil {
		return sessionResponse{}, 0, err
	}

	s.persistSessionMeta(store, info.ID, "agent", body.ProjectID, nil)
	s.state.Poke()

	titleProvider := s.resolveTitleProvider()
	MaybeGenerateTitleAsync(store, info.ID, body.Title, body.Message, titleProvider, info.WorkDir, func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "session %s: "+format+"\n", append([]any{info.ID}, args...)...)
	})

	resp := sessionToResponse(info, s.state.Config())
	resp.Kind = "agent"
	if caps, capErr := mgr.SubmissionCapabilities(info.ID); capErr == nil {
		resp.SubmissionCapabilities = caps
	}
	s.enrichSessionResponse(&resp, info, s.state.Config(), s.state.SessionProvider(), false)
	return resp, 202, nil
}

func (s *Server) createProviderSessionInternal(ctx context.Context, store beads.Store, body sessionCreateRequest, alias string) (sessionResponse, int, error) {
	cfg := s.state.Config()
	if cfg == nil {
		return sessionResponse{}, 0, httpError{status: 503, code: "unavailable", message: "city config not loaded yet"}
	}
	resolved, err := config.ResolveProvider(
		&config.Agent{Provider: body.Name},
		&cfg.Workspace,
		cfg.Providers,
		exec.LookPath,
	)
	if err != nil {
		if errors.Is(err, config.ErrProviderNotInPATH) {
			return sessionResponse{}, 0, httpError{status: 503, code: "provider_unavailable", message: err.Error()}
		}
		if errors.Is(err, config.ErrProviderNotFound) {
			return sessionResponse{}, 0, httpError{status: 404, code: "provider_not_found", message: "provider '" + body.Name + "' not found"}
		}
		return sessionResponse{}, 0, err
	}

	var extraArgs []string
	var optMeta map[string]string
	if len(body.Options) > 0 && len(resolved.OptionsSchema) == 0 {
		return sessionResponse{}, 0, httpError{status: 400, code: "unknown_option", message: "provider '" + body.Name + "' does not accept options"}
	}
	if len(resolved.OptionsSchema) > 0 {
		var optErr error
		extraArgs, optMeta, optErr = config.ResolveOptions(resolved.OptionsSchema, body.Options, resolved.EffectiveDefaults)
		if optErr != nil {
			if errors.Is(optErr, config.ErrUnknownOption) {
				return sessionResponse{}, 0, httpError{status: 400, code: "unknown_option", message: optErr.Error()}
			}
			return sessionResponse{}, 0, httpError{status: 400, code: "invalid_option_value", message: optErr.Error()}
		}
	}

	template := body.Name
	title := body.Title
	if title == "" {
		title = resolved.Name
	}
	if body.Async && strings.TrimSpace(body.Message) != "" {
		return sessionResponse{}, 0, httpError{status: 400, code: "invalid", message: "message is not supported with async session creation"}
	}
	if body.Async {
		return sessionResponse{}, 0, httpError{status: 400, code: "invalid", message: "async session creation is only supported for configured agent templates"}
	}

	workDir := s.state.CityPath()
	command := resolved.CommandString()
	if len(extraArgs) > 0 {
		command = command + " " + shellquote.Join(extraArgs)
	}

	resume := session.ProviderResume{
		ResumeFlag:    resolved.ResumeFlag,
		ResumeStyle:   resolved.ResumeStyle,
		ResumeCommand: resolved.ResumeCommand,
		SessionIDFlag: resolved.SessionIDFlag,
	}

	hints := sessionCreateHints(resolved)
	mgr := s.sessionManager(store)
	var info session.Info
	err = session.WithCitySessionAliasLock(s.state.CityPath(), alias, func() error {
		if err := session.EnsureAliasAvailableWithConfig(store, s.state.Config(), alias, ""); err != nil {
			return err
		}
		var createErr error
		info, createErr = mgr.CreateAliasedNamedWithTransport(
			ctx, alias, "", template, title, command, workDir,
			resolved.Name, "", resolved.Env, resume, hints,
		)
		return createErr
	})
	if err != nil {
		return sessionResponse{}, 0, err
	}

	s.persistSessionMeta(store, info.ID, "provider", body.ProjectID, optMeta)

	titleProvider := s.resolveTitleProvider()
	MaybeGenerateTitleAsync(store, info.ID, body.Title, body.Message, titleProvider, info.WorkDir, func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "session %s: "+format+"\n", append([]any{info.ID}, args...)...)
	})

	if msg := strings.TrimSpace(body.Message); msg != "" {
		if _, sendErr := s.submitMessageToSession(ctx, store, info.ID, msg, session.SubmitIntentDefault); sendErr != nil {
			return sessionResponse{}, 0, httpError{status: 500, code: "message_delivery_failed", message: fmt.Sprintf("session created but initial message failed: %v", sendErr)}
		}
	}

	resp := sessionToResponse(info, s.state.Config())
	resp.Kind = "provider"
	if caps, capErr := mgr.SubmissionCapabilities(info.ID); capErr == nil {
		resp.SubmissionCapabilities = caps
	}
	s.enrichSessionResponse(&resp, info, s.state.Config(), s.state.SessionProvider(), false)
	return resp, 201, nil
}
