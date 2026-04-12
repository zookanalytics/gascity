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
	"strconv"
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

type sessionTranscriptResponse struct {
	ID         string                     `json:"id"`
	Template   string                     `json:"template"`
	Format     string                     `json:"format"`
	Turns      []outputTurn               `json:"turns"`
	Pagination *sessionlog.PaginationInfo `json:"pagination,omitempty"`
}

type sessionRawTranscriptResponse struct {
	ID         string                     `json:"id"`
	Template   string                     `json:"template"`
	Format     string                     `json:"format"`
	Messages   []json.RawMessage          `json:"messages"`
	Pagination *sessionlog.PaginationInfo `json:"pagination,omitempty"`
}

type sessionTranscriptQuery struct {
	Tail   int
	Before string
	Raw    bool
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

func (s *Server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}

	var body sessionCreateRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if body.LegacySessionName != nil {
		writeError(w, http.StatusBadRequest, "invalid", "session_name is no longer accepted; use alias")
		return
	}

	kind := body.Kind
	name := body.Name
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid", "name is required")
		return
	}
	if kind != "agent" && kind != "provider" {
		writeError(w, http.StatusBadRequest, "invalid_kind", "kind must be 'agent' or 'provider'")
		return
	}

	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}

	var resolved *config.ResolvedProvider
	var workDir, transport, template string
	var optMeta map[string]string

	switch kind {
	case "agent":
		var err error
		resolved, workDir, transport, template, err = s.resolveSessionTemplate(name)
		if err != nil {
			if errors.Is(err, errSessionTemplateNotFound) {
				s.idem.unreserve(idemKey)
				writeError(w, http.StatusNotFound, "agent_not_found", "agent '"+name+"' not found")
				return
			}
			s.idem.unreserve(idemKey)
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		// Agent track: command comes from the agent config as-is.
		// Do NOT inject OptionsSchema defaults — agents encode their own CLI flags.
		// Options are stored as template_overrides and applied at start time
		// by the session lifecycle via ResolveExplicitOptions.
		if len(body.Options) > 0 {
			if len(resolved.OptionsSchema) == 0 {
				s.idem.unreserve(idemKey)
				writeError(w, http.StatusBadRequest, "unknown_option", "agent '"+name+"' does not accept options")
				return
			}
			// Validate options against the schema without applying defaults.
			if _, err := config.ResolveExplicitOptions(resolved.OptionsSchema, body.Options); err != nil {
				s.idem.unreserve(idemKey)
				if errors.Is(err, config.ErrUnknownOption) {
					writeError(w, http.StatusBadRequest, "unknown_option", err.Error())
					return
				}
				writeError(w, http.StatusBadRequest, "invalid_option_value", err.Error())
				return
			}
		}

	case "provider":
		s.createProviderSession(w, r, store, body, name, idemKey, bodyHash)
		return
	}

	title := body.Title
	if title == "" {
		title = template
	}

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

	// Merge explicit options with provider effective defaults.
	// User-specified options override defaults; unspecified options get the
	// provider's EffectiveDefaults (e.g., permission_mode=unrestricted for claude).
	command := resolved.CommandString()
	if len(resolved.OptionsSchema) > 0 {
		mergedOptions := make(map[string]string)
		for k, v := range resolved.EffectiveDefaults {
			mergedOptions[k] = v
		}
		for k, v := range body.Options {
			mergedOptions[k] = v
		}
		if mergedArgs, err := config.ResolveExplicitOptions(resolved.OptionsSchema, mergedOptions); err == nil && len(mergedArgs) > 0 {
			command = config.ReplaceSchemaFlags(command, resolved.OptionsSchema, mergedArgs)
		}
	}

	// Build template_overrides metadata. Includes schema overrides AND
	// the initial message (as "initial_message" key). The reconciler
	// handles both: schema overrides map to CLI flags, initial_message
	// is appended to the prompt on first start only.
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

	// Agent sessions always use async (bead-only) creation. The reconciler
	// starts the agent process on the next tick. This avoids blocking the
	// HTTP response for 10-30s while the agent boots in tmux, and lets MC
	// show the session in the sidebar immediately via optimistic UI.
	mgr := s.sessionManager(store)
	var info session.Info
	err = session.WithCitySessionAliasLock(s.state.CityPath(), alias, func() error {
		if err := session.EnsureAliasAvailableWithConfig(store, s.state.Config(), alias, ""); err != nil {
			return err
		}
		var createErr error
		info, createErr = mgr.CreateAliasedBeadOnlyNamedWithMetadata(
			alias,
			"",
			template,
			title,
			command,
			workDir,
			resolved.Name,
			transport,
			resume,
			extraMeta,
		)
		return createErr
	})
	if err != nil {
		s.idem.unreserve(idemKey)
		writeSessionManagerError(w, err)
		return
	}

	// Persist kind, option metadata, and project_id on the bead.
	// NOTE: template_overrides (options + initial_message) is already set via
	// extraMeta in CreateAliasedBeadOnlyNamedWithMetadata above. Do NOT
	// overwrite it here — the old code clobbered initial_message by writing
	// only the options portion.
	s.persistSessionMeta(store, info.ID, "agent", body.ProjectID, optMeta)
	s.state.Poke() // wake reconciler to start the agent

	// Auto-generate a title from the user's message if no explicit title was provided.
	titleProvider := s.resolveTitleProvider()
	MaybeGenerateTitleAsync(store, info.ID, body.Title, body.Message, titleProvider, info.WorkDir, func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "session %s: "+format+"\n", append([]any{info.ID}, args...)...)
	})

	resp := sessionToResponse(info, s.state.Config())
	resp.Kind = "agent"
	if caps, capErr := s.sessionManager(store).SubmissionCapabilities(info.ID); capErr == nil {
		resp.SubmissionCapabilities = caps
	}
	s.enrichSessionResponse(&resp, info, s.state.Config(), s.state.SessionProvider(), false)
	statusCode := http.StatusAccepted // always async for agent sessions
	s.idem.storeResponse(idemKey, bodyHash, statusCode, resp)
	writeJSON(w, statusCode, resp)
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

func (s *Server) handleSessionTranscript(w http.ResponseWriter, r *http.Request) {
	tail := 0
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n >= 0 {
			tail = n
		}
	}
	resp, err := s.getSessionTranscript(r.PathValue("id"), sessionTranscriptQuery{
		Tail:   tail,
		Before: r.URL.Query().Get("before"),
		Raw:    r.URL.Query().Get("format") == "raw",
	})
	if err != nil {
		writeSocketCompatibleSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSessionSubmit(w http.ResponseWriter, r *http.Request) {
	var body sessionSubmitRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if strings.TrimSpace(body.Message) == "" {
		writeError(w, http.StatusBadRequest, "invalid", "message is required")
		return
	}
	if body.Intent == "" {
		body.Intent = session.SubmitIntentDefault
	}
	switch body.Intent {
	case session.SubmitIntentDefault, session.SubmitIntentFollowUp, session.SubmitIntentInterruptNow:
	default:
		writeError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("intent must be one of %q, %q, or %q", session.SubmitIntentDefault, session.SubmitIntentFollowUp, session.SubmitIntentInterruptNow))
		return
	}

	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}
	resp, err := s.submitSessionTarget(r.Context(), r.PathValue("id"), body.Message, body.Intent)
	if err != nil {
		s.idem.unreserve(idemKey)
		writeSocketCompatibleSessionError(w, err)
		return
	}
	s.idem.storeResponse(idemKey, bodyHash, http.StatusAccepted, resp)
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleSessionMessage(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}

	var body sessionMessageRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if strings.TrimSpace(body.Message) == "" {
		writeError(w, http.StatusBadRequest, "invalid", "message is required")
		return
	}

	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}

	id, err := s.resolveSessionIDMaterializingNamedWithContext(r.Context(), store, r.PathValue("id"))
	if err != nil {
		s.idem.unreserve(idemKey)
		writeResolveError(w, err)
		return
	}

	if err := s.sendUserMessageToSession(r.Context(), store, id, body.Message); err != nil {
		s.idem.unreserve(idemKey)
		writeSessionManagerError(w, err)
		return
	}

	resp := map[string]string{"status": "accepted", "id": id}
	s.idem.storeResponse(idemKey, bodyHash, http.StatusAccepted, resp)
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleSessionKill(w http.ResponseWriter, r *http.Request) {
	resp, err := s.killSessionTarget(r.PathValue("id"))
	if err != nil {
		writeSocketCompatibleSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) submitSessionTarget(ctx context.Context, target, message string, intent session.SubmitIntent) (map[string]any, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return nil, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDMaterializingNamedWithContext(ctx, store, target)
	if err != nil {
		return nil, err
	}
	outcome, err := s.submitMessageToSession(ctx, store, id, message, intent)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status": "accepted",
		"id":     id,
		"queued": outcome.Queued,
		"intent": string(intent),
	}, nil
}

func (s *Server) killSessionTarget(target string) (map[string]string, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return nil, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	id, err := s.resolveSessionIDWithConfig(store, target)
	if err != nil {
		return nil, err
	}
	mgr := s.sessionManager(store)
	if err := mgr.Kill(id); err != nil {
		return nil, err
	}
	return map[string]string{"status": "ok", "id": id}, nil
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

func (s *Server) handleSessionStop(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}

	id, err := s.resolveSessionIDWithConfig(store, r.PathValue("id"))
	if err != nil {
		writeResolveError(w, err)
		return
	}

	mgr := s.sessionManager(store)
	if err := mgr.StopTurn(id); err != nil {
		writeSessionManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": id})
}

func (s *Server) handleSessionPending(w http.ResponseWriter, r *http.Request) {
	resp, err := s.getSessionPending(r.PathValue("id"))
	if err != nil {
		writeSocketCompatibleSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
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

func (s *Server) getSessionTranscript(target string, query sessionTranscriptQuery) (any, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return nil, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}

	id, err := s.resolveSessionIDAllowClosedWithConfig(store, target)
	if err != nil {
		return nil, err
	}

	mgr := s.sessionManager(store)
	info, err := mgr.Get(id)
	if err != nil {
		return nil, err
	}

	path, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		return nil, err
	}

	if path != "" {
		if query.Raw {
			var rawSess *sessionlog.Session
			if query.Before != "" {
				rawSess, err = sessionlog.ReadProviderFileRawOlder(info.Provider, path, query.Tail, query.Before)
			} else {
				rawSess, err = sessionlog.ReadProviderFileRaw(info.Provider, path, query.Tail)
			}
			if err != nil {
				return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "reading session log: " + err.Error()}
			}
			msgs := make([]json.RawMessage, 0, len(rawSess.Messages))
			for _, entry := range rawSess.Messages {
				if len(entry.Raw) > 0 {
					msgs = append(msgs, entry.Raw)
				}
			}
			return sessionRawTranscriptResponse{
				ID:         info.ID,
				Template:   info.Template,
				Format:     "raw",
				Messages:   msgs,
				Pagination: rawSess.Pagination,
			}, nil
		}

		var sess *sessionlog.Session
		if query.Before != "" {
			sess, err = sessionlog.ReadProviderFileOlder(info.Provider, path, query.Tail, query.Before)
		} else {
			sess, err = sessionlog.ReadProviderFile(info.Provider, path, query.Tail)
		}
		if err != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: "reading session log: " + err.Error()}
		}

		turns := make([]outputTurn, 0, len(sess.Messages))
		for _, entry := range sess.Messages {
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
			Pagination: sess.Pagination,
		}, nil
	}

	if query.Raw {
		return sessionRawTranscriptResponse{
			ID:       info.ID,
			Template: info.Template,
			Format:   "raw",
			Messages: []json.RawMessage{},
		}, nil
	}

	if info.State == session.StateActive && s.state.SessionProvider().IsRunning(info.SessionName) {
		output, peekErr := s.state.SessionProvider().Peek(info.SessionName, 100)
		if peekErr != nil {
			return nil, httpError{status: http.StatusInternalServerError, code: "internal", message: peekErr.Error()}
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

func (s *Server) handleSessionRespond(w http.ResponseWriter, r *http.Request) {
	var body sessionRespondRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}
	resp, err := s.respondSessionTarget(r.PathValue("id"), body)
	if err != nil {
		s.idem.unreserve(idemKey)
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
			return
		}
		if errors.Is(err, session.ErrAmbiguous) || errors.Is(err, errConfiguredNamedSessionConflict) || errors.Is(err, session.ErrSessionNotFound) {
			writeResolveError(w, err)
			return
		}
		writeSessionManagerError(w, err)
		return
	}
	s.idem.storeResponse(idemKey, bodyHash, http.StatusAccepted, resp)
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) respondSessionTarget(identifier string, body sessionRespondRequest) (map[string]string, error) {
	store := s.state.CityBeadStore()
	if store == nil {
		return nil, httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no bead store configured"}
	}
	if body.Action == "" {
		return nil, httpError{status: http.StatusBadRequest, code: "invalid", message: "action is required"}
	}
	id, err := s.resolveSessionIDWithConfig(store, identifier)
	if err != nil {
		return nil, err
	}
	mgr := s.sessionManager(store)
	if err := mgr.Respond(id, runtime.InteractionResponse{
		RequestID: body.RequestID,
		Action:    body.Action,
		Text:      body.Text,
		Metadata:  body.Metadata,
	}); err != nil {
		return nil, err
	}
	return map[string]string{"status": "accepted", "id": id}, nil
}

func (s *Server) handleSessionStream(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}

	id, err := s.resolveSessionIDAllowClosedWithConfig(store, r.PathValue("id"))
	if err != nil {
		writeResolveError(w, err)
		return
	}

	mgr := s.sessionManager(store)
	info, err := mgr.Get(id)
	if err != nil {
		writeSessionManagerError(w, err)
		return
	}
	path, err := mgr.TranscriptPath(id, s.sessionLogPaths())
	if err != nil {
		writeSessionManagerError(w, err)
		return
	}

	sp := s.state.SessionProvider()
	running := info.State == session.StateActive && sp.IsRunning(info.SessionName)
	if path == "" && !running {
		writeError(w, http.StatusNotFound, "not_found", "session "+id+" has no live output")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if info.State != "" {
		w.Header().Set("GC-Session-State", string(info.State))
	}
	if !running {
		w.Header().Set("GC-Session-Status", "stopped")
	}
	w.WriteHeader(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil {
		_ = err
	}

	ctx := r.Context()
	format := r.URL.Query().Get("format")
	if info.Closed {
		if format == "raw" {
			s.emitClosedSessionSnapshotRaw(w, info, path)
		} else {
			s.emitClosedSessionSnapshot(w, info, path)
		}
		return
	}
	switch {
	case path != "":
		if format == "raw" {
			s.streamSessionTranscriptLogRaw(ctx, w, info, path)
		} else {
			s.streamSessionTranscriptLog(ctx, w, info, path)
		}
	case format == "raw":
		// No log file yet. If the session is running, poll tmux pane content
		// and wrap it as a fake raw JSONL assistant message so MC's existing
		// rendering pipeline shows terminal output (e.g. OAuth prompts).
		if running {
			s.streamSessionPeekRaw(ctx, w, info)
		} else {
			data, _ := json.Marshal(sessionRawTranscriptResponse{
				ID:       info.ID,
				Template: info.Template,
				Format:   "raw",
				Messages: []json.RawMessage{},
			})
			writeSSE(w, "message", 1, data)
		}
		return
	default:
		s.streamSessionPeek(ctx, w, info)
	}
}

func (s *Server) emitClosedSessionSnapshot(w http.ResponseWriter, info session.Info, logPath string) {
	s.emitClosedSessionSnapshotWithEmitter(newSSESessionStreamEmitter(w), info, logPath)
}

func (s *Server) emitClosedSessionSnapshotRaw(w http.ResponseWriter, info session.Info, logPath string) {
	s.emitClosedSessionSnapshotRawWithEmitter(newSSESessionStreamEmitter(w), info, logPath)
}

func (s *Server) streamSessionTranscriptLogRaw(ctx context.Context, w http.ResponseWriter, info session.Info, logPath string) {
	s.streamSessionTranscriptLogRawWithEmitter(ctx, newSSESessionStreamEmitter(w), info, logPath)
}

func (s *Server) streamSessionTranscriptLog(ctx context.Context, w http.ResponseWriter, info session.Info, logPath string) {
	s.streamSessionTranscriptLogWithEmitter(ctx, newSSESessionStreamEmitter(w), info, logPath)
}

// streamSessionPeekRaw polls tmux pane content and wraps it as format=raw
// messages so MC's JSONL rendering pipeline can display terminal output
// (e.g. OAuth prompts, startup screens) when no transcript log exists yet.
func (s *Server) streamSessionPeekRaw(ctx context.Context, w http.ResponseWriter, info session.Info) {
	s.streamSessionPeekRawWithEmitter(ctx, newSSESessionStreamEmitter(w), info)
}

func (s *Server) streamSessionPeek(ctx context.Context, w http.ResponseWriter, info session.Info) {
	s.streamSessionPeekWithEmitter(ctx, newSSESessionStreamEmitter(w), info)
}
