package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
	workdirutil "github.com/gastownhall/gascity/internal/workdir"
	"github.com/gastownhall/gascity/internal/worker"
)

// outputTurn is a single conversation turn in the unified output response.
type outputTurn struct {
	Role      string `json:"role"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp,omitempty"`
}

// agentOutputResponse is the response for GET /v0/agent/{name}/output.
type agentOutputResponse struct {
	Agent      string                       `json:"agent"`
	Format     string                       `json:"format"` // "conversation" or "text"
	Turns      []outputTurn                 `json:"turns"`
	Pagination *worker.TranscriptPagination `json:"pagination,omitempty"`
}

type agentPeekHandle interface {
	worker.LiveObservationHandle
	worker.StateHandle
	worker.PeekHandle
}

// trySessionLogOutputHuma is the Huma-compatible variant of trySessionLogOutput.
// tail carries the client's ?tail= value; tailProvided reports whether the
// client supplied the param at all.
func (s *Server) trySessionLogOutputHuma(name string, agentCfg config.Agent, tailInput int, tailProvided bool, before string) (*agentOutputResponse, error) {
	cfg := s.state.Config()
	workDir := s.resolveAgentWorkDir(agentCfg, name)
	if workDir == "" {
		return nil, nil
	}
	provider := strings.TrimSpace(agentCfg.Provider)
	if provider == "" && cfg != nil {
		provider = strings.TrimSpace(cfg.Workspace.Provider)
	}
	factory, err := s.workerFactory(s.state.CityBeadStore())
	if err != nil {
		return nil, err
	}
	path := factory.DiscoverTranscript(provider, workDir, "")
	if path == "" {
		return nil, nil
	}

	tail := 1
	if tailProvided {
		tail = tailInput
	}

	transcript, err := factory.ReadTranscript(worker.TranscriptRequest{
		Provider:        provider,
		TranscriptPath:  path,
		TailCompactions: tail,
		BeforeEntryID:   before,
	})
	if err != nil {
		return nil, err
	}
	sess := transcript.Session

	turns := make([]outputTurn, 0, len(sess.Messages))
	for _, e := range sess.Messages {
		turn := entryToTurn(e)
		if turn.Text == "" {
			continue
		}
		turns = append(turns, turn)
	}

	return &agentOutputResponse{
		Agent:      name,
		Format:     "conversation",
		Turns:      turns,
		Pagination: sess.Pagination,
	}, nil
}

// handleAgentOutput returns unified conversation output for an agent.
// Tries structured session logs first, falls back to Peek().
func (s *Server) handleAgentOutput(w http.ResponseWriter, r *http.Request, name string) {
	cfg := s.state.Config()
	agentCfg, ok := findAgent(cfg, name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "agent "+name+" not found")
		return
	}

	resp, err := s.trySessionLogOutput(r, name, agentCfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "reading session log: "+err.Error())
		return
	}
	if resp != nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	handle := s.agentWorkerHandle(name, cfg)
	s.peekFallbackOutput(r.Context(), w, name, handle)
}

// trySessionLogOutput is the legacy HTTP wrapper around the shared Huma
// transcript reader.
func (s *Server) trySessionLogOutput(r *http.Request, name string, agentCfg config.Agent) (*agentOutputResponse, error) {
	tailInput := 0
	tailProvided := false
	if rawTail := r.URL.Query().Get("tail"); rawTail != "" {
		tailProvided = true
		if parsedTail, err := strconv.Atoi(rawTail); err == nil && parsedTail >= 0 {
			tailInput = parsedTail
		}
	}
	return s.trySessionLogOutputHuma(name, agentCfg, tailInput, tailProvided, r.URL.Query().Get("before"))
}

// peekFallbackOutput returns raw terminal text wrapped as a single turn.
func (s *Server) peekFallbackOutput(ctx context.Context, w http.ResponseWriter, name string, handle agentPeekHandle) {
	running, err := workerHandleRunning(ctx, handle)
	if err != nil || !running {
		writeError(w, http.StatusNotFound, "not_found", "agent "+name+" not running")
		return
	}

	output, err := handle.Peek(ctx, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	turns := []outputTurn{}
	if output != "" {
		turns = append(turns, outputTurn{Role: "output", Text: output})
	}

	writeJSON(w, http.StatusOK, agentOutputResponse{
		Agent:  name,
		Format: "text",
		Turns:  turns,
	})
}

// resolveAgentWorkDir returns the absolute working directory for an agent,
// honoring work_dir template expansion.
func (s *Server) resolveAgentWorkDir(a config.Agent, qualifiedName string) string {
	cfg := s.state.Config()
	return workdirutil.ResolveWorkDirPath(
		s.state.CityPath(),
		workdirutil.CityName(s.state.CityPath(), cfg),
		qualifiedName,
		a,
		cfg.Rigs,
	)
}

func (s *Server) agentWorkerHandle(name string, cfg *config.City) agentPeekHandle {
	if cfg == nil {
		return nil
	}
	sessionName := agentSessionName(s.state.CityName(), name, cfg.Workspace.SessionTemplate)
	handle, _ := s.workerHandleForSessionTarget(s.state.CityBeadStore(), sessionName)
	return handle
}

func workerHandleRunning(ctx context.Context, handle interface {
	worker.LiveObservationHandle
	worker.StateHandle
},
) (bool, error) {
	if handle == nil {
		return false, nil
	}
	obs, err := worker.ObserveHandle(ctx, handle)
	if err == nil {
		return obs.Running, nil
	}
	state, stateErr := handle.State(ctx)
	if stateErr != nil {
		if errors.Is(err, worker.ErrOperationUnsupported) {
			return false, stateErr
		}
		return false, err
	}
	return state.Phase != worker.PhaseStopped && state.Phase != worker.PhaseFailed, nil
}
