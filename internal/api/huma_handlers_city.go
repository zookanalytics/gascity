package api

import (
	"context"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/gastownhall/gascity/internal/events"
)

// humaHandleCityGet is the Huma-typed handler for GET /v0/city.
func (s *Server) humaHandleCityGet(_ context.Context, _ *CityGetInput) (*struct{ Body cityGetResponse }, error) {
	cfg := s.state.Config()
	resp := cityGetResponse{
		Name:            s.state.CityName(),
		Path:            s.state.CityPath(),
		Version:         s.state.Version(),
		Suspended:       cfg.Workspace.Suspended,
		Provider:        cfg.Workspace.Provider,
		SessionTemplate: cfg.Workspace.SessionTemplate,
		UptimeSec:       int(time.Since(s.state.StartedAt()).Seconds()),
		AgentCount:      len(cfg.Agents),
		RigCount:        len(cfg.Rigs),
	}
	return &struct{ Body cityGetResponse }{Body: resp}, nil
}

// humaHandleCityPatch is the Huma-typed handler for PATCH /v0/city.
func (s *Server) humaHandleCityPatch(_ context.Context, input *CityPatchInput) (*OKResponse, error) {
	sm, ok := s.state.(StateMutator)
	if !ok {
		return nil, errMutationsNotSupported
	}

	if input.Body.Suspended == nil {
		return nil, huma.Error400BadRequest("no fields to update")
	}

	var (
		err       error
		eventType string
	)
	if *input.Body.Suspended {
		err = sm.SuspendCity()
		eventType = events.CitySuspended
	} else {
		err = sm.ResumeCity()
		eventType = events.CityResumed
	}
	if err != nil {
		return nil, mutationError(err)
	}

	// Mirror the CLI fallback path in cmd_suspend.go: every transition
	// must record city.suspended/city.resumed so events.jsonl reflects
	// the change regardless of whether the operator hit the API or fell
	// through to direct file mutation.
	if ep := s.state.EventProvider(); ep != nil {
		ep.Record(events.Event{
			Type:  eventType,
			Actor: "api",
		})
	}

	resp := &OKResponse{}
	resp.Body.Status = "ok"
	return resp, nil
}

// humaHandleProviderReadiness is the Huma-typed handler for GET /v0/provider-readiness.
func (s *Server) humaHandleProviderReadiness(ctx context.Context, input *ProviderReadinessInput) (*ProviderReadinessOutput, error) {
	providers, err := parseRequestedReadinessItems(
		input.Providers,
		"providers",
		defaultProviderReadinessItems,
		supportedProviderReadiness,
	)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}

	resp, err := buildReadinessResponse(ctx, providers, input.Fresh)
	if err != nil {
		return nil, huma.Error500InternalServerError(err.Error())
	}

	providerResp := providerReadinessResponse{
		Providers: make(map[string]providerReadiness, len(providers)),
	}
	for _, provider := range providers {
		item := resp.Items[provider]
		providerResp.Providers[provider] = providerReadiness{
			DisplayName: item.DisplayName,
			Status:      item.Status,
			Detail:      item.Detail,
		}
	}

	return &ProviderReadinessOutput{Body: providerResp}, nil
}

// humaHandleReadiness is the Huma-typed handler for GET /v0/readiness.
func (s *Server) humaHandleReadiness(ctx context.Context, input *ReadinessInput) (*ReadinessOutput, error) {
	items, err := parseRequestedReadinessItems(
		input.Items,
		"items",
		defaultReadinessItems,
		supportedReadiness,
	)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}

	resp, err := buildReadinessResponse(ctx, items, input.Fresh)
	if err != nil {
		return nil, huma.Error500InternalServerError(err.Error())
	}

	return &ReadinessOutput{Body: resp}, nil
}
