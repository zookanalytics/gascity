package api

import "context"

func init() {
	RegisterAction("formulas.list", ActionDef{
		Description:       "List formulas",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server, payload socketFormulaScopePayload) (any, error) {
		return s.listFormulas(payload.ScopeKind, payload.ScopeRef)
	})

	RegisterAction("formulas.feed", ActionDef{
		Description:       "Get formula activity feed",
		RequiresCityScope: true,
	}, func(s *Server, payload socketFormulaFeedPayload) (any, error) {
		return s.getFormulaFeed(payload.ScopeKind, payload.ScopeRef, payload.Limit)
	})

	RegisterAction("formula.get", ActionDef{
		Description:       "Get formula details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketFormulaGetPayload) (any, error) {
		return s.getFormulaDetail(context.Background(), payload.Name, payload.ScopeKind, payload.ScopeRef, payload.Target, payload.Vars)
	})

	RegisterAction("formula.runs", ActionDef{
		Description:       "Get formula run history",
		RequiresCityScope: true,
	}, func(s *Server, payload socketFormulaRunsPayload) (any, error) {
		return s.getFormulaRuns(payload.Name, payload.ScopeKind, payload.ScopeRef, payload.Limit)
	})
}
