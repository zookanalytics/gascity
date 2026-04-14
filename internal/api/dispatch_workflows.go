package api

func init() {
	// workflow.get needs a single consistent index for both the snapshot body
	// and the response envelope. The generic framework calls latestIndex()
	// separately, which can diverge with incrementing event providers.
	// Keep on legacy switch until the framework supports caller-controlled index.
	RegisterMeta("workflow.get", ActionDef{
		Description:       "Get workflow snapshot",
		RequiresCityScope: true,
	})

	RegisterAction("workflow.delete", ActionDef{
		Description:       "Delete a workflow",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketWorkflowDeletePayload) (any, error) {
		if payload.ID == "" {
			return nil, httpError{status: 400, code: "invalid", message: "id is required"}
		}
		return s.deleteWorkflow(payload.ID, payload.ScopeKind, payload.ScopeRef, payload.Delete)
	})
}
