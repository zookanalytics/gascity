package api

func init() {
	RegisterVoidAction("city.get", ActionDef{
		Description:       "Get city details",
		RequiresCityScope: true,
	}, func(s *Server) (any, error) {
		return s.cityGet(), nil
	})

	RegisterAction("city.patch", ActionDef{
		Description:       "Update city (suspend/resume)",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, body cityPatchRequest) (map[string]string, error) {
		if body.Suspended == nil {
			return nil, httpError{status: 400, code: "invalid", message: "no fields to update"}
		}
		if err := s.patchCitySuspended(*body.Suspended); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok"}, nil
	})

	RegisterVoidAction("config.get", ActionDef{
		Description:       "Get parsed city configuration",
		RequiresCityScope: true,
	}, func(s *Server) (any, error) {
		return s.configGet(), nil
	})

	RegisterVoidAction("config.explain", ActionDef{
		Description:       "Explain config resolution",
		RequiresCityScope: true,
	}, func(s *Server) (any, error) {
		return s.configExplain(), nil
	})

	RegisterVoidAction("config.validate", ActionDef{
		Description:       "Validate city configuration",
		RequiresCityScope: true,
	}, func(s *Server) (any, error) {
		return s.configValidate(), nil
	})
}
