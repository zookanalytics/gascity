package api

func init() {
	RegisterVoidAction("services.list", ActionDef{
		Description:       "List services",
		RequiresCityScope: true,
	}, func(s *Server) (listResponse, error) {
		items := s.listServices()
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("service.get", ActionDef{
		Description:       "Get service details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		service, err := s.getService(payload.Name)
		if err != nil {
			return nil, err
		}
		return service, nil
	})

	RegisterAction("service.restart", ActionDef{
		Description:       "Restart a service",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		if err := s.restartService(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok", "action": "restart", "service": payload.Name}, nil
	})
}
