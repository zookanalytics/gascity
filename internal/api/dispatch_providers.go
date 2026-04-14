package api

func init() {
	RegisterAction("providers.list", ActionDef{
		Description:       "List providers",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server, payload socketProvidersListPayload) (listResponse, error) {
		items := s.listProviders(payload.View == "public")
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("provider.get", ActionDef{
		Description:       "Get provider details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		provider, err := s.getProvider(payload.Name)
		if err != nil {
			return nil, err
		}
		return provider, nil
	})

	RegisterAction("provider.create", ActionDef{
		Description:       "Create a provider",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketProviderCreatePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if payload.Name == "" {
			return nil, httpError{status: 400, code: "invalid", message: "name is required"}
		}
		if err := sm.CreateProvider(payload.Name, payload.Spec); err != nil {
			return nil, err
		}
		return map[string]string{"status": "created", "provider": payload.Name}, nil
	})

	RegisterAction("provider.update", ActionDef{
		Description:       "Update a provider",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketProviderUpdatePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.UpdateProvider(payload.Name, payload.Update); err != nil {
			return nil, err
		}
		return map[string]string{"status": "updated", "provider": payload.Name}, nil
	})

	RegisterAction("provider.delete", ActionDef{
		Description:       "Delete a provider",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.DeleteProvider(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted", "provider": payload.Name}, nil
	})
}
