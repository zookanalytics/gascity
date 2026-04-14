package api

func init() {
	RegisterVoidAction("orders.list", ActionDef{
		Description:       "List orders",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server) (map[string]any, error) {
		aa := s.state.Orders()
		resp := make([]orderResponse, len(aa))
		for i, a := range aa {
			resp[i] = toOrderResponse(a)
		}
		return map[string]any{"orders": resp}, nil
	})

	RegisterVoidAction("orders.check", ActionDef{
		Description:       "Check order gate conditions",
		RequiresCityScope: true,
	}, func(s *Server) (map[string]any, error) {
		return s.checkOrders(), nil
	})

	RegisterAction("orders.history", ActionDef{
		Description:       "Get order execution history",
		RequiresCityScope: true,
	}, func(s *Server, payload socketOrdersHistoryPayload) (any, error) {
		return s.getOrderHistory(payload.ScopedName, payload.Limit, payload.Before)
	})

	RegisterAction("orders.feed", ActionDef{
		Description:       "Get order activity feed",
		RequiresCityScope: true,
	}, func(s *Server, payload socketOrdersFeedPayload) (any, error) {
		return s.getOrdersFeed(payload.ScopeKind, payload.ScopeRef, payload.Limit)
	})

	RegisterAction("order.get", ActionDef{
		Description:       "Get order details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (orderResponse, error) {
		a, err := resolveOrder(s.state.Orders(), payload.Name)
		if err != nil {
			return orderResponse{}, err
		}
		return toOrderResponse(*a), nil
	})

	RegisterAction("order.enable", ActionDef{
		Description:       "Enable an order",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		a, err := resolveOrder(s.state.Orders(), payload.Name)
		if err != nil {
			return nil, err
		}
		if err := sm.EnableOrder(a.Name, a.Rig); err != nil {
			return nil, err
		}
		return map[string]string{"status": "enabled", "order": a.Name}, nil
	})

	RegisterAction("order.disable", ActionDef{
		Description:       "Disable an order",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		a, err := resolveOrder(s.state.Orders(), payload.Name)
		if err != nil {
			return nil, err
		}
		if err := sm.DisableOrder(a.Name, a.Rig); err != nil {
			return nil, err
		}
		return map[string]string{"status": "disabled", "order": a.Name}, nil
	})

	RegisterAction("order.history.detail", ActionDef{
		Description:       "Get order history detail",
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (any, error) {
		store := s.state.CityBeadStore()
		if store == nil {
			return nil, httpError{status: 503, code: "unavailable", message: "no bead store configured"}
		}
		bead, err := store.Get(payload.ID)
		if err != nil {
			return nil, err
		}
		return bead, nil
	})
}
