package api

func init() {
	RegisterVoidAction("convoys.list", ActionDef{
		Description:       "List convoys",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server) (listResponse, error) {
		items := s.listConvoys()
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("convoy.get", ActionDef{
		Description:       "Get convoy details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]any, error) {
		return s.getConvoySnapshot(payload.ID)
	})

	RegisterAction("convoy.create", ActionDef{
		Description:       "Create a convoy",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload convoyCreateRequest) (any, error) {
		return s.createConvoy(payload)
	})

	RegisterAction("convoy.add", ActionDef{
		Description:       "Add items to a convoy",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketConvoyItemsPayload) (map[string]string, error) {
		if err := s.convoyAddItems(payload.ID, payload.Items); err != nil {
			return nil, err
		}
		return map[string]string{"status": "updated"}, nil
	})

	RegisterAction("convoy.remove", ActionDef{
		Description:       "Remove items from a convoy",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketConvoyItemsPayload) (map[string]string, error) {
		if err := s.convoyRemoveItems(payload.ID, payload.Items); err != nil {
			return nil, err
		}
		return map[string]string{"status": "updated"}, nil
	})

	RegisterAction("convoy.check", ActionDef{
		Description:       "Check convoy completion",
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (any, error) {
		return s.convoyCheck(payload.ID)
	})

	RegisterAction("convoy.close", ActionDef{
		Description:       "Close a convoy",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]string, error) {
		if err := s.convoyClose(payload.ID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "closed"}, nil
	})

	RegisterAction("convoy.delete", ActionDef{
		Description:       "Delete a convoy",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketIDPayload) (map[string]string, error) {
		if err := s.convoyDelete(payload.ID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted"}, nil
	})
}
