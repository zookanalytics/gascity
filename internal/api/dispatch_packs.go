package api

func init() {
	RegisterVoidAction("packs.list", ActionDef{
		Description:       "List packs",
		RequiresCityScope: true,
	}, func(s *Server) (map[string]any, error) {
		return map[string]any{"packs": s.listPacks()}, nil
	})
}
