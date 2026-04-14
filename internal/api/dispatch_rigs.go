package api

import (
	"github.com/gastownhall/gascity/internal/config"
)

func init() {
	RegisterVoidAction("rigs.list", ActionDef{
		Description:       "List rigs",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server) (listResponse, error) {
		items := s.listRigResponses(false)
		return listResponse{Items: items, Total: len(items)}, nil
	})

	RegisterAction("rig.get", ActionDef{
		Description:       "Get rig details",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		resp, ok := s.getRigResponse(payload.Name, false)
		if !ok {
			return nil, httpError{status: 404, code: "not_found", message: "rig " + payload.Name + " not found"}
		}
		return resp, nil
	})

	RegisterAction("rig.suspend", ActionDef{
		Description:       "Suspend a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		result, err := s.applyRigAction(payload.Name, "suspend")
		if err != nil {
			return nil, err
		}
		return result, nil
	})

	RegisterAction("rig.resume", ActionDef{
		Description:       "Resume a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		result, err := s.applyRigAction(payload.Name, "resume")
		if err != nil {
			return nil, err
		}
		return result, nil
	})

	RegisterAction("rig.restart", ActionDef{
		Description:       "Restart a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (any, error) {
		result, err := s.applyRigAction(payload.Name, "restart")
		if err != nil {
			return nil, err
		}
		return result, nil
	})

	RegisterAction("rig.create", ActionDef{
		Description:       "Create a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketRigCreatePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if payload.Name == "" {
			return nil, httpError{status: 400, code: "invalid", message: "name is required"}
		}
		if err := sm.CreateRig(config.Rig{Name: payload.Name, Path: payload.Path}); err != nil {
			return nil, err
		}
		return map[string]string{"status": "created", "rig": payload.Name}, nil
	})

	RegisterAction("rig.update", ActionDef{
		Description:       "Update a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketRigUpdatePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.UpdateRig(payload.Name, RigUpdate{Path: payload.Path, Prefix: payload.Prefix, Suspended: payload.Suspended}); err != nil {
			return nil, err
		}
		return map[string]string{"status": "updated", "rig": payload.Name}, nil
	})

	RegisterAction("rig.delete", ActionDef{
		Description:       "Delete a rig",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.DeleteRig(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted", "rig": payload.Name}, nil
	})
}
