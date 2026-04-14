package api

import (
	"github.com/gastownhall/gascity/internal/config"
)

func init() {
	RegisterVoidAction("patches.agents.list", ActionDef{
		Description:       "List agent patches",
		RequiresCityScope: true,
	}, func(s *Server) (listResponse, error) {
		cfg := s.state.Config()
		patches := cfg.Patches.Agents
		if patches == nil {
			patches = []config.AgentPatch{}
		}
		return listResponse{Items: patches, Total: len(patches)}, nil
	})

	RegisterAction("patches.agent.get", ActionDef{
		Description:       "Get agent patch",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (config.AgentPatch, error) {
		cfg := s.state.Config()
		dir, base := config.ParseQualifiedName(payload.Name)
		for _, p := range cfg.Patches.Agents {
			if p.Dir == dir && p.Name == base {
				return p, nil
			}
		}
		return config.AgentPatch{}, httpError{status: 404, code: "not_found", message: "agent patch " + payload.Name + " not found"}
	})

	RegisterAction("patches.agents.set", ActionDef{
		Description:       "Set agent patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, patch config.AgentPatch) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if patch.Name == "" {
			return nil, httpError{status: 400, code: "invalid", message: "name is required"}
		}
		if err := sm.SetAgentPatch(patch); err != nil {
			return nil, err
		}
		qn := patch.Name
		if patch.Dir != "" {
			qn = patch.Dir + "/" + patch.Name
		}
		return map[string]string{"status": "ok", "agent_patch": qn}, nil
	})

	RegisterAction("patches.agent.delete", ActionDef{
		Description:       "Delete agent patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.DeleteAgentPatch(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted", "agent_patch": payload.Name}, nil
	})

	RegisterVoidAction("patches.rigs.list", ActionDef{
		Description:       "List rig patches",
		RequiresCityScope: true,
	}, func(s *Server) (listResponse, error) {
		cfg := s.state.Config()
		patches := cfg.Patches.Rigs
		if patches == nil {
			patches = []config.RigPatch{}
		}
		return listResponse{Items: patches, Total: len(patches)}, nil
	})

	RegisterAction("patches.rig.get", ActionDef{
		Description:       "Get rig patch",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (config.RigPatch, error) {
		cfg := s.state.Config()
		for _, p := range cfg.Patches.Rigs {
			if p.Name == payload.Name {
				return p, nil
			}
		}
		return config.RigPatch{}, httpError{status: 404, code: "not_found", message: "rig patch " + payload.Name + " not found"}
	})

	RegisterAction("patches.rigs.set", ActionDef{
		Description:       "Set rig patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, patch config.RigPatch) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if patch.Name == "" {
			return nil, httpError{status: 400, code: "invalid", message: "name is required"}
		}
		if err := sm.SetRigPatch(patch); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok", "rig_patch": patch.Name}, nil
	})

	RegisterAction("patches.rig.delete", ActionDef{
		Description:       "Delete rig patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.DeleteRigPatch(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted", "rig_patch": payload.Name}, nil
	})

	RegisterVoidAction("patches.providers.list", ActionDef{
		Description:       "List provider patches",
		RequiresCityScope: true,
	}, func(s *Server) (listResponse, error) {
		cfg := s.state.Config()
		patches := cfg.Patches.Providers
		if patches == nil {
			patches = []config.ProviderPatch{}
		}
		return listResponse{Items: patches, Total: len(patches)}, nil
	})

	RegisterAction("patches.provider.get", ActionDef{
		Description:       "Get provider patch",
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (config.ProviderPatch, error) {
		cfg := s.state.Config()
		for _, p := range cfg.Patches.Providers {
			if p.Name == payload.Name {
				return p, nil
			}
		}
		return config.ProviderPatch{}, httpError{status: 404, code: "not_found", message: "provider patch " + payload.Name + " not found"}
	})

	RegisterAction("patches.providers.set", ActionDef{
		Description:       "Set provider patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, patch config.ProviderPatch) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if patch.Name == "" {
			return nil, httpError{status: 400, code: "invalid", message: "name is required"}
		}
		if err := sm.SetProviderPatch(patch); err != nil {
			return nil, err
		}
		return map[string]string{"status": "ok", "provider_patch": patch.Name}, nil
	})

	RegisterAction("patches.provider.delete", ActionDef{
		Description:       "Delete provider patch",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketNamePayload) (map[string]string, error) {
		sm, ok := s.state.(StateMutator)
		if !ok {
			return nil, httpError{status: 500, code: "internal", message: "mutations not supported"}
		}
		if err := sm.DeleteProviderPatch(payload.Name); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted", "provider_patch": payload.Name}, nil
	})
}
