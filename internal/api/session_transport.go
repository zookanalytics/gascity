package api

import (
	"fmt"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionacp "github.com/gastownhall/gascity/internal/runtime/acp"
)

type acpRoutingProvider interface {
	RouteACP(name string)
}

func providerSessionTransport(resolved *config.ResolvedProvider, sp runtime.Provider) (string, error) {
	if resolved == nil || resolved.DefaultSessionTransport() != "acp" {
		return "", nil
	}
	if transportSupportsACP(sp) {
		return "acp", nil
	}
	return "", fmt.Errorf("provider %q requires ACP transport but the session provider cannot route ACP sessions", resolved.Name)
}

func transportSupportsACP(sp runtime.Provider) bool {
	if sp == nil {
		return false
	}
	if _, ok := sp.(acpRoutingProvider); ok {
		return true
	}
	_, ok := sp.(*sessionacp.Provider)
	return ok
}
