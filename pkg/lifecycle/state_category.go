package lifecycle

// StateCategory groups states for forward-compatible handling. Consumers
// that encounter an unknown state can fall back to its category.
type StateCategory string

// State categories for forward-compatible grouping.
const (
	CategoryActive        StateCategory = "active"
	CategoryTransitioning StateCategory = "transitioning"
	CategoryTerminal      StateCategory = "terminal"
	CategoryError         StateCategory = "error"
)
