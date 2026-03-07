package lifecycle

// RuntimeState represents the lifecycle state of an agent runtime
// within a hosted workspace.
type RuntimeState string

// Runtime lifecycle states.
const (
	RuntimeStarting   RuntimeState = "starting"
	RuntimeActive     RuntimeState = "active"
	RuntimePausing    RuntimeState = "pausing"
	RuntimePaused     RuntimeState = "paused"
	RuntimeResuming   RuntimeState = "resuming"
	RuntimeCompleting RuntimeState = "completing"
	RuntimeCompleted  RuntimeState = "completed"
	RuntimeFailed     RuntimeState = "failed"
	RuntimeExpired    RuntimeState = "expired"
)

var runtimeTransitions = []Transition[RuntimeState]{
	{RuntimeStarting, RuntimeActive, "runtime ready"},
	{RuntimeStarting, RuntimeFailed, "start failed"},
	{RuntimeActive, RuntimePausing, "pause requested"},
	{RuntimeActive, RuntimeCompleting, "completion requested"},
	{RuntimeActive, RuntimeFailed, "runtime error"},
	{RuntimeActive, RuntimeExpired, "timeout reached"},
	{RuntimePausing, RuntimePaused, "pause completed"},
	{RuntimePausing, RuntimeFailed, "pause failed"},
	{RuntimePaused, RuntimeResuming, "resume requested"},
	{RuntimePaused, RuntimeExpired, "timeout reached"},
	{RuntimeResuming, RuntimeActive, "resume completed"},
	{RuntimeResuming, RuntimeFailed, "resume failed"},
	{RuntimeCompleting, RuntimeCompleted, "completion finished"},
	{RuntimeCompleting, RuntimeFailed, "completion failed"},
}

var runtimeTransitionSet = buildTransitionSet(runtimeTransitions)

// ValidRuntimeTransitions returns all valid runtime state transitions.
func ValidRuntimeTransitions() []Transition[RuntimeState] {
	out := make([]Transition[RuntimeState], len(runtimeTransitions))
	copy(out, runtimeTransitions)
	return out
}

// CanTransitionRuntime reports whether transitioning from one runtime
// state to another is valid.
func CanTransitionRuntime(from, to RuntimeState) bool {
	return runtimeTransitionSet[transitionKey{string(from), string(to)}]
}

// TransitionRuntime validates and performs a runtime state transition.
// Returns the new state or an InvalidTransitionError.
func TransitionRuntime(from, to RuntimeState) (RuntimeState, error) {
	if !CanTransitionRuntime(from, to) {
		return from, &InvalidTransitionError{Entity: "runtime", From: string(from), To: string(to)}
	}
	return to, nil
}

// RuntimeStateCategory returns the category for a runtime state.
func RuntimeStateCategory(s RuntimeState) StateCategory {
	switch s {
	case RuntimeActive, RuntimePaused:
		return CategoryActive
	case RuntimeStarting, RuntimePausing, RuntimeResuming, RuntimeCompleting:
		return CategoryTransitioning
	case RuntimeCompleted, RuntimeExpired:
		return CategoryTerminal
	case RuntimeFailed:
		return CategoryError
	default:
		return CategoryError
	}
}

// IsTerminalRuntime reports whether the runtime state is terminal
// (no further transitions possible).
func IsTerminalRuntime(s RuntimeState) bool {
	switch s {
	case RuntimeCompleted, RuntimeFailed, RuntimeExpired:
		return true
	default:
		return false
	}
}
