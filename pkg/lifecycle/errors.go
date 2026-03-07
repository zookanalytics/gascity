package lifecycle

import "fmt"

// InvalidTransitionError is returned when a state transition is not allowed.
type InvalidTransitionError struct {
	Entity string
	From   string
	To     string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid %s transition: %s -> %s", e.Entity, e.From, e.To)
}
