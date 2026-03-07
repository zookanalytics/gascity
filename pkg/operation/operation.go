package operation

import (
	"fmt"
	"time"
)

// Phase represents the lifecycle phase of an operation.
type Phase string

// Operation phase values.
const (
	Pending   Phase = "pending"
	Running   Phase = "running"
	Succeeded Phase = "succeeded"
	Failed    Phase = "failed"
	Canceled  Phase = "canceled"
)

// Operation tracks a long-running process.
type Operation struct {
	ID          string            `json:"id"`
	Kind        string            `json:"kind"` // e.g. "workspace.create", "bundle.compile"
	Phase       Phase             `json:"phase"`
	ResourceID  string            `json:"resource_id,omitempty"`
	Message     string            `json:"message,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Error       string            `json:"error,omitempty"`
}

var validPhaseTransitions = map[Phase][]Phase{
	Pending: {Running, Canceled, Failed},
	Running: {Succeeded, Failed, Canceled},
}

// IsTerminal reports whether the operation is in a terminal phase.
func (o *Operation) IsTerminal() bool {
	switch o.Phase {
	case Succeeded, Failed, Canceled:
		return true
	default:
		return false
	}
}

// Transition moves the operation to a new phase. Returns an error if
// the transition is invalid. Updates UpdatedAt and sets CompletedAt
// for terminal phases.
func (o *Operation) Transition(to Phase) error {
	allowed := validPhaseTransitions[o.Phase]
	for _, p := range allowed {
		if p == to {
			o.Phase = to
			now := time.Now().UTC()
			o.UpdatedAt = now
			if o.IsTerminal() {
				o.CompletedAt = &now
			}
			return nil
		}
	}
	return fmt.Errorf("invalid operation transition: %s -> %s", o.Phase, to)
}
