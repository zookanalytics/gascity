package event

import (
	"errors"
	"time"
)

// AuditEntry records a security-relevant action for compliance logging.
type AuditEntry struct {
	ID          string    `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	Actor       string    `json:"actor"`
	Action      string    `json:"action"`
	Resource    string    `json:"resource"`
	ResourceID  string    `json:"resource_id"`
	Outcome     string    `json:"outcome"` // "success", "failure", "denied"
	Detail      string    `json:"detail,omitempty"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	IP          string    `json:"ip,omitempty"`
}

// Validate checks that required fields are present and Outcome is valid.
func (a *AuditEntry) Validate() error {
	if a.ID == "" {
		return errors.New("audit entry: missing id")
	}
	if a.Timestamp.IsZero() {
		return errors.New("audit entry: missing timestamp")
	}
	if a.Actor == "" {
		return errors.New("audit entry: missing actor")
	}
	if a.Action == "" {
		return errors.New("audit entry: missing action")
	}
	if a.Resource == "" {
		return errors.New("audit entry: missing resource")
	}
	if a.ResourceID == "" {
		return errors.New("audit entry: missing resource_id")
	}
	switch a.Outcome {
	case "success", "failure", "denied":
		// valid
	default:
		return errors.New("audit entry: outcome must be success, failure, or denied")
	}
	return nil
}
