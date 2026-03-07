package event

import (
	"testing"
	"time"
)

func validEntry() AuditEntry {
	return AuditEntry{
		ID:         "ae-1",
		Timestamp:  time.Now(),
		Actor:      "user@example.com",
		Action:     "workspace.create",
		Resource:   "workspace",
		ResourceID: "ws-123",
		Outcome:    "success",
	}
}

func TestAuditEntryValidateSuccess(t *testing.T) {
	e := validEntry()
	if err := e.Validate(); err != nil {
		t.Errorf("valid entry should not error: %v", err)
	}
}

func TestAuditEntryValidateRequiredFields(t *testing.T) {
	fields := []struct {
		name string
		mod  func(*AuditEntry)
	}{
		{"id", func(a *AuditEntry) { a.ID = "" }},
		{"timestamp", func(a *AuditEntry) { a.Timestamp = time.Time{} }},
		{"actor", func(a *AuditEntry) { a.Actor = "" }},
		{"action", func(a *AuditEntry) { a.Action = "" }},
		{"resource", func(a *AuditEntry) { a.Resource = "" }},
		{"resource_id", func(a *AuditEntry) { a.ResourceID = "" }},
	}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			e := validEntry()
			tc.mod(&e)
			if err := e.Validate(); err == nil {
				t.Errorf("missing %s should cause validation error", tc.name)
			}
		})
	}
}

func TestAuditEntryValidateOutcome(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "denied"} {
		e := validEntry()
		e.Outcome = outcome
		if err := e.Validate(); err != nil {
			t.Errorf("outcome %q should be valid: %v", outcome, err)
		}
	}

	e := validEntry()
	e.Outcome = "invalid"
	if err := e.Validate(); err == nil {
		t.Error("invalid outcome should cause validation error")
	}
}

func TestAuditEntryOptionalFields(t *testing.T) {
	e := validEntry()
	e.Detail = "extra detail"
	e.WorkspaceID = "ws-456"
	e.IP = "192.168.1.1"
	if err := e.Validate(); err != nil {
		t.Errorf("optional fields should not cause error: %v", err)
	}
}
