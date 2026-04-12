
package wscontract

type EventsListRequest struct {
  ReservedType string `json:"type,omitempty"`
  Actor string `json:"actor,omitempty"`
  Since string `json:"since,omitempty"`
}