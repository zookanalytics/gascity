
package wscontract

type BeadsListRequest struct {
  Status string `json:"status,omitempty"`
  ReservedType string `json:"type,omitempty"`
  Label string `json:"label,omitempty"`
  Assignee string `json:"assignee,omitempty"`
  Rig string `json:"rig,omitempty"`
}