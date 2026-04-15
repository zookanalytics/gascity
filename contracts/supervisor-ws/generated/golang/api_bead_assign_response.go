
package wscontract

type ApiBeadAssignResponse struct {
  Assignee string `json:"assignee,omitempty"`
  Status string `json:"status,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}