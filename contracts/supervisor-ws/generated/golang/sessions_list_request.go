
package wscontract

type SessionsListRequest struct {
  State string `json:"state,omitempty"`
  Template string `json:"template,omitempty"`
  Peek bool `json:"peek,omitempty"`
}