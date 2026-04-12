
package wscontract

type MailListRequest struct {
  Agent string `json:"agent,omitempty"`
  Status string `json:"status,omitempty"`
  Rig string `json:"rig,omitempty"`
}