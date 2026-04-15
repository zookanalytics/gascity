
package wscontract

type ApiSessionSubmitResponse struct {
  Id string `json:"id,omitempty"`
  Intent string `json:"intent,omitempty"`
  Queued bool `json:"queued,omitempty"`
  Status string `json:"status,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}