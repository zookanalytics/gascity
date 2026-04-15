
package wscontract

type ApiSessionAgentDetailResponse struct {
  Messages []interface{} `json:"messages,omitempty"`
  Status string `json:"status,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}