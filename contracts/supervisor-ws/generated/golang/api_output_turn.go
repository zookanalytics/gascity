
package wscontract

type ApiOutputTurn struct {
  Role string `json:"role,omitempty"`
  Text string `json:"text,omitempty"`
  Timestamp string `json:"timestamp,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}