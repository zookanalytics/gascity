
package wscontract

type ApiSessionAgentsResponse struct {
  Agents []SessionlogAgentMapping `json:"agents,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}