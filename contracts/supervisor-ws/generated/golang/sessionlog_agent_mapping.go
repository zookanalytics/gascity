
package wscontract

type SessionlogAgentMapping struct {
  AgentId string `json:"agent_id,omitempty"`
  ParentToolUseId string `json:"parent_tool_use_id,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}