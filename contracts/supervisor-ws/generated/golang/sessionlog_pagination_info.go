
package wscontract

type SessionlogPaginationInfo struct {
  HasOlderMessages bool `json:"has_older_messages,omitempty"`
  ReturnedMessageCount int `json:"returned_message_count,omitempty"`
  TotalCompactions int `json:"total_compactions,omitempty"`
  TotalMessageCount int `json:"total_message_count,omitempty"`
  TruncatedBeforeMessage string `json:"truncated_before_message,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}