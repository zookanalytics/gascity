
package wscontract

type ApiBeadUpdateRequest struct {
  Assignee *string `json:"assignee,omitempty"`
  Description *string `json:"description,omitempty"`
  Id string `json:"id,omitempty"`
  Labels []string `json:"labels,omitempty"`
  Metadata map[string]string `json:"metadata,omitempty"`
  Priority map[string]interface{} `json:"priority,omitempty"`
  RemoveLabels []string `json:"remove_labels,omitempty"`
  Status *string `json:"status,omitempty"`
  Title *string `json:"title,omitempty"`
  ReservedType *string `json:"type,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}