
package wscontract

type ApiSessionTranscriptResult struct {
  Format string `json:"format,omitempty"`
  Id string `json:"id,omitempty"`
  Messages []map[string]interface{} `json:"messages,omitempty"`
  Pagination *SessionlogPaginationInfo `json:"pagination,omitempty"`
  Template string `json:"template,omitempty"`
  Turns []ApiOutputTurn `json:"turns,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}