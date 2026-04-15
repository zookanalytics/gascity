
package wscontract

type ApiConvoySnapshotResponse struct {
  Children []BeadsBead `json:"children,omitempty"`
  Convoy *BeadsBead `json:"convoy,omitempty"`
  Progress *ApiConvoyProgressResponse `json:"progress,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}