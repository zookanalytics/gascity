
package wscontract

type ApiConvoyProgressResponse struct {
  Closed int `json:"closed,omitempty"`
  Total int `json:"total,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}