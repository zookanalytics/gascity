
package wscontract

type ApiConvoyCheckResponse struct {
  Closed int `json:"closed,omitempty"`
  Complete bool `json:"complete,omitempty"`
  ConvoyId string `json:"convoy_id,omitempty"`
  Total int `json:"total,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}