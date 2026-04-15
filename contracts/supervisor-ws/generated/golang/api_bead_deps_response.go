
package wscontract

type ApiBeadDepsResponse struct {
  Children []BeadsBead `json:"children,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}