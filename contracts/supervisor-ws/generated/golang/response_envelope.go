
package wscontract

type ResponseEnvelope struct {
  ReservedType *AnonymousSchema_54 `json:"type" binding:"required"`
  Id string `json:"id" binding:"required"`
  Index int `json:"index,omitempty"`
  Result map[string]interface{} `json:"result,omitempty"`
}