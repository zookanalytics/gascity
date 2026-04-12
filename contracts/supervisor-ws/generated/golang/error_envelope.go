
package wscontract

type ErrorEnvelope struct {
  ReservedType *AnonymousSchema_58 `json:"type" binding:"required"`
  Id string `json:"id,omitempty"`
  Code string `json:"code" binding:"required"`
  Message string `json:"message" binding:"required"`
  Details []FieldError `json:"details,omitempty"`
}