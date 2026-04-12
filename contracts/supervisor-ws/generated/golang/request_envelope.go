
package wscontract

type RequestEnvelope struct {
  ReservedType *AnonymousSchema_1 `json:"type" binding:"required"`
  Id string `json:"id" binding:"required"`
  Action *AnonymousSchema_3 `json:"action" binding:"required"`
  IdempotencyKey string `json:"idempotency_key,omitempty"`
  Scope *Scope `json:"scope,omitempty"`
  Payload *RequestPayload `json:"payload,omitempty"`
}