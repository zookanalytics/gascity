
package wscontract

type EventEnvelope struct {
  ReservedType *AnonymousSchema_65 `json:"type" binding:"required"`
  SubscriptionId string `json:"subscription_id" binding:"required"`
  EventType string `json:"event_type" binding:"required"`
  Index int `json:"index,omitempty"`
  Cursor string `json:"cursor,omitempty"`
  Payload map[string]interface{} `json:"payload,omitempty"`
}