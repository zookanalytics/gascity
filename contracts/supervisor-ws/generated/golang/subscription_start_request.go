
package wscontract

type SubscriptionStartRequest struct {
  Kind *AnonymousSchema_43 `json:"kind" binding:"required"`
  Id string `json:"id,omitempty"`
  Cursor string `json:"cursor,omitempty"`
  Format *AnonymousSchema_46 `json:"format,omitempty"`
}