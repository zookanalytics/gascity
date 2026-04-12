
package wscontract

type SubscriptionStopRequest struct {
  SubscriptionId string `json:"subscription_id" binding:"required"`
}