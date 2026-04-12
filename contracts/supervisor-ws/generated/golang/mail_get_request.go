
package wscontract

type MailGetRequest struct {
  Id string `json:"id" binding:"required"`
  Rig string `json:"rig,omitempty"`
}