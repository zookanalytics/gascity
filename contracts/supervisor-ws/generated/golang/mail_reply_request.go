
package wscontract

type MailReplyRequest struct {
  Id string `json:"id" binding:"required"`
  Rig string `json:"rig,omitempty"`
  From string `json:"from" binding:"required"`
  Subject string `json:"subject" binding:"required"`
  Body string `json:"body" binding:"required"`
}