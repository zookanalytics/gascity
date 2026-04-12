
package wscontract

type MailSendRequest struct {
  Rig string `json:"rig,omitempty"`
  From string `json:"from,omitempty"`
  To string `json:"to" binding:"required"`
  Subject string `json:"subject" binding:"required"`
  Body string `json:"body,omitempty"`
}