
package wscontract

type SessionSubmitRequest struct {
  Id string `json:"id" binding:"required"`
  Message string `json:"message" binding:"required"`
  Intent string `json:"intent,omitempty"`
}