
package wscontract

type SessionTranscriptRequest struct {
  Id string `json:"id" binding:"required"`
  Tail int `json:"tail,omitempty"`
  Before string `json:"before,omitempty"`
  Format *AnonymousSchema_42 `json:"format,omitempty"`
}