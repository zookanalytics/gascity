
package wscontract

type IdRequest struct {
  Id string `json:"id" binding:"required"`
}