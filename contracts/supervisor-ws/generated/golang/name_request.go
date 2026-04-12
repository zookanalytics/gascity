
package wscontract

type NameRequest struct {
  Name string `json:"name" binding:"required"`
}