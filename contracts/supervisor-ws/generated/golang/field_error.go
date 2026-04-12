
package wscontract

type FieldError struct {
  Field string `json:"field" binding:"required"`
  Message string `json:"message" binding:"required"`
}