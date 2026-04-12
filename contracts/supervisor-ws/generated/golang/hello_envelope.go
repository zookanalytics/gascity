
package wscontract

type HelloEnvelope struct {
  ReservedType *AnonymousSchema_48 `json:"type" binding:"required"`
  Protocol *AnonymousSchema_49 `json:"protocol" binding:"required"`
  ServerRole *AnonymousSchema_50 `json:"server_role" binding:"required"`
  ReadOnly bool `json:"read_only" binding:"required"`
  Capabilities []string `json:"capabilities" binding:"required"`
}