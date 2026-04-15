
package wscontract

type ApiMutationStatusIdResponse struct {
  Id string `json:"id,omitempty"`
  Status string `json:"status,omitempty"`
  AdditionalProperties map[string]interface{} `json:"-,omitempty"`
}