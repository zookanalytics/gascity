
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_50 uint

const (
  AnonymousSchema_50City AnonymousSchema_50 = iota
  AnonymousSchema_50Supervisor
)

// Value returns the value of the enum.
func (op AnonymousSchema_50) Value() any {
	if op >= AnonymousSchema_50(len(AnonymousSchema_50Values)) {
		return nil
	}
	return AnonymousSchema_50Values[op]
}

var AnonymousSchema_50Values = []any{"city","supervisor"}
var ValuesToAnonymousSchema_50 = map[any]AnonymousSchema_50{
  AnonymousSchema_50Values[AnonymousSchema_50City]: AnonymousSchema_50City,
  AnonymousSchema_50Values[AnonymousSchema_50Supervisor]: AnonymousSchema_50Supervisor,
}

 
func (op *AnonymousSchema_50) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_50[v]
  return nil
}

func (op AnonymousSchema_50) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}