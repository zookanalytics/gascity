
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_54 uint

const (
  AnonymousSchema_54Response AnonymousSchema_54 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_54) Value() any {
	if op >= AnonymousSchema_54(len(AnonymousSchema_54Values)) {
		return nil
	}
	return AnonymousSchema_54Values[op]
}

var AnonymousSchema_54Values = []any{"response"}
var ValuesToAnonymousSchema_54 = map[any]AnonymousSchema_54{
  AnonymousSchema_54Values[AnonymousSchema_54Response]: AnonymousSchema_54Response,
}

 
func (op *AnonymousSchema_54) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_54[v]
  return nil
}

func (op AnonymousSchema_54) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}