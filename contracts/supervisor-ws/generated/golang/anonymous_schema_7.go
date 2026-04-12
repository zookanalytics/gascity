
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_7 uint

const (
  AnonymousSchema_7Hello AnonymousSchema_7 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_7) Value() any {
	if op >= AnonymousSchema_7(len(AnonymousSchema_7Values)) {
		return nil
	}
	return AnonymousSchema_7Values[op]
}

var AnonymousSchema_7Values = []any{"hello"}
var ValuesToAnonymousSchema_7 = map[any]AnonymousSchema_7{
  AnonymousSchema_7Values[AnonymousSchema_7Hello]: AnonymousSchema_7Hello,
}

 
func (op *AnonymousSchema_7) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_7[v]
  return nil
}

func (op AnonymousSchema_7) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}