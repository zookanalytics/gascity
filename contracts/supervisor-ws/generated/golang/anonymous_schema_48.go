
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_48 uint

const (
  AnonymousSchema_48Hello AnonymousSchema_48 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_48) Value() any {
	if op >= AnonymousSchema_48(len(AnonymousSchema_48Values)) {
		return nil
	}
	return AnonymousSchema_48Values[op]
}

var AnonymousSchema_48Values = []any{"hello"}
var ValuesToAnonymousSchema_48 = map[any]AnonymousSchema_48{
  AnonymousSchema_48Values[AnonymousSchema_48Hello]: AnonymousSchema_48Hello,
}

 
func (op *AnonymousSchema_48) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_48[v]
  return nil
}

func (op AnonymousSchema_48) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}