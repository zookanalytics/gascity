
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_17 uint

const (
  AnonymousSchema_17Error AnonymousSchema_17 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_17) Value() any {
	if op >= AnonymousSchema_17(len(AnonymousSchema_17Values)) {
		return nil
	}
	return AnonymousSchema_17Values[op]
}

var AnonymousSchema_17Values = []any{"error"}
var ValuesToAnonymousSchema_17 = map[any]AnonymousSchema_17{
  AnonymousSchema_17Values[AnonymousSchema_17Error]: AnonymousSchema_17Error,
}

 
func (op *AnonymousSchema_17) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_17[v]
  return nil
}

func (op AnonymousSchema_17) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}