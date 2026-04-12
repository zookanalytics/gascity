
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_58 uint

const (
  AnonymousSchema_58Error AnonymousSchema_58 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_58) Value() any {
	if op >= AnonymousSchema_58(len(AnonymousSchema_58Values)) {
		return nil
	}
	return AnonymousSchema_58Values[op]
}

var AnonymousSchema_58Values = []any{"error"}
var ValuesToAnonymousSchema_58 = map[any]AnonymousSchema_58{
  AnonymousSchema_58Values[AnonymousSchema_58Error]: AnonymousSchema_58Error,
}

 
func (op *AnonymousSchema_58) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_58[v]
  return nil
}

func (op AnonymousSchema_58) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}