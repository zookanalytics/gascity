
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_35 uint

const (
  AnonymousSchema_35Public AnonymousSchema_35 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_35) Value() any {
	if op >= AnonymousSchema_35(len(AnonymousSchema_35Values)) {
		return nil
	}
	return AnonymousSchema_35Values[op]
}

var AnonymousSchema_35Values = []any{"public"}
var ValuesToAnonymousSchema_35 = map[any]AnonymousSchema_35{
  AnonymousSchema_35Values[AnonymousSchema_35Public]: AnonymousSchema_35Public,
}

 
func (op *AnonymousSchema_35) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_35[v]
  return nil
}

func (op AnonymousSchema_35) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}