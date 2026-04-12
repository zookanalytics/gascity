
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_46 uint

const (
  AnonymousSchema_46Raw AnonymousSchema_46 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_46) Value() any {
	if op >= AnonymousSchema_46(len(AnonymousSchema_46Values)) {
		return nil
	}
	return AnonymousSchema_46Values[op]
}

var AnonymousSchema_46Values = []any{"raw"}
var ValuesToAnonymousSchema_46 = map[any]AnonymousSchema_46{
  AnonymousSchema_46Values[AnonymousSchema_46Raw]: AnonymousSchema_46Raw,
}

 
func (op *AnonymousSchema_46) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_46[v]
  return nil
}

func (op AnonymousSchema_46) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}