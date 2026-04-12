
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_24 uint

const (
  AnonymousSchema_24Event AnonymousSchema_24 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_24) Value() any {
	if op >= AnonymousSchema_24(len(AnonymousSchema_24Values)) {
		return nil
	}
	return AnonymousSchema_24Values[op]
}

var AnonymousSchema_24Values = []any{"event"}
var ValuesToAnonymousSchema_24 = map[any]AnonymousSchema_24{
  AnonymousSchema_24Values[AnonymousSchema_24Event]: AnonymousSchema_24Event,
}

 
func (op *AnonymousSchema_24) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_24[v]
  return nil
}

func (op AnonymousSchema_24) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}