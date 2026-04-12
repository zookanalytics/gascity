
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_65 uint

const (
  AnonymousSchema_65Event AnonymousSchema_65 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_65) Value() any {
	if op >= AnonymousSchema_65(len(AnonymousSchema_65Values)) {
		return nil
	}
	return AnonymousSchema_65Values[op]
}

var AnonymousSchema_65Values = []any{"event"}
var ValuesToAnonymousSchema_65 = map[any]AnonymousSchema_65{
  AnonymousSchema_65Values[AnonymousSchema_65Event]: AnonymousSchema_65Event,
}

 
func (op *AnonymousSchema_65) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_65[v]
  return nil
}

func (op AnonymousSchema_65) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}