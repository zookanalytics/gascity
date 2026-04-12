
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_42 uint

const (
  AnonymousSchema_42Raw AnonymousSchema_42 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_42) Value() any {
	if op >= AnonymousSchema_42(len(AnonymousSchema_42Values)) {
		return nil
	}
	return AnonymousSchema_42Values[op]
}

var AnonymousSchema_42Values = []any{"raw"}
var ValuesToAnonymousSchema_42 = map[any]AnonymousSchema_42{
  AnonymousSchema_42Values[AnonymousSchema_42Raw]: AnonymousSchema_42Raw,
}

 
func (op *AnonymousSchema_42) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_42[v]
  return nil
}

func (op AnonymousSchema_42) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}