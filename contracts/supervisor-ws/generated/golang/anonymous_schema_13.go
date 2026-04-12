
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_13 uint

const (
  AnonymousSchema_13Response AnonymousSchema_13 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_13) Value() any {
	if op >= AnonymousSchema_13(len(AnonymousSchema_13Values)) {
		return nil
	}
	return AnonymousSchema_13Values[op]
}

var AnonymousSchema_13Values = []any{"response"}
var ValuesToAnonymousSchema_13 = map[any]AnonymousSchema_13{
  AnonymousSchema_13Values[AnonymousSchema_13Response]: AnonymousSchema_13Response,
}

 
func (op *AnonymousSchema_13) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_13[v]
  return nil
}

func (op AnonymousSchema_13) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}