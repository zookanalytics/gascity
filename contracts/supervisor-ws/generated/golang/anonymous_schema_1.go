
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_1 uint

const (
  AnonymousSchema_1Request AnonymousSchema_1 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_1) Value() any {
	if op >= AnonymousSchema_1(len(AnonymousSchema_1Values)) {
		return nil
	}
	return AnonymousSchema_1Values[op]
}

var AnonymousSchema_1Values = []any{"request"}
var ValuesToAnonymousSchema_1 = map[any]AnonymousSchema_1{
  AnonymousSchema_1Values[AnonymousSchema_1Request]: AnonymousSchema_1Request,
}

 
func (op *AnonymousSchema_1) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_1[v]
  return nil
}

func (op AnonymousSchema_1) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}