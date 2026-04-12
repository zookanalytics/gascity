
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_8 uint

const (
  AnonymousSchema_8GcDotV1alpha1 AnonymousSchema_8 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_8) Value() any {
	if op >= AnonymousSchema_8(len(AnonymousSchema_8Values)) {
		return nil
	}
	return AnonymousSchema_8Values[op]
}

var AnonymousSchema_8Values = []any{"gc.v1alpha1"}
var ValuesToAnonymousSchema_8 = map[any]AnonymousSchema_8{
  AnonymousSchema_8Values[AnonymousSchema_8GcDotV1alpha1]: AnonymousSchema_8GcDotV1alpha1,
}

 
func (op *AnonymousSchema_8) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_8[v]
  return nil
}

func (op AnonymousSchema_8) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}