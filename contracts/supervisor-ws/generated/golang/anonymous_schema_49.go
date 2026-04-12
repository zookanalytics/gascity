
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_49 uint

const (
  AnonymousSchema_49GcDotV1alpha1 AnonymousSchema_49 = iota
)

// Value returns the value of the enum.
func (op AnonymousSchema_49) Value() any {
	if op >= AnonymousSchema_49(len(AnonymousSchema_49Values)) {
		return nil
	}
	return AnonymousSchema_49Values[op]
}

var AnonymousSchema_49Values = []any{"gc.v1alpha1"}
var ValuesToAnonymousSchema_49 = map[any]AnonymousSchema_49{
  AnonymousSchema_49Values[AnonymousSchema_49GcDotV1alpha1]: AnonymousSchema_49GcDotV1alpha1,
}

 
func (op *AnonymousSchema_49) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_49[v]
  return nil
}

func (op AnonymousSchema_49) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}