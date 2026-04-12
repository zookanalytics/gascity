
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_43 uint

const (
  AnonymousSchema_43Events AnonymousSchema_43 = iota
  AnonymousSchema_43SessionDotStream
)

// Value returns the value of the enum.
func (op AnonymousSchema_43) Value() any {
	if op >= AnonymousSchema_43(len(AnonymousSchema_43Values)) {
		return nil
	}
	return AnonymousSchema_43Values[op]
}

var AnonymousSchema_43Values = []any{"events","session.stream"}
var ValuesToAnonymousSchema_43 = map[any]AnonymousSchema_43{
  AnonymousSchema_43Values[AnonymousSchema_43Events]: AnonymousSchema_43Events,
  AnonymousSchema_43Values[AnonymousSchema_43SessionDotStream]: AnonymousSchema_43SessionDotStream,
}

 
func (op *AnonymousSchema_43) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_43[v]
  return nil
}

func (op AnonymousSchema_43) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}