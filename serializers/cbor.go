package serializers

import (
	"github.com/fxamacker/cbor/v2"
)

type CBORSerializer struct {
	EncMode cbor.EncMode
	DecMode cbor.DecMode
}

func (c *CBORSerializer) Serialize(i interface{}) ([]byte, error) {
	if c.EncMode != nil {
		return c.EncMode.Marshal(i)
	}
	return cbor.Marshal(i)
}

func (c *CBORSerializer) Deserialize(b []byte, i interface{}) error {
	if c.DecMode != nil {
		return c.DecMode.Unmarshal(b, i)
	}
	return cbor.Unmarshal(b, i)
}
