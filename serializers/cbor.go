package serializers

import (
	"github.com/fxamacker/cbor/v2"
)

type CBORSerializer struct {
}

func (c *CBORSerializer) Serialize(i interface{}) ([]byte, error) {
	return cbor.Marshal(i)
}

func (c *CBORSerializer) Deserialize(b []byte, i interface{}) error {
	return cbor.Unmarshal(b, i)
}
