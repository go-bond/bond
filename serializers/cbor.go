package serializers

import (
	"bytes"

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

func (c *CBORSerializer) SerializeFuncWithBuffer(buff *bytes.Buffer) func(T any) ([]byte, error) {
	var userEncMode cbor.UserBufferEncMode
	if c.EncMode != nil {
		userEncMode, _ = c.EncMode.EncOptions().UserBufferEncMode()
	} else {
		userEncMode, _ = cbor.EncOptions{}.UserBufferEncMode()
	}

	return func(v any) ([]byte, error) {
		buff.Reset()
		if err := userEncMode.MarshalToBuffer(v, buff); err != nil {
			return nil, err
		}
		return buff.Bytes(), nil
	}
}
