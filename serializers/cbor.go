package serializers

import (
	"bytes"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-bond/bond/utils"
)

type CBORSerializer struct {
	EncMode cbor.EncMode
	DecMode cbor.DecMode

	// -- or ..

	// NOTE: performance here is pretty much the same..
	UserEncMode cbor.UserBufferEncMode
	Buffer      utils.SyncPool[*bytes.Buffer]
}

func (c *CBORSerializer) Serialize(i interface{}) ([]byte, error) {
	if c.UserEncMode != nil {
		buff := c.Buffer.Get()
		buff.Reset()

		err := c.UserEncMode.MarshalToBuffer(i, buff)
		if err != nil {
			c.Buffer.Put(buff)
			return nil, err
		}

		out := make([]byte, buff.Len())
		copy(out, buff.Bytes())
		c.Buffer.Put(buff)

		return out, nil
	} else {
		if c.EncMode != nil {
			return c.EncMode.Marshal(i)
		}
		return cbor.Marshal(i)
	}
	// return buff.Bytes(), nil

	// if c.EncMode != nil {
	// 	return c.EncMode.Marshal(i)
	// }
	// return cbor.Marshal(i)

	// return nil, nil
}

func (c *CBORSerializer) Deserialize(b []byte, i interface{}) error {
	if c.DecMode != nil {
		return c.DecMode.Unmarshal(b, i)
	}
	return cbor.Unmarshal(b, i)
}

func (c *CBORSerializer) SerializeFuncWithBuffer(buff *bytes.Buffer) func(T any) ([]byte, error) {
	// var encoder *cbor.Encoder
	// if c.EncMode != nil {
	// 	encoder = c.EncMode.NewEncoder(buff)
	// } else {
	// 	encoder = cbor.NewEncoder(buff)
	// }

	// not great, cuz we lose options, etc.
	// probably could just get EncOpiotns and DecOptions in the struct in the first place..
	userEncMode, _ := cbor.EncOptions{}.UserBufferEncMode()

	return func(v any) ([]byte, error) {
		buff.Reset()
		// if err := encoder.Encode(v); err != nil {
		// 	return nil, err
		// }
		// return buff.Bytes(), nil

		if err := userEncMode.MarshalToBuffer(v, buff); err != nil {
			return nil, err
		}
		return buff.Bytes(), nil
	}
}
