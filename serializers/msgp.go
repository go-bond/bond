package serializers

import (
	"bytes"

	"github.com/go-bond/bond/utils"
	"github.com/vmihailenco/msgpack/v5"
)

type MsgpackSerializer struct {
	Encoder utils.SyncPool[*msgpack.Encoder]
	Decoder utils.SyncPool[*msgpack.Decoder]
	Buffer  utils.SyncPool[bytes.Buffer]
}

func (m *MsgpackSerializer) Serialize(i interface{}) ([]byte, error) {
	if m.Encoder != nil {
		var (
			enc  = m.Encoder.Get()
			buff = m.getBuffer()
		)

		enc.Reset(&buff)

		err := enc.Encode(i)
		if err != nil {
			return nil, err
		}

		m.Encoder.Put(enc)

		return buff.Bytes(), nil
	}
	return msgpack.Marshal(i)
}

func (m *MsgpackSerializer) SerializerWithCloseable(i interface{}) ([]byte, func(), error) {
	if m.Encoder != nil {
		var (
			enc  = m.Encoder.Get()
			buff = m.getBuffer()
		)

		enc.Reset(&buff)

		err := enc.Encode(i)
		if err != nil {
			return nil, nil, err
		}

		m.Encoder.Put(enc)

		closeable := func() {
			m.freeBuffer(buff)
		}

		return buff.Bytes(), closeable, nil
	}

	b, err := msgpack.Marshal(i)
	return b, func() {}, err
}

func (m *MsgpackSerializer) Deserialize(b []byte, i interface{}) error {
	return msgpack.Unmarshal(b, i)
}

func (m *MsgpackSerializer) getBuffer() bytes.Buffer {
	if m.Buffer != nil {
		return m.Buffer.Get()
	} else {
		return bytes.Buffer{}
	}
}

func (m *MsgpackSerializer) freeBuffer(buffer bytes.Buffer) {
	if m.Buffer != nil {
		m.Buffer.Put(buffer)
	}
}
