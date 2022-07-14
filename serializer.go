package bond

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

type Serializer interface {
	Serialize(i interface{}) ([]byte, error)
	Deserialize(b []byte, i interface{}) error
}

type JsonSerializer struct {
}

func (s *JsonSerializer) Serialize(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

func (s *JsonSerializer) Deserialize(b []byte, i interface{}) error {
	return json.Unmarshal(b, i)
}

type MsgPackSerializer struct {
}

func (m *MsgPackSerializer) Serialize(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

func (m *MsgPackSerializer) Deserialize(b []byte, i interface{}) error {
	return msgpack.Unmarshal(b, i)
}
