package serializers

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/go-bond/bond/utils"
	"github.com/tinylib/msgp/msgp"
)

type MsgpackGenSerializer struct {
	Buffer utils.SyncPool[bytes.Buffer]
}

func (m *MsgpackGenSerializer) Serialize(i interface{}) ([]byte, error) {
	e, ok := i.(msgp.Encodable)
	if !ok {
		if typ := reflect.TypeOf(i); typ.Kind() == reflect.Ptr {
			i := reflect.ValueOf(i).Elem().Interface()
			e, ok = i.(msgp.Encodable)
			if !ok {
				return nil, fmt.Errorf("interface does not implement msgp.Encodable")
			}
		} else {
			return nil, fmt.Errorf("interface does not implement msgp.Encodable")
		}
	}

	var buf bytes.Buffer

	err := msgp.Encode(&buf, e)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *MsgpackGenSerializer) SerializerWithCloseable(i interface{}) ([]byte, func(), error) {
	e, ok := i.(msgp.Encodable)
	if !ok {
		if typ := reflect.TypeOf(i); typ.Kind() == reflect.Ptr {
			i := reflect.ValueOf(i).Elem().Interface()
			e, ok = i.(msgp.Encodable)
			if !ok {
				return nil, nil, fmt.Errorf("interface does not implement msgp.Encodable")
			}
		} else {
			return nil, nil, fmt.Errorf("interface does not implement msgp.Encodable")
		}
	}

	buff := m.getBuffer()

	err := msgp.Encode(&buff, e)
	if err != nil {
		return nil, nil, err
	}

	closeable := func() {
		m.freeBuffer(buff)
	}

	return buff.Bytes(), closeable, nil
}

func (m *MsgpackGenSerializer) Deserialize(b []byte, i interface{}) error {
	newEntry := utils.MakeNewAny(i)
	root := utils.FindRootInterface(reflect.ValueOf(newEntry))
	d, ok := root.(msgp.Decodable)
	if !ok {
		return fmt.Errorf("interface does not implement msgp.Decodable")
	}

	err := msgp.Decode(bytes.NewBuffer(b), d)
	reflect.ValueOf(i).Elem().Set(reflect.ValueOf(newEntry).Elem())
	return err
}

func (m *MsgpackGenSerializer) getBuffer() bytes.Buffer {
	if m.Buffer != nil {
		return m.Buffer.Get()
	} else {
		return bytes.Buffer{}
	}
}

func (m *MsgpackGenSerializer) freeBuffer(buffer bytes.Buffer) {
	if m.Buffer != nil {
		m.Buffer.Put(buffer)
	}
}
