package serializers

import "fmt"

// ProtobufSerializer with support for vtproto (code-gen'd) protobuf,
// and standard protobuf (github.com/golang/protobuf/proto).
//
// To use standard protobuf, set the Encoder and Decoder to
// proto.Marshal and proto.Unmarshal respectively. You can
// leave them unset if you use vtproto.
type ProtobufSerializer struct {
	Encoder ProtobufMarshaler
	Decoder ProtobufUnmarshaler
}

type ProtobufVTMmarshaler interface {
	MarshalVT() ([]byte, error)
}

type ProtobufVTUnmarshaler interface {
	UnmarshalVT(b []byte) error
}

type ProtobufMarshaler interface {
	Marshal(m ProtobufMessageV1) ([]byte, error)
}

type ProtobufUnmarshaler interface {
	Unmarshal(b []byte, m ProtobufMessageV1) error
}

type ProtobufMessageV1 interface {
	Reset()
	String() string
	ProtoMessage()
}

func (s *ProtobufSerializer) Serialize(i interface{}) ([]byte, error) {
	// check if protobuf type uses vtproto, which is code-generated
	// marshaler which is faster than the default protobuf marshaler.
	if v, ok := i.(ProtobufVTMmarshaler); ok {
		return v.MarshalVT()
	}

	// check if default marshaller is set, via github.com/golang/protobuf/proto
	if s.Encoder != nil {
		if v, ok := i.(ProtobufMessageV1); ok {
			return s.Encoder.Marshal(v)
		}
	}

	return nil, fmt.Errorf("error: %T does not implement protobuf marshaler", i)
}

func (s *ProtobufSerializer) Deserialize(b []byte, i interface{}) error {
	// check if protobuf type uses vtproto, which is code-generated
	// unmarshaler which is faster than the default protobuf unmarshaler.
	if v, ok := i.(ProtobufVTUnmarshaler); ok {
		return v.UnmarshalVT(b)
	}

	// check if default unmarshaller is set, via github.com/golang/protobuf/proto
	if s.Decoder != nil {
		if v, ok := i.(ProtobufMessageV1); ok {
			return s.Decoder.Unmarshal(b, v)
		}
	}

	return fmt.Errorf("error: %T does not implement protobuf unmarshaler", i)
}
