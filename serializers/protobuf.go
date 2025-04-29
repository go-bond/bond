package serializers

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtobufSerializer with support for vtproto (code-gen'd) protobuf,
// and standard protobuf (google.golang.org/protobuf/proto).
//
// To use standard protobuf, set the Encoder and Decoder to
// proto.Marshal and proto.Unmarshal respectively. You can
// leave them unset if you use vtproto.
type ProtobufSerializer struct {
	Encoder ProtobufMarshaler
	Decoder ProtobufUnmarshaler
	SkipVT  bool
}

type ProtobufVTMmarshaler interface {
	MarshalVT() ([]byte, error)
}

type ProtobufVTUnmarshaler interface {
	UnmarshalVT(b []byte) error
}

type ProtobufMarshaler interface {
	Marshal(m proto.Message) ([]byte, error)
}

type ProtobufUnmarshaler interface {
	Unmarshal(b []byte, m proto.Message) error
}

func (s *ProtobufSerializer) Serialize(i interface{}) ([]byte, error) {
	// check if protobuf type uses vtproto, which is code-generated
	// marshaler which is faster than the default protobuf marshaler.
	if !s.SkipVT {
		if v, ok := i.(ProtobufVTMmarshaler); ok {
			return v.MarshalVT()
		}
	}

	// check if default marshaller is set, via google.golang.org/protobuf/proto
	if s.Encoder != nil {
		if v, ok := i.(proto.Message); ok {
			return s.Encoder.Marshal(v)
		}
	}

	return nil, fmt.Errorf("error: %T does not implement protobuf marshaler", i)
}

func (s *ProtobufSerializer) Deserialize(b []byte, i interface{}) error {
	// check if protobuf type uses vtproto, which is code-generated
	// unmarshaler which is faster than the default protobuf unmarshaler.
	if !s.SkipVT {
		if v, ok := i.(ProtobufVTUnmarshaler); ok {
			return v.UnmarshalVT(b)
		}
	}

	// check if default unmarshaller is set, via google.golang.org/protobuf/proto
	if s.Decoder != nil {
		if v, ok := i.(proto.Message); ok {
			return s.Decoder.Unmarshal(b, v)
		}
	}

	return fmt.Errorf("error: %T does not implement protobuf unmarshaler", i)
}
