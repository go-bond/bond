package serializers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/0xsequence/go-sequence/lib/prototyp"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/serializers/sample/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO: lets inspect how cbor is marshalling this.. maybe we can make it
// more efficient out of the gate..? by having it implement the encoding.BinaryMarshaler
// interface. ....??..
//
// .. then lets review prototyp.BigInt

type TokenHistory struct {
	BlockNumber     uint64        `json:"blockNumber" cbor:"1"`
	BlockHash       prototyp.Hash `json:"blockHash" cbor:"2"`
	AccountAddress  prototyp.Hash `json:"accountAddress" cbor:"3"`
	ContractAddress prototyp.Hash `json:"contractAddress" cbor:"4"`
	ContractType    uint32        `json:"contractType" cbor:"5"`
	FromAddress     prototyp.Hash `json:"fromAddress" cbor:"6"`
	ToAddress       prototyp.Hash `json:"toAddress" cbor:"7"`
	TxnHash         prototyp.Hash `json:"txnHash" cbor:"8"`
	TxnIndex        uint          `json:"txnIndex" cbor:"9"`
	TxnLogIndex     uint          `json:"txnLogIndex" cbor:"a"`
	// deprecated in favour of TokenIDs and Amounts
	LogData  string            `json:"logData" cbor:"b"`
	TokenIDs []prototyp.BigInt `json:"tokenIDs" cbor:"d"`
	Amounts  []prototyp.BigInt `json:"Amounts" cbor:"e"`
	TS       time.Time         `json:"ts" cbor:"c"`
}

// TODO: we need to add the Hash and BigInt types here ..

func TestSerializeCBOR(t *testing.T) {
	serializer := &serializers.CBORSerializer{}
	tokenHistory := &TokenHistory{
		BlockNumber:     1,
		BlockHash:       prototyp.HashFromString("0x1234567890abcdef"),
		AccountAddress:  prototyp.HashFromString("0x2"),
		ContractAddress: prototyp.HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     prototyp.HashFromString("0x5"),
		ToAddress:       prototyp.HashFromString("0x6"),
		TxnHash:         prototyp.HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIDs:        []prototyp.BigInt{prototyp.NewBigIntFromNumberString("888888888888888888888888")},
		Amounts:         []prototyp.BigInt{prototyp.NewBigIntFromNumberString("999999999999999999999999")},
		TS:              time.Now(),
	}

	serialized, err := serializer.Serialize(tokenHistory)
	if err != nil {
		t.Fatalf("failed to serialize token history: %v", err)
	}

	fmt.Println("serialized num of bytes:", len(serialized))
	fmt.Printf("serialized: %v\n", serialized)
	spew.Dump(serialized)

	// assertion to ensure that the serialized data is less than 90 bytes
	// to confirm that the cbor serialization is efficient.
	//
	// if this test fails, we need to review the cbor serialization as
	// the underlying library may have changed its behavior.
	require.True(t, len(serialized) <= 90)
}

func TestSerializeProtobuf(t *testing.T) {
	protoMarshaller := proto.MarshalOptions{}
	protoUnmarshaller := proto.UnmarshalOptions{}

	serializer := &serializers.ProtobufSerializer{
		Encoder: protoMarshaller,
		Decoder: protoUnmarshaller,
	}
	tokenHistory := &pb.TokenHistory{
		BlockNumber:     1,
		BlockHash:       []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5}, // prototyp.HashFromString("0x1"),
		AccountAddress:  []byte{2},                                           // prototyp.HashFromString("0x2"),
		ContractAddress: []byte{3},                                           // prototyp.HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     []byte{5}, // prototyp.HashFromString("0x5"),
		ToAddress:       []byte{6}, // prototyp.HashFromString("0x6"),
		TxnHash:         []byte{7}, // prototyp.HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIds:        []*pb.BigInt{{Value: prototyp.NewBigIntFromNumberString("888888888888888888888888").Bytes()}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("8")},
		Amounts:         []*pb.BigInt{{Value: prototyp.NewBigIntFromNumberString("999999999999999999999999").Bytes()}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("9")},
		Ts:              timestamppb.New(time.Now()),
	}

	serialized, err := serializer.Serialize(tokenHistory)
	if err != nil {
		t.Fatalf("failed to serialize token history: %v", err)
	}

	fmt.Println("serialized num of bytes:", len(serialized))
	fmt.Printf("serialized: %v\n", serialized)
	spew.Dump(serialized)
}

func BenchmarkSerializeCBOR(b *testing.B) {
	serializer := &serializers.CBORSerializer{}
	tokenHistory := &TokenHistory{
		BlockNumber:     1,
		BlockHash:       prototyp.HashFromString("0x1"),
		AccountAddress:  prototyp.HashFromString("0x2"),
		ContractAddress: prototyp.HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     prototyp.HashFromString("0x5"),
		ToAddress:       prototyp.HashFromString("0x6"),
		TxnHash:         prototyp.HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIDs:        []prototyp.BigInt{prototyp.NewBigIntFromNumberString("888888888888888888888888")},
		Amounts:         []prototyp.BigInt{prototyp.NewBigIntFromNumberString("999999999999999999999999")},
		TS:              time.Now(),
	}

	for i := 0; i < b.N; i++ {
		_, err := serializer.Serialize(tokenHistory)
		if err != nil {
			b.Fatalf("failed to serialize token history: %v", err)
		}
	}
}

func BenchmarkSerializeProtobuf(b *testing.B) {
	protoMarshaller := proto.MarshalOptions{}
	protoUnmarshaller := proto.UnmarshalOptions{}

	serializer := &serializers.ProtobufSerializer{
		Encoder: protoMarshaller,
		Decoder: protoUnmarshaller,
	}
	tokenHistory := &pb.TokenHistory{
		BlockNumber:     1,
		BlockHash:       []byte{1}, // prototyp.HashFromString("0x1"),
		AccountAddress:  []byte{2}, // prototyp.HashFromString("0x2"),
		ContractAddress: []byte{3}, // prototyp.HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     []byte{5}, // prototyp.HashFromString("0x5"),
		ToAddress:       []byte{6}, // prototyp.HashFromString("0x6"),
		TxnHash:         []byte{7}, // prototyp.HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIds:        []*pb.BigInt{{Value: []byte{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("8")},
		Amounts:         []*pb.BigInt{{Value: []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("9")},
		Ts:              timestamppb.New(time.Now()),
	}

	for i := 0; i < b.N; i++ {
		_, err := serializer.Serialize(tokenHistory)
		if err != nil {
			b.Fatalf("failed to serialize token history: %v", err)
		}
	}
}
