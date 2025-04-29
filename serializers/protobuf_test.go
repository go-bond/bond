package serializers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/serializers/sample/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODOXXX: clean up this file..

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
		TokenIds:        []*pb.BigInt{{Value: NewBigIntFromNumberString("888888888888888888888888").Bytes()}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("8")},
		Amounts:         []*pb.BigInt{{Value: NewBigIntFromNumberString("999999999999999999999999").Bytes()}}, //[]prototyp.BigInt{prototyp.NewBigIntFromNumberString("9")},
		Ts:              timestamppb.New(time.Now()),
	}

	serialized, err := serializer.Serialize(tokenHistory)
	if err != nil {
		t.Fatalf("failed to serialize token history: %v", err)
	}

	fmt.Println("serialized num of bytes:", len(serialized))
	fmt.Printf("serialized: %v\n", serialized)
	// spew.Dump(serialized)
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
