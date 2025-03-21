package serializers_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fxamacker/cbor/v2"
	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/utils"
	"github.com/stretchr/testify/require"
)

type TokenTransfer struct {
	BlockNumber     uint64    `json:"blockNumber" cbor:"1"`
	BlockHash       Hash      `json:"blockHash" cbor:"2"`
	AccountAddress  Hash      `json:"accountAddress" cbor:"3"`
	ContractAddress Hash      `json:"contractAddress" cbor:"4"`
	ContractType    uint32    `json:"contractType" cbor:"5"`
	FromAddress     Hash      `json:"fromAddress" cbor:"6"`
	ToAddress       Hash      `json:"toAddress" cbor:"7"`
	TxnHash         Hash      `json:"txnHash" cbor:"8"`
	TxnIndex        uint      `json:"txnIndex" cbor:"9"`
	TxnLogIndex     uint      `json:"txnLogIndex" cbor:"a"`
	LogData         string    `json:"logData" cbor:"b"`
	TokenIDs        []BigInt  `json:"tokenIDs" cbor:"c"`
	Amounts         []BigInt  `json:"Amounts" cbor:"d"`
	TS              time.Time `json:"ts" cbor:"e"`
}

func TestCBOR(t *testing.T) {
	// encMode, err := cbor.EncOptions{}.EncMode()
	// require.NoError(t, err)
	// decMode, err := cbor.DecOptions{}.DecMode()
	// require.NoError(t, err)

	userEncMode, err := cbor.EncOptions{}.UserBufferEncMode()
	require.NoError(t, err)

	serializer := &serializers.CBORSerializer{
		// EncMode: encMode,
		// DecMode: decMode,
		UserEncMode: userEncMode,
		Buffer: &utils.SyncPoolWrapper[*bytes.Buffer]{
			Pool: sync.Pool{New: func() interface{} { return &bytes.Buffer{} }},
		},
	}
	tokenHistory := &TokenTransfer{
		BlockNumber:     1,
		BlockHash:       HashFromString("0x1234567890abcdef"),
		AccountAddress:  HashFromString("0x2"),
		ContractAddress: HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     HashFromString("0x5"),
		ToAddress:       HashFromString("0x6"),
		TxnHash:         HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIDs:        []BigInt{NewBigIntFromNumberString("888888888888888888888888")},
		Amounts:         []BigInt{NewBigIntFromNumberString("999999999999999999999999")},
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
	require.True(t, len(serialized) <= 91)
}

func BenchmarkCBOR(b *testing.B) {
	// serializer := &serializers.CBORSerializer{}

	userEncMode, err := cbor.EncOptions{
		FieldName: cbor.FieldNameToByteString,
	}.UserBufferEncMode()
	_ = err

	serializer := &serializers.CBORSerializer{
		UserEncMode: userEncMode,
		Buffer: &utils.SyncPoolWrapper[*bytes.Buffer]{
			Pool: sync.Pool{New: func() interface{} { return &bytes.Buffer{} }},
		},
	}

	tokenHistory := &TokenTransfer{
		BlockNumber:     1,
		BlockHash:       HashFromString("0x1"),
		AccountAddress:  HashFromString("0x2"),
		ContractAddress: HashFromString("0x3"),
		ContractType:    0x4,
		FromAddress:     HashFromString("0x5"),
		ToAddress:       HashFromString("0x6"),
		TxnHash:         HashFromString("0x7"),
		TxnIndex:        1,
		TxnLogIndex:     2,
		LogData:         "logData",
		TokenIDs:        []BigInt{NewBigIntFromNumberString("888888888888888888888888")},
		Amounts:         []BigInt{NewBigIntFromNumberString("999999999999999999999999")},
		TS:              time.Now(),
	}

	for i := 0; i < b.N; i++ {
		_, err := serializer.Serialize(tokenHistory)
		if err != nil {
			b.Fatalf("failed to serialize token history: %v", err)
		}
	}
}
