package serializers_test

// TODOXXX: delete..

// TODO: lets inspect how cbor is marshalling this.. maybe we can make it
// more efficient out of the gate..? by having it implement the encoding.BinaryMarshaler
// interface. ....??..
//
// .. then lets review prototyp.BigInt

// type TokenHistory struct {
// 	BlockNumber     uint64            `json:"blockNumber" cbor:"1"`
// 	BlockHash       prototyp.Hash     `json:"blockHash" cbor:"2"`
// 	AccountAddress  prototyp.Hash     `json:"accountAddress" cbor:"3"`
// 	ContractAddress prototyp.Hash     `json:"contractAddress" cbor:"4"`
// 	ContractType    uint32            `json:"contractType" cbor:"5"`
// 	FromAddress     prototyp.Hash     `json:"fromAddress" cbor:"6"`
// 	ToAddress       prototyp.Hash     `json:"toAddress" cbor:"7"`
// 	TxnHash         prototyp.Hash     `json:"txnHash" cbor:"8"`
// 	TxnIndex        uint              `json:"txnIndex" cbor:"9"`
// 	TxnLogIndex     uint              `json:"txnLogIndex" cbor:"a"`
// 	LogData         string            `json:"logData" cbor:"b"`
// 	TokenIDs        []prototyp.BigInt `json:"tokenIDs" cbor:"c"`
// 	Amounts         []prototyp.BigInt `json:"Amounts" cbor:"d"`
// 	TS              time.Time         `json:"ts" cbor:"e"`
// }

// TODO: we need to add the Hash and BigInt types here ..

// func TestSerializeCBOR(t *testing.T) {
// 	serializer := &serializers.CBORSerializer{}
// 	tokenHistory := &TokenHistory{
// 		BlockNumber:     1,
// 		BlockHash:       prototyp.HashFromString("0x1234567890abcdef"),
// 		AccountAddress:  prototyp.HashFromString("0x2"),
// 		ContractAddress: prototyp.HashFromString("0x3"),
// 		ContractType:    0x4,
// 		FromAddress:     prototyp.HashFromString("0x5"),
// 		ToAddress:       prototyp.HashFromString("0x6"),
// 		TxnHash:         prototyp.HashFromString("0x7"),
// 		TxnIndex:        1,
// 		TxnLogIndex:     2,
// 		LogData:         "logData",
// 		TokenIDs:        []prototyp.BigInt{prototyp.NewBigIntFromNumberString("888888888888888888888888")},
// 		Amounts:         []prototyp.BigInt{prototyp.NewBigIntFromNumberString("999999999999999999999999")},
// 		TS:              time.Now(),
// 	}

// 	serialized, err := serializer.Serialize(tokenHistory)
// 	if err != nil {
// 		t.Fatalf("failed to serialize token history: %v", err)
// 	}

// 	fmt.Println("serialized num of bytes:", len(serialized))
// 	fmt.Printf("serialized: %v\n", serialized)
// 	spew.Dump(serialized)

// 	// assertion to ensure that the serialized data is less than 90 bytes
// 	// to confirm that the cbor serialization is efficient.
// 	//
// 	// if this test fails, we need to review the cbor serialization as
// 	// the underlying library may have changed its behavior.
// 	require.True(t, len(serialized) <= 91)
// }

// func BenchmarkSerializeCBOR(b *testing.B) {
// 	serializer := &serializers.CBORSerializer{}
// 	tokenHistory := &TokenHistory{
// 		BlockNumber:     1,
// 		BlockHash:       prototyp.HashFromString("0x1"),
// 		AccountAddress:  prototyp.HashFromString("0x2"),
// 		ContractAddress: prototyp.HashFromString("0x3"),
// 		ContractType:    0x4,
// 		FromAddress:     prototyp.HashFromString("0x5"),
// 		ToAddress:       prototyp.HashFromString("0x6"),
// 		TxnHash:         prototyp.HashFromString("0x7"),
// 		TxnIndex:        1,
// 		TxnLogIndex:     2,
// 		LogData:         "logData",
// 		TokenIDs:        []prototyp.BigInt{prototyp.NewBigIntFromNumberString("888888888888888888888888")},
// 		Amounts:         []prototyp.BigInt{prototyp.NewBigIntFromNumberString("999999999999999999999999")},
// 		TS:              time.Now(),
// 	}

// 	for i := 0; i < b.N; i++ {
// 		_, err := serializer.Serialize(tokenHistory)
// 		if err != nil {
// 			b.Fatalf("failed to serialize token history: %v", err)
// 		}
// 	}
// }
