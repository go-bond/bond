package bond

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type TestStructure struct {
	ID          int
	Name        string
	Description string
}

func TestIndexOrder_Single_Int64_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int32_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int16_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int64_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt64(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int32_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt32(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int16_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderInt16(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint64_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint32_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint16_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint64_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint64(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint32_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint32(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint16_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderUint16(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_BigInt256_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_BigInt256_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Multi(t *testing.T) {
	indexOrderKey1 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(50, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.
		OrderUint64(1, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(90, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{KeyBuilder: NewKeyBuilder([]byte{})}.
		OrderUint64(1, IndexOrderTypeASC).
		OrderUint64(100000000, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey1,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestBond_NewIndex(t *testing.T) {
	const (
		TokenBalanceAccountIDIndexID = IndexID(1)
	)

	TokenBalanceAccountIDIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   TokenBalanceAccountIDIndexID,
		IndexName: "account_id_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		IndexOrderFunc: IndexOrderDefault[*TokenBalance],
	})

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIDIndex.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 2}))

	TokenBalanceAccountIndexSelective := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   TokenBalanceAccountIDIndexID,
		IndexName: "account_id_1_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		IndexFilterFunc: func(tb *TokenBalance) bool {
			return tb.AccountID == 1
		},
	})

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIndexSelective.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, false, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 2}))
}

func TestIndex_IndexKeyFunction(t *testing.T) {
	NameIndexID := IndexID(1)
	NameIndex := NewIndex[*TestStructure](IndexOptions[*TestStructure]{
		IndexID:   NameIndexID,
		IndexName: "name_idx",
		IndexKeyFunc: func(builder KeyBuilder, ts *TestStructure) []byte {
			return builder.AddStringField(ts.Name).Bytes()
		},
		IndexOrderFunc: IndexOrderDefault[*TestStructure],
	})

	testStructure := &TestStructure{1, "test", "test desc"}

	assert.Equal(t, NameIndexID, NameIndex.IndexID)
	assert.Equal(t, []byte{0x01, 't', 'e', 's', 't'}, NameIndex.IndexKeyFunction(NewKeyBuilder([]byte{}), testStructure))
	assert.Equal(t, []byte{}, NameIndex.IndexOrderFunction(IndexOrder{NewKeyBuilder([]byte{})}, testStructure).Bytes())
}

func TestIndex_Iter(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalancePrimaryIndex        = tokenBalanceTable.PrimaryIndex()
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	// Insert test data
	testTokenBalances := []*TokenBalance{
		{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
		{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
		{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
		{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
		{5, 5, "0xtestContract", "0xtestAccount2", 500, 500},
		{6, 6, "0xtestContract", "0xtestAccount2", 600, 600},
		{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
		{8, 8, "0xtestContract", "0xtestAccount3", 800, 800},
		{9, 9, "0xtestContract", "0xtestAccount3", 900, 900},
		{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
	}

	err = tokenBalanceTable.Insert(context.Background(), testTokenBalances)
	require.NoError(t, err)

	testInputs := []struct {
		Name     string
		Index    *Index[*TokenBalance]
		Selector Selector[*TokenBalance]
		Expected []*TokenBalance
	}{
		// Primary index test starting from ID 8
		{
			Name:     "TokenBalancePrimaryIndex_SelectorPoint",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorPoint(&TokenBalance{ID: 8}),
			Expected: []*TokenBalance{
				{8, 8, "0xtestContract", "0xtestAccount3", 800, 800},
				{9, 9, "0xtestContract", "0xtestAccount3", 900, 900},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		// Primary index test with empty selector
		{
			Name:     "TokenBalancePrimaryIndex_SelectorPoint_Empty",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorPoint(&TokenBalance{}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
				{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
				{5, 5, "0xtestContract", "0xtestAccount2", 500, 500},
				{6, 6, "0xtestContract", "0xtestAccount2", 600, 600},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{8, 8, "0xtestContract", "0xtestAccount3", 800, 800},
				{9, 9, "0xtestContract", "0xtestAccount3", 900, 900},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorPoints",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorPoints(&TokenBalance{ID: 1}, &TokenBalance{ID: 7}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorPoints_Not_Exist",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorPoints(&TokenBalance{ID: 11}, &TokenBalance{ID: 15}),
			Expected: []*TokenBalance{},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorPoints_ID_7_And_11",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorPoints(&TokenBalance{ID: 7}, &TokenBalance{ID: 11}),
			Expected: []*TokenBalance{
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorRange",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorRange(&TokenBalance{ID: 0}, &TokenBalance{ID: math.MaxUint64}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
				{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
				{5, 5, "0xtestContract", "0xtestAccount2", 500, 500},
				{6, 6, "0xtestContract", "0xtestAccount2", 600, 600},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{8, 8, "0xtestContract", "0xtestAccount3", 800, 800},
				{9, 9, "0xtestContract", "0xtestAccount3", 900, 900},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorRange_Low_Up_Bound_Same",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorRange(&TokenBalance{ID: 1}, &TokenBalance{ID: 1}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorRange_ID_0_2",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorRange(&TokenBalance{ID: 0}, &TokenBalance{ID: 2}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorRange_ID_2_7",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorRange(&TokenBalance{ID: 2}, &TokenBalance{ID: 7}),
			Expected: []*TokenBalance{
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
				{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
				{5, 5, "0xtestContract", "0xtestAccount2", 500, 500},
				{6, 6, "0xtestContract", "0xtestAccount2", 600, 600},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
			},
		},
		{
			Name:     "TokenBalancePrimaryIndex_SelectorRange_Not_Exist",
			Index:    TokenBalancePrimaryIndex,
			Selector: NewSelectorRange(&TokenBalance{ID: 11}, &TokenBalance{ID: 12}),
			Expected: []*TokenBalance{},
		},
		{
			Name:  "TokenBalancePrimaryIndex_SelectorRanges_ID_0_1_And_10_Max",
			Index: TokenBalancePrimaryIndex,
			Selector: NewSelectorRanges(
				[]*TokenBalance{
					{ID: 0}, {ID: 1},
				},
				[]*TokenBalance{
					{ID: 10}, {ID: math.MaxUint64},
				},
			),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:  "TokenBalancePrimaryIndex_SelectorRanges_ID_0_1_And_7_7_And_10_Max",
			Index: TokenBalancePrimaryIndex,
			Selector: NewSelectorRanges(
				[]*TokenBalance{
					{ID: 0}, {ID: 1},
				},
				[]*TokenBalance{
					{ID: 7}, {ID: 7},
				},
				[]*TokenBalance{
					{ID: 10}, {ID: math.MaxUint64},
				},
			),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorPoint",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorPoints",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorPoints(&TokenBalance{AccountAddress: "0xtestAccount"}, &TokenBalance{AccountAddress: "0xtestAccount1"}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
				{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
				{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorRange",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRange(&TokenBalance{AccountAddress: "0xtestAccount", ID: 0}, &TokenBalance{AccountAddress: "0xtestAccount1", ID: math.MaxUint64}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
				{3, 3, "0xtestContract", "0xtestAccount1", 300, 300},
				{4, 4, "0xtestContract", "0xtestAccount1", 400, 400},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorRange_Low_Up_Bound_Same",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRange(&TokenBalance{AccountAddress: "0xtestAccount", ID: 0}, &TokenBalance{AccountAddress: "0xtestAccount", ID: math.MaxUint64}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorRange_ID_0_2",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRange(&TokenBalance{AccountAddress: "0xtestAccount", ID: 0}, &TokenBalance{AccountAddress: "0xtestAccount", ID: 2}),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorRange_ID_2_7",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRange(&TokenBalance{AccountAddress: "0xtestAccount", ID: 2}, &TokenBalance{AccountAddress: "0xtestAccount", ID: 7}),
			Expected: []*TokenBalance{
				{2, 2, "0xtestContract", "0xtestAccount", 200, 200},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
			},
		},
		{
			Name:     "TokenBalanceAccountAddressIndex_SelectorRange_Not_Exist",
			Index:    TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRange(&TokenBalance{AccountAddress: "0xtestAccount", ID: 3}, &TokenBalance{AccountAddress: "0xtestAccount", ID: 6}),
			Expected: []*TokenBalance{},
		},
		{
			Name:  "TokenBalanceAccountAddressIndex_SelectorRanges_ID_0_1_And_10_Max",
			Index: TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRanges(
				[]*TokenBalance{
					{AccountAddress: "0xtestAccount", ID: 0}, {AccountAddress: "0xtestAccount", ID: 1},
				},
				[]*TokenBalance{
					{AccountAddress: "0xtestAccount", ID: 10}, {AccountAddress: "0xtestAccount", ID: math.MaxUint64},
				},
			),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
		{
			Name:  "TokenBalanceAccountAddressIndex_SelectorRanges_ID_0_1_And_7_7_And_10_Max",
			Index: TokenBalanceAccountAddressIndex,
			Selector: NewSelectorRanges(
				[]*TokenBalance{
					{AccountAddress: "0xtestAccount", ID: 0}, {AccountAddress: "0xtestAccount", ID: 1},
				},
				[]*TokenBalance{
					{AccountAddress: "0xtestAccount", ID: 7}, {AccountAddress: "0xtestAccount", ID: 7},
				},
				[]*TokenBalance{
					{AccountAddress: "0xtestAccount", ID: 10}, {AccountAddress: "0xtestAccount", ID: math.MaxUint64},
				},
			),
			Expected: []*TokenBalance{
				{1, 1, "0xtestContract", "0xtestAccount", 100, 100},
				{7, 7, "0xtestContract1", "0xtestAccount", 700, 700},
				{10, 10, "0xtestContract2", "0xtestAccount", 1000, 1000},
			},
		},
	}

	for _, testInput := range testInputs {
		t.Run(testInput.Name, func(t *testing.T) {
			iter := testInput.Index.Iter(
				tokenBalanceTable,
				testInput.Selector)

			var actual []*TokenBalance
			for iter.First(); iter.Valid(); iter.Next() {
				key := KeyBytes(iter.Key())
				if key.IsIndexKey() {
					key = key.ToDataKeyBytes([]byte{})
				}

				data, closer, err := db.Get(key)
				if err != nil {
					continue
				}

				var tb *TokenBalance
				err = tokenBalanceTable.Serializer().Deserialize(data, &tb)
				if err != nil {
					closer.Close()
					continue
				}
				closer.Close()

				actual = append(actual, tb)
			}
			iter.Close()

			require.Equal(t, len(testInput.Expected), len(actual))
			for i := 0; i < len(testInput.Expected); i++ {
				assert.Equal(t, testInput.Expected[i], actual[i])
			}
		})
	}
}

func TestIndex_Callbacks(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	var (
		idx1 = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   1,
			IndexName: "test",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		idx2Ordered = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   2,
			IndexName: "test",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, t *TokenBalance) IndexOrder {
				return o.OrderUint64(t.Balance, IndexOrderTypeDESC)
			},
		})
		idx3Filtered = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   3,
			IndexName: "test",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.Balance > 100
			},
		})
	)

	testCases := []struct {
		name               string
		op                 string
		index              *Index[*TokenBalance]
		existingRow        *TokenBalance
		newRow             *TokenBalance
		setCallExpected    bool
		deleteCallExpected bool
	}{
		{
			name:            "Index_1_OnInsert",
			op:              "insert",
			index:           idx1,
			newRow:          &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected: true,
		},
		{
			name:               "Index_1_OnUpdate",
			op:                 "update",
			index:              idx1,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_1_OnUpdate_Same",
			op:                 "update",
			index:              idx1,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test"},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_1_OnDelete",
			op:                 "delete",
			index:              idx1,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			deleteCallExpected: true,
		},
		{
			name:            "Index_2_Ordered_OnInsert",
			op:              "insert",
			index:           idx2Ordered,
			newRow:          &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected: true,
		},
		{
			name:               "Index_2_Ordered_OnUpdate",
			op:                 "update",
			index:              idx2Ordered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected:    true,
			deleteCallExpected: true,
		},
		{
			name:               "Index_2_Ordered_OnUpdate_Same",
			op:                 "update",
			index:              idx2Ordered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test"},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_2_Ordered_OnDelete",
			op:                 "delete",
			index:              idx2Ordered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			deleteCallExpected: true,
		},
		{
			name:            "Index_3_Filtered_OnInsert",
			op:              "insert",
			index:           idx3Filtered,
			newRow:          &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected: false,
		},
		{
			name:               "Index_3_Filtered_OnUpdate",
			op:                 "update",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_3_Filtered_OnUpdate_Same",
			op:                 "update",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test"},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_3_Filtered_OnDelete",
			op:                 "delete",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
			deleteCallExpected: false,
		},
		{
			name:            "Index_3_Filtered_2_OnInsert",
			op:              "insert",
			index:           idx3Filtered,
			newRow:          &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			setCallExpected: true,
		},
		{
			name:               "Index_3_Filtered_2_OnUpdate",
			op:                 "update",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			setCallExpected:    true,
			deleteCallExpected: false,
		},
		{
			name:               "Index_3_Filtered_2_OnUpdate_Same",
			op:                 "update",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			newRow:             &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			setCallExpected:    false,
			deleteCallExpected: false,
		},
		{
			name:               "Index_3_Filtered_2_OnDelete",
			op:                 "delete",
			index:              idx3Filtered,
			existingRow:        &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			deleteCallExpected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockBatch := MockBatch{}

			switch tc.op {
			case "insert":
				if tc.setCallExpected {
					mockBatch.On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}

				err := tc.index.OnInsert(tokenBalanceTable, tc.newRow, &mockBatch)
				require.NoError(t, err)

				mockBatch.AssertExpectations(t)
			case "update":
				if tc.setCallExpected {
					mockBatch.On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}
				if tc.deleteCallExpected {
					mockBatch.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}

				err := tc.index.OnUpdate(tokenBalanceTable, tc.existingRow, tc.newRow, &mockBatch)
				require.NoError(t, err)

				mockBatch.AssertExpectations(t)
			case "delete":
				if tc.deleteCallExpected {
					mockBatch.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}

				err := tc.index.OnDelete(tokenBalanceTable, tc.existingRow, &mockBatch)
				require.NoError(t, err)

				mockBatch.AssertExpectations(t)
			}
		})
	}
}

func TestBond_NewIndex_Ordered(t *testing.T) {
	const (
		AccountIDOrderDESCBalanceIndexID = IndexID(1)
	)

	AccountIDIndexOrderDESCBalance := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   AccountIDOrderDESCBalanceIndexID,
		IndexName: "id_ord_by_bal_desc_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	testStructure := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "test",
		AccountAddress:  "test",
		TokenID:         0,
		Balance:         15,
	}

	_ = AccountIDIndexOrderDESCBalance
	assert.Equal(t, AccountIDOrderDESCBalanceIndexID, AccountIDIndexOrderDESCBalance.IndexID)
	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x01},
		AccountIDIndexOrderDESCBalance.IndexKeyFunction(NewKeyBuilder([]byte{}), testStructure))
	assert.Equal(t, []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF0},
		AccountIDIndexOrderDESCBalance.IndexOrderFunction(IndexOrder{NewKeyBuilder([]byte{})}, testStructure).Bytes())
}

func TestBond_Table_Index_Insert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	require.NoError(t, err)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}
	it.Close()

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[7]), "0xtestContract"))

	fmt.Printf("----------------- Database Contents ----------------- \n")

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
	it.Close()
}

func TestBond_Table_Index_Insert_Ordered(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance1Account2,
			tokenBalance2Account1,
		},
	)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}
	it.Close()

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1") && bytes.Contains(keys[3], []byte{0xF0}))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1") && bytes.Contains(keys[4], []byte{0xFA}))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[6]), "0xtestContract"))

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
	it.Close()
}

func TestBond_Table_Index_Update(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	tokenBalanceAccount1.AccountAddress = "0xtestAccount3"
	tokenBalance2Account1.AccountAddress = "0xtestAccount3"
	tokenBalance1Account2.AccountAddress = "0xtestAccount3"

	err = tokenBalanceTable.Update(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}
	it.Close()

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
	it.Close()
}

func TestBond_Table_Index_Update_Ordered(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	tokenBalanceAccount1.AccountAddress = "0xtestAccount3"
	tokenBalance2Account1.AccountAddress = "0xtestAccount3"
	tokenBalance1Account2.AccountAddress = "0xtestAccount3"

	err = tokenBalanceTable.Update(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}
	it.Close()

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount3") && bytes.Contains(keys[3], []byte{0xF0}))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount3") && bytes.Contains(keys[4], []byte{0xF8}))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount3") && bytes.Contains(keys[5], []byte{0xFA}))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
	it.Close()
}

func TestBond_Table_Index_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	err = tokenBalanceTable.Delete(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)
	assert.False(t, it.First())
	it.Close()

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
	it.Close()
}

func TestBond_Table_Reindex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		})
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAddressIndex}, false)
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err = tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	err = tokenBalanceTable.ReIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[7]), "0xtestContract"))

	_ = it.Close()

	TokenBalanceAccountAndContractAddressIndex.IndexFilterFunction = func(tr *TokenBalance) bool { return tr.ContractAddress == "0xtestContract2" }

	tokenBalanceTable = NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	err = tokenBalanceTable.ReIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex, TokenBalanceAccountAddressIndex})
	require.NoError(t, err)

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	keys = [][]byte{}
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 7, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))

	_ = it.Close()

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}

	_ = it.Close()
}

func TestIndex_Operations_WithDatabaseValidation(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceBalanceIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceBalanceIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceBalanceIndexID,
			IndexName: "balance_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddUint64Field(tb.Balance).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.Balance > 100 // Only index balances > 100
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceBalanceIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	// Helper function to validate database contents
	validateDatabaseContents := func(t *testing.T, expectedKeys []string) {
		it, err := db.Backend().NewIter(&pebble.IterOptions{
			LowerBound: []byte{byte(TokenBalanceTableID)},
			UpperBound: []byte{byte(TokenBalanceTableID + 1)},
		})
		require.NoError(t, err)
		defer it.Close()

		var actualKeys []string
		for it.First(); it.Valid(); it.Next() {
			actualKeys = append(actualKeys, fmt.Sprintf("%x", it.Key()))
		}

		// Handle nil vs empty slice difference
		if len(expectedKeys) == 0 && len(actualKeys) == 0 {
			return // Both are empty, regardless of nil vs []string{}
		}

		// Sort both slices for comparison
		sort.Strings(expectedKeys)
		sort.Strings(actualKeys)

		assert.Equal(t, expectedKeys, actualKeys, "Database contents don't match expected keys")
	}

	t.Run("OnInsert", func(t *testing.T) {
		// Test data
		records := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1", Balance: 50},  // Won't be in balance index
			{ID: 2, AccountAddress: "addr2", Balance: 150}, // Will be in both indexes
		}

		err := tokenBalanceTable.Insert(context.Background(), records)
		require.NoError(t, err)

		// Expected keys based on actual format observed
		expectedKeys := []string{
			// Primary keys (data) - TableID(01) + IndexID(00) + IndexLen(00000000) + OrderLen(00000000) + PrimaryKey(ID with field prefix)
			"01000000000000000000010000000000000001", // ID 1
			"01000000000000000000010000000000000002", // ID 2

			// Account address index keys - TableID(01) + IndexID(01) + IndexLen + IndexKey + OrderLen(00000000) + PrimaryKey
			"01010000000601616464723100000000010000000000000001", // addr1 -> ID 1
			"01010000000601616464723200000000010000000000000002", // addr2 -> ID 2

			// Balance index key (only for ID 2 as Balance > 100) - includes DESC ordering
			"0102000000090100000000000000960000000901ffffffffffffff69010000000000000002", // 150 DESC -> ID 2
		}

		validateDatabaseContents(t, expectedKeys)
	})

	t.Run("OnUpdate", func(t *testing.T) {
		// Update records
		updates := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1_new", Balance: 200}, // Will now be in balance index
			{ID: 2, AccountAddress: "addr2", Balance: 50},      // Will be removed from balance index
		}

		err := tokenBalanceTable.Update(context.Background(), updates)
		require.NoError(t, err)

		// Expected keys after update
		expectedKeys := []string{
			// Primary keys (data)
			"01000000000000000000010000000000000001", // ID 1
			"01000000000000000000010000000000000002", // ID 2

			// Account address index keys
			"01010000000601616464723200000000010000000000000002",         // addr2 -> ID 2
			"01010000000a0161646472315f6e657700000000010000000000000001", // addr1_new -> ID 1

			// Balance index key (only for ID 1 now as its Balance > 100)
			"0102000000090100000000000000c80000000901ffffffffffffff37010000000000000001", // 200 DESC -> ID 1
		}

		validateDatabaseContents(t, expectedKeys)
	})

	t.Run("OnDelete", func(t *testing.T) {
		// Delete one record
		toDelete := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1_new", Balance: 200},
		}

		err := tokenBalanceTable.Delete(context.Background(), toDelete)
		require.NoError(t, err)

		// Expected keys after deletion
		expectedKeys := []string{
			// Primary key (data)
			"01000000000000000000010000000000000002", // ID 2

			// Account address index key
			"01010000000601616464723200000000010000000000000002", // addr2 -> ID 2

			// No balance index keys as remaining record has Balance <= 100
		}

		validateDatabaseContents(t, expectedKeys)
	})

	t.Run("OnDelete_All", func(t *testing.T) {
		// Delete remaining record
		toDelete := []*TokenBalance{
			{ID: 2, AccountAddress: "addr2", Balance: 50},
		}

		err := tokenBalanceTable.Delete(context.Background(), toDelete)
		require.NoError(t, err)

		// Expect no keys in the database
		expectedKeys := []string{}
		validateDatabaseContents(t, expectedKeys)
	})
}

func TestIndex_Operations_WithOrderedIndex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountBalanceIndexID = iota
	)

	var TokenBalanceAccountBalanceIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   TokenBalanceAccountBalanceIndexID,
		IndexName: "account_balance_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountBalanceIndex}, false)
	require.NoError(t, err)

	// Helper function to validate database contents and order
	validateDatabaseContents := func(t *testing.T, expectedKeys []string) {
		it, err := db.Backend().NewIter(&pebble.IterOptions{
			LowerBound: []byte{byte(TokenBalanceTableID)},
			UpperBound: []byte{byte(TokenBalanceTableID + 1)},
		})
		require.NoError(t, err)
		defer it.Close()

		var actualKeys []string
		for it.First(); it.Valid(); it.Next() {
			actualKeys = append(actualKeys, fmt.Sprintf("%x", it.Key()))
		}

		assert.Equal(t, expectedKeys, actualKeys, "Database contents don't match expected keys")
	}

	t.Run("Insert_Ordered", func(t *testing.T) {
		records := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1", Balance: 150},
			{ID: 2, AccountAddress: "addr1", Balance: 300},
			{ID: 3, AccountAddress: "addr1", Balance: 100},
		}

		err := tokenBalanceTable.Insert(context.Background(), records)
		require.NoError(t, err)

		// Expected keys in hex format with ordered index
		// Balance 300 -> ^300 = fffffffffffffed3 (DESC)
		// Balance 150 -> ^150 = ffffffffffffff69 (DESC)
		// Balance 100 -> ^100 = ffffffffffffff9b (DESC)
		expectedKeys := []string{
			// Primary keys (data)
			"01000000000000000000010000000000000001",
			"01000000000000000000010000000000000002",
			"01000000000000000000010000000000000003",

			// Account-Balance ordered index keys (Balance in descending order)
			"0101000000060161646472310000000901fffffffffffffed3010000000000000002", // addr1, 300 DESC -> ID 2
			"0101000000060161646472310000000901ffffffffffffff69010000000000000001", // addr1, 150 DESC -> ID 1
			"0101000000060161646472310000000901ffffffffffffff9b010000000000000003", // addr1, 100 DESC -> ID 3
		}

		validateDatabaseContents(t, expectedKeys)
	})

	t.Run("Update_Ordered", func(t *testing.T) {
		updates := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1", Balance: 400}, // Move to top
			{ID: 2, AccountAddress: "addr1", Balance: 50},  // Move to bottom
		}

		err := tokenBalanceTable.Update(context.Background(), updates)
		require.NoError(t, err)

		// Expected keys after update with new order
		// Balance 400 -> ^400 = fffffffffffffe6f (DESC)
		// Balance 100 -> ^100 = ffffffffffffff9b (DESC)
		// Balance 50 -> ^50 = ffffffffffffffcd (DESC)
		expectedKeys := []string{
			// Primary keys (data)
			"01000000000000000000010000000000000001",
			"01000000000000000000010000000000000002",
			"01000000000000000000010000000000000003",

			// Account-Balance ordered index keys (Balance in descending order)
			"0101000000060161646472310000000901fffffffffffffe6f010000000000000001", // addr1, 400 DESC -> ID 1
			"0101000000060161646472310000000901ffffffffffffff9b010000000000000003", // addr1, 100 DESC -> ID 3
			"0101000000060161646472310000000901ffffffffffffffcd010000000000000002", // addr1, 50 DESC -> ID 2
		}

		validateDatabaseContents(t, expectedKeys)
	})

	t.Run("Delete_Ordered", func(t *testing.T) {
		toDelete := []*TokenBalance{
			{ID: 1, AccountAddress: "addr1", Balance: 400},
			{ID: 3, AccountAddress: "addr1", Balance: 100},
		}

		err := tokenBalanceTable.Delete(context.Background(), toDelete)
		require.NoError(t, err)

		// Expected keys after deletion
		expectedKeys := []string{
			// Primary key (data)
			"01000000000000000000010000000000000002",

			// Account-Balance ordered index key
			"0101000000060161646472310000000901ffffffffffffffcd010000000000000002", // addr1, 50 DESC -> ID 2
		}

		validateDatabaseContents(t, expectedKeys)
	})
}

func TestIndex_Operations_ComplexScenarios(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceHighBalanceIndexID
		TokenBalanceAccountContractIndexID
	)

	var (
		// Simple index
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})

		// Filtered index with ordering
		TokenBalanceHighBalanceIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceHighBalanceIndexID,
			IndexName: "high_balance_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddUint64Field(tb.Balance).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.Balance >= 1000 // Only high balances
			},
		})

		// Composite index
		TokenBalanceAccountContractIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountContractIndexID,
			IndexName: "account_contract_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.ID, IndexOrderTypeASC)
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceHighBalanceIndex,
		TokenBalanceAccountContractIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	// Helper function to count keys by index
	countKeysByIndex := func(t *testing.T) map[IndexID]int {
		it, err := db.Backend().NewIter(&pebble.IterOptions{
			LowerBound: []byte{byte(TokenBalanceTableID)},
			UpperBound: []byte{byte(TokenBalanceTableID + 1)},
		})
		require.NoError(t, err)
		defer it.Close()

		counts := make(map[IndexID]int)
		for it.First(); it.Valid(); it.Next() {
			key := KeyBytes(it.Key())
			counts[key.IndexID()]++
		}
		return counts
	}

	t.Run("Complex_Insert_Update_Delete_Cycle", func(t *testing.T) {
		// Insert initial records
		initialRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "alice", ContractAddress: "tokenA", Balance: 500},   // Not in high balance index
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenA", Balance: 1500},    // In high balance index
			{ID: 3, AccountAddress: "alice", ContractAddress: "tokenB", Balance: 2000},  // In high balance index
			{ID: 4, AccountAddress: "charlie", ContractAddress: "tokenA", Balance: 100}, // Not in high balance index
		}

		err := tokenBalanceTable.Insert(context.Background(), initialRecords)
		require.NoError(t, err)

		counts := countKeysByIndex(t)
		assert.Equal(t, 4, counts[PrimaryIndexID], "Should have 4 primary keys")
		assert.Equal(t, 4, counts[TokenBalanceAccountAddressIndexID], "Should have 4 account address index keys")
		assert.Equal(t, 2, counts[TokenBalanceHighBalanceIndexID], "Should have 2 high balance index keys")
		assert.Equal(t, 4, counts[TokenBalanceAccountContractIndexID], "Should have 4 account-contract index keys")

		// Update records to test filter transitions
		updates := []*TokenBalance{
			{ID: 1, AccountAddress: "alice", ContractAddress: "tokenA", Balance: 1200},     // Now enters high balance index
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenA", Balance: 800},        // Exits high balance index
			{ID: 3, AccountAddress: "alice_new", ContractAddress: "tokenB", Balance: 2500}, // Stays in high balance, changes account
		}

		err = tokenBalanceTable.Update(context.Background(), updates)
		require.NoError(t, err)

		counts = countKeysByIndex(t)
		assert.Equal(t, 4, counts[PrimaryIndexID], "Should still have 4 primary keys")
		assert.Equal(t, 4, counts[TokenBalanceAccountAddressIndexID], "Should still have 4 account address index keys")
		assert.Equal(t, 2, counts[TokenBalanceHighBalanceIndexID], "Should still have 2 high balance index keys (1 and 3)")
		assert.Equal(t, 4, counts[TokenBalanceAccountContractIndexID], "Should still have 4 account-contract index keys")

		// Delete some records
		toDelete := []*TokenBalance{
			{ID: 1, AccountAddress: "alice", ContractAddress: "tokenA", Balance: 1200},
			{ID: 4, AccountAddress: "charlie", ContractAddress: "tokenA", Balance: 100},
		}

		err = tokenBalanceTable.Delete(context.Background(), toDelete)
		require.NoError(t, err)

		counts = countKeysByIndex(t)
		assert.Equal(t, 2, counts[PrimaryIndexID], "Should have 2 primary keys")
		assert.Equal(t, 2, counts[TokenBalanceAccountAddressIndexID], "Should have 2 account address index keys")
		assert.Equal(t, 1, counts[TokenBalanceHighBalanceIndexID], "Should have 1 high balance index key (only ID 3)")
		assert.Equal(t, 2, counts[TokenBalanceAccountContractIndexID], "Should have 2 account-contract index keys")

		// Final cleanup
		finalDelete := []*TokenBalance{
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenA", Balance: 800},
			{ID: 3, AccountAddress: "alice_new", ContractAddress: "tokenB", Balance: 2500},
		}

		err = tokenBalanceTable.Delete(context.Background(), finalDelete)
		require.NoError(t, err)

		counts = countKeysByIndex(t)
		for indexID, count := range counts {
			assert.Equal(t, 0, count, "Index %d should have no keys", indexID)
		}
	})

	t.Run("Filter_Boundary_Conditions", func(t *testing.T) {
		// Test records at filter boundary
		boundaryRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "user1", ContractAddress: "token1", Balance: 999},  // Just below threshold
			{ID: 2, AccountAddress: "user2", ContractAddress: "token1", Balance: 1000}, // Exactly at threshold
			{ID: 3, AccountAddress: "user3", ContractAddress: "token1", Balance: 1001}, // Just above threshold
		}

		err := tokenBalanceTable.Insert(context.Background(), boundaryRecords)
		require.NoError(t, err)

		counts := countKeysByIndex(t)
		assert.Equal(t, 3, counts[PrimaryIndexID], "Should have 3 primary keys")
		assert.Equal(t, 2, counts[TokenBalanceHighBalanceIndexID], "Should have 2 high balance keys (>= 1000)")

		// Update to cross boundary
		updates := []*TokenBalance{
			{ID: 1, AccountAddress: "user1", ContractAddress: "token1", Balance: 1500}, // Crosses into filter
			{ID: 2, AccountAddress: "user2", ContractAddress: "token1", Balance: 500},  // Crosses out of filter
		}

		err = tokenBalanceTable.Update(context.Background(), updates)
		require.NoError(t, err)

		counts = countKeysByIndex(t)
		assert.Equal(t, 3, counts[PrimaryIndexID], "Should still have 3 primary keys")
		assert.Equal(t, 2, counts[TokenBalanceHighBalanceIndexID], "Should still have 2 high balance keys (1 and 3)")

		// Cleanup
		err = tokenBalanceTable.Delete(context.Background(), []*TokenBalance{
			{ID: 1, AccountAddress: "user1", ContractAddress: "token1", Balance: 1500},
			{ID: 2, AccountAddress: "user2", ContractAddress: "token1", Balance: 500},
			{ID: 3, AccountAddress: "user3", ContractAddress: "token1", Balance: 1001},
		})
		require.NoError(t, err)
	})

	t.Run("Empty_String_And_Zero_Values", func(t *testing.T) {
		// Test edge cases with empty/zero values
		edgeRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "", ContractAddress: "", Balance: 0},
			{ID: 2, AccountAddress: "a", ContractAddress: "", Balance: 0},
			{ID: 3, AccountAddress: "", ContractAddress: "b", Balance: 1000},
		}

		err := tokenBalanceTable.Insert(context.Background(), edgeRecords)
		require.NoError(t, err)

		counts := countKeysByIndex(t)
		assert.Equal(t, 3, counts[PrimaryIndexID], "Should have 3 primary keys")
		assert.Equal(t, 3, counts[TokenBalanceAccountAddressIndexID], "Should have 3 account address index keys")
		assert.Equal(t, 1, counts[TokenBalanceHighBalanceIndexID], "Should have 1 high balance key")
		assert.Equal(t, 3, counts[TokenBalanceAccountContractIndexID], "Should have 3 account-contract index keys")

		// Cleanup
		err = tokenBalanceTable.Delete(context.Background(), edgeRecords)
		require.NoError(t, err)
	})
}

func TestIndex_Operations_WithUpsert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceBalanceIndexID
		TokenBalanceAccountContractIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceBalanceIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceBalanceIndexID,
			IndexName: "balance_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddUint64Field(tb.Balance).Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
			IndexFilterFunc: func(tb *TokenBalance) bool {
				return tb.Balance > 100 // Only index balances > 100
			},
		})
		TokenBalanceAccountContractIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountContractIndexID,
			IndexName: "account_contract_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.ID, IndexOrderTypeASC)
			},
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceBalanceIndex,
		TokenBalanceAccountContractIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	require.NoError(t, err)

	// Helper function to count keys by index
	countKeysByIndex := func(t *testing.T) map[IndexID]int {
		it, err := db.Backend().NewIter(&pebble.IterOptions{
			LowerBound: []byte{byte(TokenBalanceTableID)},
			UpperBound: []byte{byte(TokenBalanceTableID + 1)},
		})
		require.NoError(t, err)
		defer it.Close()

		counts := make(map[IndexID]int)
		for it.First(); it.Valid(); it.Next() {
			key := KeyBytes(it.Key())
			counts[key.IndexID()]++
		}
		return counts
	}

	// Helper function to get actual records from database
	getRecords := func(t *testing.T) []*TokenBalance {
		var records []*TokenBalance
		err := tokenBalanceTable.Scan(context.Background(), &records, false)
		require.NoError(t, err)

		// Sort by ID for consistent comparison
		sort.Slice(records, func(i, j int) bool {
			return records[i].ID < records[j].ID
		})
		return records
	}

	t.Run("Upsert_Insert_New_Records", func(t *testing.T) {
		// Upsert new records (should behave like insert)
		records := []*TokenBalance{
			{ID: 1, AccountAddress: "alice", ContractAddress: "tokenA", Balance: 50},     // Not in balance index
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenB", Balance: 200},      // In balance index
			{ID: 3, AccountAddress: "charlie", ContractAddress: "tokenA", Balance: 1000}, // In balance index
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), records, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Verify database state
		counts := countKeysByIndex(t)
		assert.Equal(t, 3, counts[PrimaryIndexID], "Should have 3 primary keys")
		assert.Equal(t, 3, counts[TokenBalanceAccountAddressIndexID], "Should have 3 account address index keys")
		assert.Equal(t, 2, counts[TokenBalanceBalanceIndexID], "Should have 2 balance index keys (>100)")
		assert.Equal(t, 3, counts[TokenBalanceAccountContractIndexID], "Should have 3 account-contract index keys")

		// Verify actual records
		actualRecords := getRecords(t)
		assert.Equal(t, records, actualRecords, "Records should match exactly")
	})

	t.Run("Upsert_Update_Existing_Records", func(t *testing.T) {
		// Upsert existing records with changes (should behave like update)
		updates := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_new", ContractAddress: "tokenA", Balance: 150}, // Now in balance index
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenB", Balance: 80},        // No longer in balance index
			{ID: 3, AccountAddress: "charlie", ContractAddress: "tokenC", Balance: 2000},  // Still in balance index, changed contract
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), updates, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Verify database state
		counts := countKeysByIndex(t)
		assert.Equal(t, 3, counts[PrimaryIndexID], "Should still have 3 primary keys")
		assert.Equal(t, 3, counts[TokenBalanceAccountAddressIndexID], "Should still have 3 account address index keys")
		assert.Equal(t, 2, counts[TokenBalanceBalanceIndexID], "Should have 2 balance index keys (1 and 3)")
		assert.Equal(t, 3, counts[TokenBalanceAccountContractIndexID], "Should still have 3 account-contract index keys")

		// Verify actual records
		actualRecords := getRecords(t)
		assert.Equal(t, updates, actualRecords, "Records should match updates")
	})

	t.Run("Upsert_Mixed_Insert_Update", func(t *testing.T) {
		// Mix of new records and updates to existing ones
		mixed := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_final", ContractAddress: "tokenA", Balance: 300}, // Update existing
			{ID: 4, AccountAddress: "david", ContractAddress: "tokenD", Balance: 50},        // New record, not in balance index
			{ID: 5, AccountAddress: "eve", ContractAddress: "tokenE", Balance: 500},         // New record, in balance index
			{ID: 3, AccountAddress: "charlie", ContractAddress: "tokenC", Balance: 75},      // Update existing, remove from balance index
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), mixed, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Verify database state
		counts := countKeysByIndex(t)
		assert.Equal(t, 5, counts[PrimaryIndexID], "Should have 5 primary keys")
		assert.Equal(t, 5, counts[TokenBalanceAccountAddressIndexID], "Should have 5 account address index keys")
		assert.Equal(t, 2, counts[TokenBalanceBalanceIndexID], "Should have 2 balance index keys (1 and 5)")
		assert.Equal(t, 5, counts[TokenBalanceAccountContractIndexID], "Should have 5 account-contract index keys")

		// Verify actual records
		expectedRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_final", ContractAddress: "tokenA", Balance: 300},
			{ID: 2, AccountAddress: "bob", ContractAddress: "tokenB", Balance: 80},
			{ID: 3, AccountAddress: "charlie", ContractAddress: "tokenC", Balance: 75},
			{ID: 4, AccountAddress: "david", ContractAddress: "tokenD", Balance: 50},
			{ID: 5, AccountAddress: "eve", ContractAddress: "tokenE", Balance: 500},
		}
		actualRecords := getRecords(t)
		assert.Equal(t, expectedRecords, actualRecords, "Records should match expected state")

		// Cleanup for next test
		err = tokenBalanceTable.Delete(context.Background(), actualRecords)
		require.NoError(t, err)
	})

	t.Run("Upsert_Custom_Conflict_Resolution", func(t *testing.T) {
		// First, insert some base records to have conflicts with
		baseRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_original", ContractAddress: "tokenA", Balance: 300},
			{ID: 2, AccountAddress: "bob_original", ContractAddress: "tokenB", Balance: 80},
			{ID: 5, AccountAddress: "eve_original", ContractAddress: "tokenE", Balance: 500},
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), baseRecords, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Test custom conflict resolution function
		customConflictResolver := func(old, new *TokenBalance) *TokenBalance {
			// Keep the higher balance, but use new account address
			return &TokenBalance{
				ID:              new.ID,
				AccountID:       new.AccountID,
				ContractAddress: new.ContractAddress,
				AccountAddress:  new.AccountAddress,
				TokenID:         new.TokenID,
				Balance:         max(old.Balance, new.Balance),
			}
		}

		conflicts := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_conflict", ContractAddress: "tokenA", Balance: 100}, // Lower balance, should keep 300
			{ID: 2, AccountAddress: "bob_conflict", ContractAddress: "tokenB", Balance: 150},   // Higher balance, should update to 150
			{ID: 6, AccountAddress: "frank", ContractAddress: "tokenF", Balance: 800},          // New record
		}

		_, err = tokenBalanceTable.Upsert(context.Background(), conflicts, customConflictResolver)
		require.NoError(t, err)

		// Verify the conflict resolution worked
		actualRecords := getRecords(t)

		// Find the conflicted records
		var record1, record2, record5, record6 *TokenBalance
		for _, r := range actualRecords {
			switch r.ID {
			case 1:
				record1 = r
			case 2:
				record2 = r
			case 5:
				record5 = r
			case 6:
				record6 = r
			}
		}

		require.NotNil(t, record1)
		require.NotNil(t, record2)
		require.NotNil(t, record5)
		require.NotNil(t, record6)

		// Check conflict resolution results
		assert.Equal(t, "alice_conflict", record1.AccountAddress, "Should use new account address")
		assert.Equal(t, uint64(300), record1.Balance, "Should keep higher balance (300 > 100)")

		assert.Equal(t, "bob_conflict", record2.AccountAddress, "Should use new account address")
		assert.Equal(t, uint64(150), record2.Balance, "Should use new higher balance (150 > 80)")

		assert.Equal(t, "eve_original", record5.AccountAddress, "Should remain unchanged")
		assert.Equal(t, uint64(500), record5.Balance, "Should remain unchanged")

		assert.Equal(t, "frank", record6.AccountAddress, "New record should be inserted as-is")
		assert.Equal(t, uint64(800), record6.Balance, "New record should be inserted as-is")

		// Verify index counts
		counts := countKeysByIndex(t)
		assert.Equal(t, 4, counts[PrimaryIndexID], "Should have 4 primary keys")
		// Records with balance > 100: 1 (300), 2 (150), 5 (500), 6 (800) = 4 keys
		assert.Equal(t, 4, counts[TokenBalanceBalanceIndexID], "Should have 4 balance index keys (1, 2, 5, 6)")

		// Cleanup for next test
		err = tokenBalanceTable.Delete(context.Background(), actualRecords)
		require.NoError(t, err)
	})

	t.Run("Upsert_Filter_Boundary_Transitions", func(t *testing.T) {
		// First insert some base records
		baseRecords := []*TokenBalance{
			{ID: 1, AccountAddress: "alice_base", ContractAddress: "tokenA", Balance: 300}, // Above threshold
			{ID: 4, AccountAddress: "david_base", ContractAddress: "tokenD", Balance: 50},  // Below threshold
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), baseRecords, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Test records crossing filter boundaries during upsert
		boundaryTests := []*TokenBalance{
			{ID: 7, AccountAddress: "user7", ContractAddress: "token7", Balance: 99},           // New, below threshold
			{ID: 8, AccountAddress: "user8", ContractAddress: "token8", Balance: 101},          // New, above threshold
			{ID: 1, AccountAddress: "alice_boundary", ContractAddress: "tokenA", Balance: 50},  // Update, cross below threshold
			{ID: 4, AccountAddress: "david_boundary", ContractAddress: "tokenD", Balance: 200}, // Update, cross above threshold
		}

		_, err = tokenBalanceTable.Upsert(context.Background(), boundaryTests, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		// Verify filter transitions
		counts := countKeysByIndex(t)
		assert.Equal(t, 4, counts[PrimaryIndexID], "Should have 4 primary keys")
		// Should have balance index keys for: 8 (101), 4 (200) = 2 keys
		assert.Equal(t, 2, counts[TokenBalanceBalanceIndexID], "Should have 2 balance index keys")

		// Verify specific records
		actualRecords := getRecords(t)
		recordMap := make(map[uint64]*TokenBalance)
		for _, r := range actualRecords {
			recordMap[r.ID] = r
		}

		assert.Equal(t, uint64(50), recordMap[1].Balance, "ID 1 should have balance 50 (below threshold)")
		assert.Equal(t, uint64(200), recordMap[4].Balance, "ID 4 should have balance 200 (above threshold)")
		assert.Equal(t, uint64(99), recordMap[7].Balance, "ID 7 should have balance 99 (below threshold)")
		assert.Equal(t, uint64(101), recordMap[8].Balance, "ID 8 should have balance 101 (above threshold)")

		// Cleanup for next test
		err = tokenBalanceTable.Delete(context.Background(), actualRecords)
		require.NoError(t, err)
	})

	t.Run("Upsert_Duplicate_Keys_In_Batch", func(t *testing.T) {
		// Test upsert with duplicate keys in the same batch
		duplicates := []*TokenBalance{
			{ID: 9, AccountAddress: "user9_v1", ContractAddress: "token9", Balance: 100},
			{ID: 9, AccountAddress: "user9_v2", ContractAddress: "token9", Balance: 200}, // Same ID, should conflict with above
			{ID: 10, AccountAddress: "user10", ContractAddress: "token10", Balance: 300},
		}

		// Custom resolver that combines account addresses
		combineResolver := func(old, new *TokenBalance) *TokenBalance {
			return &TokenBalance{
				ID:              new.ID,
				AccountID:       new.AccountID,
				ContractAddress: new.ContractAddress,
				AccountAddress:  old.AccountAddress + "_" + new.AccountAddress,
				TokenID:         new.TokenID,
				Balance:         old.Balance + new.Balance,
			}
		}

		_, err := tokenBalanceTable.Upsert(context.Background(), duplicates, combineResolver)
		require.ErrorContains(t, err, "duplicate record found")
	})

	// Cleanup for next test
	t.Run("Cleanup", func(t *testing.T) {
		// Get all records and delete them
		allRecords := getRecords(t)
		if len(allRecords) > 0 {
			err := tokenBalanceTable.Delete(context.Background(), allRecords)
			require.NoError(t, err)
		}

		// Verify cleanup
		counts := countKeysByIndex(t)
		for indexID, count := range counts {
			assert.Equal(t, 0, count, "Index %d should have no keys after cleanup", indexID)
		}
	})
}
