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
	indexOrderKey1 := IndexOrder{}.OrderInt64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt64(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt64(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt64(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderInt32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt32(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt32(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt32(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderInt16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt16(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt16(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt16(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderInt64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt64(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt64(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt64(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderInt32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt32(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt32(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt32(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderInt16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt16(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt16(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt16(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint64(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint64(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint64(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint32(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint32(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint32(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint16(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint16(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint16(2, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint64(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint64(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint64(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint32(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint32(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint32(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderUint16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint16(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint16(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint16(2, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeASC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeDESC).Bytes()

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
	indexOrderKey1 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(50, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.
		OrderUint64(1, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(90, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.
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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
		name        string
		op          string
		index       *Index[*TokenBalance]
		existingRow *TokenBalance
		newRow      *TokenBalance
	}{
		{
			name:   "Index_1_OnInsert",
			op:     "insert",
			index:  idx1,
			newRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_1_OnUpdate",
			op:          "update",
			index:       idx1,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_1_OnUpdate_Same",
			op:          "update",
			index:       idx1,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test"},
		},
		{
			name:        "Index_1_OnDelete",
			op:          "delete",
			index:       idx1,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:   "Index_2_Ordered_OnInsert",
			op:     "insert",
			index:  idx2Ordered,
			newRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_2_Ordered_OnUpdate",
			op:          "update",
			index:       idx2Ordered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_2_Ordered_OnUpdate_Same",
			op:          "update",
			index:       idx2Ordered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test"},
		},
		{
			name:        "Index_2_Ordered_OnDelete",
			op:          "delete",
			index:       idx2Ordered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:   "Index_3_Filtered_OnInsert",
			op:     "insert",
			index:  idx3Filtered,
			newRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_3_Filtered_OnUpdate",
			op:          "update",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:        "Index_3_Filtered_OnUpdate_Same",
			op:          "update",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test"},
		},
		{
			name:        "Index_3_Filtered_OnDelete",
			op:          "delete",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 20},
		},
		{
			name:   "Index_3_Filtered_2_OnInsert",
			op:     "insert",
			index:  idx3Filtered,
			newRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
		},
		{
			name:        "Index_3_Filtered_2_OnUpdate",
			op:          "update",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test"},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
		},
		{
			name:        "Index_3_Filtered_2_OnUpdate_Same",
			op:          "update",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
			newRow:      &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
		},
		{
			name:        "Index_3_Filtered_2_OnDelete",
			op:          "delete",
			index:       idx3Filtered,
			existingRow: &TokenBalance{ID: 1, AccountAddress: "test", Balance: 200},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockBatch := MockBatch{}

			switch tc.op {
			case "insert":
				if tc.index.IndexFilterFunction(tc.newRow) {
					mockBatch.On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}

				err := tc.index.OnInsert(tokenBalanceTable, tc.newRow, &mockBatch)
				require.NoError(t, err)

				mockBatch.AssertExpectations(t)
			case "update":
				if tc.index.IndexFilterFunction(tc.newRow) {
					if !bytes.Equal(
						encodeIndexKey(tokenBalanceTable, tc.existingRow, tc.index, []byte{}),
						encodeIndexKey(tokenBalanceTable, tc.newRow, tc.index, []byte{})) {
						mockBatch.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil)
						mockBatch.On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
					}
				}

				err := tc.index.OnUpdate(tokenBalanceTable, tc.existingRow, tc.newRow, &mockBatch)
				require.NoError(t, err)

				mockBatch.AssertExpectations(t)
			case "delete":
				if tc.index.IndexFilterFunction(tc.existingRow) {
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
	defer tearDownDatabase(db)

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

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

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

	fmt.Printf("----------------- Database Contents ----------------- \n")

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Insert_Ordered(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1") && bytes.Contains(keys[3], []byte{0xF0}))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1") && bytes.Contains(keys[4], []byte{0xFA}))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[6]), "0xtestContract"))

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Update(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Update_Ordered(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount3") && bytes.Contains(keys[3], []byte{0xF0}))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount3") && bytes.Contains(keys[4], []byte{0xF8}))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount3") && bytes.Contains(keys[5], []byte{0xFA}))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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
	assert.False(t, db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	}).First())

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Reindex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	err = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex}, true)
	require.NoError(t, err)

	it := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

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

	err = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex}, true)
	require.NoError(t, err)

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

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

	it = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}

	_ = it.Close()
}
