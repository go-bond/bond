package bond

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
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

func TestBond_IndexSelector(t *testing.T) {
	/*
		// Index Selector (already in bond, however with different api)
		// idx1: index on AccountAddress
		// Selects all records where AccountAddress == account1
		Table.Query().
			With(
				idx1,
				NewIndexSelectorPoints[*TokenBalance](
					&TokenBalance{AccountAddress: "account1"},
				),
			).
			Execute()

		// Index Selector
		// idx1: index on AccountAddress
		// Selects records for AccountAddress IN [account1, account8, account10]
		Table.Query().
			With(
				idx1,
				NewIndexSelectorPoints[*TokenBalance](
					&TokenBalance{AccountAddress: "account1"},
					&TokenBalance{AccountAddress: "account8"},
					&TokenBalance{AccountAddress: "account10"},
				),
			).
			Execute()

		// Range Index Selector
		// idx2: index on ContractAddress & TokenID
		// Selects all records with ContractAddress == contract0 and 0 <= TokenID < math.MaxUint64
		Table.Query().
			With(
				idx2,
				NewIndexSelectorRanges[*TokenBalance](
					[]*TokenBalance{
						{ContractAddress: "contract0", TokenID: 0},
						{ContractAddress: "contract0", TokenID: math.MaxUint64},
					},
				),
			).
			Execute()

		// Range Index Selector
		// idx2: index on ContractAddress & TokenID
		// Selects all records with ContractAddress == contract0 and TokenID >= 25
		Table.Query().
			With(
				idx2,
				NewIndexSelectorRanges[*TokenBalance](
					[]*TokenBalance{
						{ContractAddress: "contract0", TokenID: 25},
						{ContractAddress: "contract0", TokenID: math.MaxUint64},
					},
				),
			).
			Execute()

		// Range Index Selector
		// idx2: index on ContractAddress & TokenID
		// Selects all records with ContractAddress == contract0 and TokenID < 20 and TokenID >= 25
		Table.Query().
			With(
				idx2,
				NewIndexSelectorRanges[*TokenBalance](
					[]*TokenBalance{
						{ContractAddress: "contract0", TokenID: 0},
						{ContractAddress: "contract0", TokenID: 20},
					},
					[]*TokenBalance{
						{ContractAddress: "contract0", TokenID: 25},
						{ContractAddress: "contract0", TokenID: math.MaxUint64},
					},
				),
			).
			Execute()
	*/
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
