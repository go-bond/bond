package bond

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBond_NewTable(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](
		db,
		TokenBalanceTableID,
		func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	)
	require.NotNil(t, tokenBalanceTable)
	assert.Equal(t, TokenBalanceTableID, tokenBalanceTable.TableID)
}

func TestBondTable_Insert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	it := db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount1, &tokenBalanceAccount1FromDB)
	}
}

func TestBondTable_Insert_When_Exist(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.Error(t, err)

	it := db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount1, &tokenBalanceAccount1FromDB)
	}
}

func TestBondTable_Update(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it := db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	err = tokenBalanceTable.Update([]*TokenBalance{tokenBalanceAccountUpdated})
	require.NoError(t, err)

	it = db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccountUpdated, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()
}

func TestBondTable_Upsert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccount2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         15,
	}

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it := db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	err = tokenBalanceTable.Upsert([]*TokenBalance{tokenBalanceAccountUpdated, tokenBalanceAccount2})
	require.NoError(t, err)

	it = db.NewIter(nil)

	var tokenBalances []*TokenBalance
	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Update_No_Such_Entry(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Update([]*TokenBalance{tokenBalanceAccountUpdated})
	require.Error(t, err)
	assert.False(t, db.NewIter(nil).First())
}

func TestBondTable_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = tokenBalanceTable.Delete([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	assert.False(t, db.NewIter(nil).First())
}

func TestBondTable_Exist(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	ifExist, record := tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.False(t, ifExist)
	assert.Nil(t, record)

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	ifExist, record = tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.True(t, ifExist)
	assert.NotNil(t, record)
}

func TestBondTable_Scan(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.Scan(&tokenBalances)
	require.NoError(t, err)
	require.Equal(t, len(tokenBalances), 3)

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])
}

func TestBondTable_ScanIndex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.ScanIndex(TokenBalanceAccountAddressIndex,
		&TokenBalance{AccountAddress: "0xtestAccount"}, &tokenBalances)
	require.NoError(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
}

func BenchmarkBondTableInsert_1(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tokenBalancesForInsert[0].ID += uint64(i)

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func TestBond_Batch(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	batch := db.NewIndexedBatch()

	exist, tokenBalance := tokenBalanceTable.Exist(&TokenBalance{ID: 1}, batch)
	require.True(t, exist)
	require.NotNil(t, tokenBalance)

	tokenBalance.Balance += 20

	err = tokenBalanceTable.Update([]*TokenBalance{tokenBalance}, batch)
	require.NoError(t, err)

	err = batch.Commit(pebble.Sync)
	require.NoError(t, err)

	it := db.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalance, &tokenBalanceAccountFromDB)
	}
}

func BenchmarkBondTableInsert_1000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, tokenBalance := range tokenBalancesForInsert {
			tokenBalance.ID += uint64(10000 * i)
		}

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableInsert_1000000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, tokenBalance := range tokenBalancesForInsert {
			tokenBalance.ID += uint64(1000000 * i)
		}

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableInsert_MsgPack_1(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tokenBalancesForInsert[0].ID += uint64(i)

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableInsert_MsgPack_1000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, tokenBalance := range tokenBalancesForInsert {
			tokenBalance.ID += uint64(10000 * i)
		}

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableInsert_MsgPack_1000000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, tokenBalance := range tokenBalancesForInsert {
			tokenBalance.ID += uint64(1000000 * i)
		}

		err := tokenBalanceTable.Insert(tokenBalancesForInsert)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScan_1000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScan_1000000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_1000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_1000000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScan_MsgPack_1000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScan_MsgPack_1000000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_MsgPack_1000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_MsgPack_1000000(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_1000000_Skip_Through(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			DefaultOrder[*TokenBalance],
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := tokenBalanceTable.ScanForEach(func(l Lazy[*TokenBalance]) (bool, error) {
			return true, nil
		})
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}
