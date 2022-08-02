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

	ifExist := tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.False(t, ifExist)

	err := tokenBalanceTable.Insert([]*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	ifExist = tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.True(t, ifExist)
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
			IndexOrderDefault[*TokenBalance],
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

	exist := tokenBalanceTable.Exist(&TokenBalance{ID: 1}, batch)
	require.True(t, exist)

	tokenBalance, err := tokenBalanceTable.Get(&TokenBalance{ID: 1}, batch)
	require.NoError(t, err)
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

func BenchmarkTableSuite(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	var serializers = []struct {
		Name       string
		Serializer Serializer[any]
	}{
		{"JSONSerializer", &JsonSerializer{}},
		{"MsgPackSerializer", &MsgPackSerializer{}},
	}

	for _, serializer := range serializers {
		b.Run(serializer.Name, func(b *testing.B) {
			db := setupDatabase(serializer.Serializer)

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
					IndexOrderDefault[*TokenBalance],
				)
			)

			err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
				TokenBalanceAccountAddressIndex,
			})
			if err != nil {
				panic(err)
			}

			var tokenBalances []*TokenBalance
			for i := 0; i < 10000000; i++ {
				tokenBalances = append(tokenBalances, &TokenBalance{
					ID:              uint64(i + 1),
					AccountID:       uint32(i % 10),
					ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
					AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5),
					Balance:         uint64((i % 100) * 10),
				})
			}

			var tokenBalancesAccount0 []*TokenBalance
			for _, tokenBalance := range tokenBalances {
				if tokenBalance.AccountAddress == "0xtestAccount0" {
					tokenBalancesAccount0 = append(tokenBalancesAccount0, tokenBalance)
				}
			}

			/*var insertBatches = []struct {
				batchSize int
			}{
				{batchSize: 1},
				{batchSize: 10},
				{batchSize: 100},
				{batchSize: 1000},
				{batchSize: 10000},
				{batchSize: 100000},
				{batchSize: 1000000},
			}

			for _, v := range insertBatches {
				b.Run(
					fmt.Sprintf("Insert_%d", v.batchSize),
					InsertInBatchSize(tokenBalanceTable, tokenBalances, v.batchSize),

			})

			var scanSizes = []struct {
				scanSize int
			}{
				{scanSize: 1},
				{scanSize: 10},
				{scanSize: 1000},
				{scanSize: 10000},
				{scanSize: 100000},
				{scanSize: 1000000},
			}

			for _, v := range scanSizes {
				b.Run(
					fmt.Sprintf("Scan_%d", v.scanSize),
					ScanElements(tokenBalanceTable, tokenBalances, v.scanSize),
				)
			}

			for _, v := range scanSizes {
				b.Run(
					fmt.Sprintf("ScanIndex_%d", v.scanSize),
					ScanIndexElements(tokenBalanceTable, TokenBalanceAccountAddressIndex,
						&TokenBalance{AccountAddress: "0xtestAccount0"}, tokenBalancesAccount0, v.scanSize),
				)
			}*/

			var skipReadSizes = []struct {
				skipNumber int
				readNumber int
			}{
				{skipNumber: 0, readNumber: 0},
				{skipNumber: 1000, readNumber: 0},
				{skipNumber: 10000, readNumber: 0},
				{skipNumber: 100000, readNumber: 0},
				{skipNumber: 1000000, readNumber: 0},
				{skipNumber: 0, readNumber: 1000},
				{skipNumber: 1000, readNumber: 1000},
				{skipNumber: 10000, readNumber: 1000},
				{skipNumber: 100000, readNumber: 1000},
				{skipNumber: 1000000, readNumber: 1000},
			}

			for _, v := range skipReadSizes {
				b.Run(
					fmt.Sprintf("Scan_Skip_%d_Read_%d", v.skipNumber, v.readNumber),
					ScanSkipThrough(tokenBalanceTable, tokenBalances, v.skipNumber, v.readNumber),
				)
			}

			for _, v := range skipReadSizes {
				b.Run(
					fmt.Sprintf("ScanIndex_Skip_%d_Read_%d", v.skipNumber, v.readNumber),
					ScanIndexSkipThrough(tokenBalanceTable, TokenBalanceAccountAddressIndex,
						&TokenBalance{AccountAddress: "0xtestAccount0"}, tokenBalancesAccount0, v.skipNumber, v.readNumber),
				)
			}

			tearDownDatabase(db)
		})
	}
}

func InsertInBatchSize(tbt *Table[*TokenBalance], tbs []*TokenBalance, insertBatchSize int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := tbt.Insert(tbs[:insertBatchSize])
			if err != nil {
				panic(err)
			}

			b.StopTimer()
			err = tbt.Delete(tbs[:insertBatchSize])
			b.StartTimer()
		}
	}
}

func ScanElements(tbt *Table[*TokenBalance], tbs []*TokenBalance, numberToScan int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = tbt.Scan(&tokenBalances)
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()
		err = tbt.Delete(tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

func ScanIndexElements(tbt *Table[*TokenBalance], idx *Index[*TokenBalance], sel *TokenBalance, tbs []*TokenBalance, numberToScan int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = tbt.ScanIndex(idx, sel, &tokenBalances)
		}

		b.StopTimer()
		err = tbt.Delete(tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

func ScanSkipThrough(tbt *Table[*TokenBalance], tbs []*TokenBalance, numberToSkip int, numberToRead int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(tbs[:numberToSkip+numberToRead])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var counter = 0
			var tokenBalances []*TokenBalance
			err = tbt.ScanForEach(func(l Lazy[*TokenBalance]) (bool, error) {
				counter++
				if counter <= numberToSkip {
					return true, nil
				}

				if counter == numberToSkip+numberToRead {
					return false, nil
				}

				tb, err := l.Get()
				if err != nil {
					return false, err
				}

				tokenBalances = append(tokenBalances, tb)
				return true, nil
			})
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()
		err = tbt.Delete(tbs[:numberToSkip+numberToRead])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

func ScanIndexSkipThrough(tbt *Table[*TokenBalance], idx *Index[*TokenBalance], sel *TokenBalance, tbs []*TokenBalance, numberToSkip int, numberToRead int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(tbs[:numberToSkip+numberToRead])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var counter = 1
			var tokenBalances []*TokenBalance
			err = tbt.ScanForEach(func(l Lazy[*TokenBalance]) (bool, error) {
				counter++
				if counter <= numberToSkip {
					return true, nil
				}

				if counter == numberToSkip+numberToRead {
					return false, nil
				}

				tb, err := l.Get()
				if err != nil {
					return false, err
				}

				tokenBalances = append(tokenBalances, tb)
				return true, nil
			})
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()
		err = tbt.Delete(tbs[:numberToSkip+numberToRead])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}
