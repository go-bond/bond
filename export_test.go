package bond

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExport(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = IndexID(iota)
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
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	for i := 0; i < 10; i++ {
		err := tokenBalanceTable.Insert(context.TODO(), []*TokenBalance{&TokenBalance{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("contractaddr_%d", i),
			AccountAddress:  fmt.Sprintf("accountaddr_%d", i),
			Balance:         uint64(i),
		}})
		require.NoError(t, err)
	}
	err := db.Export(context.TODO(), "./export", true, tokenBalanceTable)
	require.NoError(t, err)

	// // create a tmp db.
	// db2 := setupDB("tmp_db")
	// defer tearDownDB("tmp_db", db2)
	// table := tokenBalanceTable.(*_table[*TokenBalance])
	// table.db = db2
	// err = db2.Import(context.TODO(), "export", true, table)
	// require.NoError(t, err)

	// // make sure both db has same keys and values.
	// itr := db.Iter(&IterOptions{})
	// itr2 := db2.Iter(&IterOptions{})
	// itr.First()
	// itr2.First()
	// i := 0
	// for ; itr.Valid(); itr.Next() {
	// 	require.Equal(t, itr.Key(), itr2.Key())
	// 	require.Equal(t, itr.Value(), itr2.Value())
	// 	itr2.Next()
	// 	fmt.Println(i)
	// 	i++
	// }
	// require.False(t, itr2.Valid())
}
