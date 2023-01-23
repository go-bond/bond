package suites

import (
	"os"

	"github.com/go-bond/bond"
)

//go:generate msgp

type TokenBalance struct {
	ID              uint64 `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

type TokenBalanceSerializer struct {
}

func (s *TokenBalanceSerializer) Serialize(tb **TokenBalance) ([]byte, error) {
	return (*tb).MarshalMsg(make([]byte, 0, (*tb).Msgsize()))
}

func (s *TokenBalanceSerializer) Deserialize(b []byte, tb **TokenBalance) error {
	nTb := &TokenBalance{}
	_, err := nTb.UnmarshalMsg(b)
	*tb = nTb
	return err
}

const dbName = "bench_db"

func setupDatabase(serializer ...bond.Serializer[any]) bond.DB {
	_ = os.RemoveAll(dbName)
	options := &bond.Options{}
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, _ := bond.Open(dbName, options)
	return db
}

func tearDownDatabase(db bond.DB) {
	_ = db.Close()
	_ = os.RemoveAll(dbName)
}
