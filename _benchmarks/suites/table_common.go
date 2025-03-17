package suites

import (
	"os"

	"github.com/go-bond/bond"
)

type TokenBalance struct {
	ID              uint64 `json:"id" cbor:"1"`
	AccountID       uint32 `json:"accountId" cbor:"2"`
	ContractAddress string `json:"contractAddress" cbor:"3"`
	AccountAddress  string `json:"accountAddress" cbor:"4"`
	TokenID         uint32 `json:"tokenId" cbor:"5"`
	Balance         uint64 `json:"balance" cbor:"6"`
}

const dbName = "bench_db"

func setupDatabase(serializer ...bond.Serializer[any]) bond.DB {
	options := bond.DefaultOptions()
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, err := bond.Open(dbName, options)
	if err != nil {
		panic(err)
	}
	return db
}

func tearDownDatabase(db bond.DB) {
	_ = db.Close()
	_ = os.RemoveAll(dbName)
}
