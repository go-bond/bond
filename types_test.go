package bond

import "time"

//go:generate msgp

type TokenBalance struct {
	ID              uint64 `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

type Token struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
}

type TokenHistory struct {
	BlockNumber     uint64    `json:"blockNumber" cbor:"1"`
	BlockHash       string    `json:"blockHash" cbor:"2"`
	ContractAddress string    `json:"contractAddress" cbor:"4"`
	FromAddress     string    `json:"fromAddress" cbor:"6"`
	ToAddress       string    `json:"toAddress" cbor:"7"`
	TxnHash         string    `json:"txnHash" cbor:"8"`
	TxnIndex        uint      `json:"txnIndex" cbor:"9"`
	TxnLogIndex     uint      `json:"txnLogIndex" cbor:"a"`
	TokenIDs        []uint64  `json:"tokenIds" cbor:"b"`
	Amounts         []uint64  `json:"amounts" cbor:"c"`
	TS              time.Time `json:"ts" cbor:"d"`
	//--
	// IndexAccountAddress string `json:"-" cbor:"-"`
}
