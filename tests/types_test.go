package bond_test

import (
	"encoding/json"
	"fmt"
)

type Account struct {
	ID      uint32 `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

func accountKey(id uint32) []byte {
	return []byte(fmt.Sprintf("account/%d", id))
}

func (a *Account) Key() []byte {
	// return []byte(fmt.Sprintf("%d", a.ID))
	return accountKey(a.ID)
}

func (a *Account) Value() ([]byte, error) {
	return json.Marshal(a)
}

type TokenBalance struct {
	ID              uint64 `json:"uint64"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

func (b *TokenBalance) Key() []byte {
	return []byte(fmt.Sprintf("%s-%s-%d", b.ContractAddress, b.AccountAddress, b.TokenID))
}

func (b *TokenBalance) Value() ([]byte, error) {
	return json.Marshal(b)
}
