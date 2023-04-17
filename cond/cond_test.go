package cond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TokenBalance struct {
	AccountID uint32
}

var TokenBalanceAccountID = func(tb *TokenBalance) uint32 {
	return tb.AccountID
}

func TestBond_Eq(t *testing.T) {
	cond := Eq(TokenBalanceAccountID, 1)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 2}))
}

func TestBond_Gt(t *testing.T) {
	cond := Gt(TokenBalanceAccountID, 0)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Gte(t *testing.T) {
	cond := Gte(TokenBalanceAccountID, 1)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lt(t *testing.T) {
	cond := Lt(TokenBalanceAccountID, 1)

	assert.False(t, cond.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, cond.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lte(t *testing.T) {
	cond := Lte(TokenBalanceAccountID, 0)

	assert.False(t, cond.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, cond.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_And(t *testing.T) {
	cond := And(
		Gte(TokenBalanceAccountID, 5),
		Lt(TokenBalanceAccountID, 6),
	)

	assert.False(t, cond.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, cond.Eval(&TokenBalance{AccountID: 5}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 6}))
}

func TestBond_Or(t *testing.T) {
	cond := Or(
		Gte(TokenBalanceAccountID, 5),
		Lt(TokenBalanceAccountID, 3),
	)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 2}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 3}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, cond.Eval(&TokenBalance{AccountID: 5}))
}

func TestBond_Not(t *testing.T) {
	cond := Not(
		Gte(TokenBalanceAccountID, 5),
	)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 4}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 5}))
}

func TestBond_Mix(t *testing.T) {
	cond := And(
		Or(
			Gte(TokenBalanceAccountID, 5),
			Lt(TokenBalanceAccountID, 3),
		),
		Not(
			Gte(TokenBalanceAccountID, 5),
		),
	)

	assert.True(t, cond.Eval(&TokenBalance{AccountID: 2}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 3}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 4}))
	assert.False(t, cond.Eval(&TokenBalance{AccountID: 5}))
}
