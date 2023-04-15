package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var TokenBalanceAccountID = func(tb *TokenBalance) uint32 {
	return tb.AccountID
}

func TestBond_Eq(t *testing.T) {
	evaluable := Eq(TokenBalanceAccountID, 1)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
}

func TestBond_Gt(t *testing.T) {
	evaluable := Gt(TokenBalanceAccountID, 0)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Gte(t *testing.T) {
	evaluable := Gte(TokenBalanceAccountID, 1)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lt(t *testing.T) {
	evaluable := Lt(TokenBalanceAccountID, 1)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lte(t *testing.T) {
	evaluable := Lte(TokenBalanceAccountID, 0)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_And(t *testing.T) {
	evaluable := And(
		Gte(TokenBalanceAccountID, 5),
		Lt(TokenBalanceAccountID, 6),
	)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 6}))
}

func TestBond_Or(t *testing.T) {
	evaluable := Or(
		Gte(TokenBalanceAccountID, 5),
		Lt(TokenBalanceAccountID, 3),
	)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 3}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
}

func TestBond_Not(t *testing.T) {
	evaluable := Not(
		Gte(TokenBalanceAccountID, 5),
	)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
}
