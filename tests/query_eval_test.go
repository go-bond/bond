package tests

import (
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
)

func TestBond_Eq(t *testing.T) {
	eq := &bond.Eq[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := bond.Evaluable[*TokenBalance](eq)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
}

func TestBond_Gt(t *testing.T) {
	gt := &bond.Gt[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 0}

	evaluable := bond.Evaluable[*TokenBalance](gt)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Gte(t *testing.T) {
	gte := &bond.Gte[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := bond.Evaluable[*TokenBalance](gte)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lt(t *testing.T) {
	lt := &bond.Lt[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := bond.Evaluable[*TokenBalance](lt)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lte(t *testing.T) {
	lte := &bond.Lte[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 0}

	evaluable := bond.Evaluable[*TokenBalance](lte)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_And(t *testing.T) {
	andCond := &bond.And[*TokenBalance]{
		&bond.Gte[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			5,
		},
		&bond.Lt[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			6,
		},
	}

	evaluable := bond.Evaluable[*TokenBalance](andCond)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 6}))
}

func TestBond_Or(t *testing.T) {
	orCond := &bond.Or[*TokenBalance]{
		&bond.Gte[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			5,
		},
		&bond.Lt[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			3,
		},
	}

	evaluable := bond.Evaluable[*TokenBalance](orCond)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 3}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
}
