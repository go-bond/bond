package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBond_Eq(t *testing.T) {
	eq := &Eq[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := Evaluable[*TokenBalance](eq)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
}

func TestBond_Gt(t *testing.T) {
	gt := &Gt[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 0}

	evaluable := Evaluable[*TokenBalance](gt)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Gte(t *testing.T) {
	gte := &Gte[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := Evaluable[*TokenBalance](gte)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lt(t *testing.T) {
	lt := &Lt[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 1}

	evaluable := Evaluable[*TokenBalance](lt)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_Lte(t *testing.T) {
	lte := &Lte[*TokenBalance, uint32]{func(tb *TokenBalance) uint32 {
		return tb.AccountID
	}, 0}

	evaluable := Evaluable[*TokenBalance](lte)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 1}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 0}))
}

func TestBond_And(t *testing.T) {
	andCond := &and[*TokenBalance]{
		&Gte[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			5,
		},
		&Lt[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			6,
		},
	}

	evaluable := Evaluable[*TokenBalance](andCond)

	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 6}))
}

func TestBond_Or(t *testing.T) {
	orCond := &or[*TokenBalance]{
		&Gte[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			5,
		},
		&Lt[*TokenBalance, uint32]{
			func(tb *TokenBalance) uint32 {
				return tb.AccountID
			},
			3,
		},
	}

	evaluable := Evaluable[*TokenBalance](orCond)

	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 2}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 3}))
	assert.False(t, evaluable.Eval(&TokenBalance{AccountID: 4}))
	assert.True(t, evaluable.Eval(&TokenBalance{AccountID: 5}))
}
