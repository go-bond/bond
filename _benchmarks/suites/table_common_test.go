package suites

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenBalanceSerializer_Serialize(t *testing.T) {
	tb := &TokenBalance{
		ID:              1,
		AccountID:       10,
		ContractAddress: "0x0",
		AccountAddress:  "0x1",
		TokenID:         11,
		Balance:         7,
	}

	data, err := tb.MarshalMsg(make([]byte, 0, tb.Msgsize()))
	require.NoError(t, err)

	tb2 := &TokenBalance{}
	_, err = tb2.UnmarshalMsg(data)
	require.NoError(t, err)

	require.Equal(t, *tb, *tb2)
}
