package bond

import (
	"bytes"
	"testing"

	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/require"
)

func TestCBORSerializer(t *testing.T) {
	s := serializers.CBORSerializer{}

	tb := &TokenBalance{
		ID:              5,
		AccountID:       3,
		ContractAddress: "abc",
		AccountAddress:  "xyz",
		TokenID:         12,
		Balance:         7,
	}

	buff, err := s.Serialize(tb)
	require.NoError(t, err)
	require.NotNil(t, buff)

	var tb2 *TokenBalance
	err = s.Deserialize(buff, &tb2)
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}

func TestCBORSerializerWithBuffer(t *testing.T) {
	s := serializers.CBORSerializer{}

	tb := &TokenBalance{
		ID:              5,
		AccountID:       3,
		ContractAddress: "abc",
		AccountAddress:  "xyz",
		TokenID:         12,
		Balance:         7,
	}

	buff := bytes.NewBuffer(nil)
	serialize := s.SerializeFuncWithBuffer(buff)

	data, err := serialize(&tb)
	require.NoError(t, err)
	require.NotNil(t, data)

	var tb2 *TokenBalance
	err = s.Deserialize(data, &tb2)
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}
