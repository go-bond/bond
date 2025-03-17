package bond

import (
	"testing"

	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/require"
)

func TestCBORSerializer(t *testing.T) {
	s := serializers.CBORSerializer{
		// TODOXXX
		// Encoder: &utils.SyncPoolWrapper[*msgpack.Encoder]{
		// 	Pool: sync.Pool{New: func() interface{} {
		// 		return msgpack.NewEncoder(nil)
		// 	}},
		// },
		// Decoder: &utils.SyncPoolWrapper[*msgpack.Decoder]{
		// 	Pool: sync.Pool{New: func() interface{} {
		// 		return msgpack.NewDecoder(nil)
		// 	}},
		// },
		// Buffer: &utils.SyncPoolWrapper[bytes.Buffer]{
		// 	Pool: sync.Pool{New: func() interface{} { return bytes.Buffer{} }},
		// },
	}

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
