package bond

import (
	"bytes"
	"sync"
	"testing"

	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestMsgpackSerializer_SerializerWithClosable(t *testing.T) {
	s := serializers.MsgpackSerializer{
		Encoder: &utils.SyncPoolWrapper[*msgpack.Encoder]{
			Pool: sync.Pool{New: func() interface{} {
				return msgpack.NewEncoder(nil)
			}},
		},
		Decoder: &utils.SyncPoolWrapper[*msgpack.Decoder]{
			Pool: sync.Pool{New: func() interface{} {
				return msgpack.NewDecoder(nil)
			}},
		},
		Buffer: &utils.SyncPoolWrapper[bytes.Buffer]{
			Pool: sync.Pool{New: func() interface{} { return bytes.Buffer{} }},
		},
	}

	tb := &TokenBalance{
		ID:              5,
		AccountID:       3,
		ContractAddress: "abc",
		AccountAddress:  "xyz",
		TokenID:         12,
		Balance:         7,
	}

	buff, closeBuff, err := s.SerializerWithCloseable(tb)
	require.NoError(t, err)
	require.NotNil(t, buff)
	require.NotNil(t, closeBuff)

	var tb2 *TokenBalance
	err = s.Deserialize(buff, &tb2)
	require.NoError(t, err)

	closeBuff()

	assert.Equal(t, tb, tb2)
}

func TestMsgpackGenSerializer_SerializerWithClosable(t *testing.T) {
	s := serializers.MsgpackGenSerializer{
		Buffer: &utils.SyncPoolWrapper[bytes.Buffer]{
			Pool: sync.Pool{New: func() interface{} { return bytes.Buffer{} }},
		},
	}

	tb := &TokenBalance{
		ID:              5,
		AccountID:       3,
		ContractAddress: "abc",
		AccountAddress:  "xyz",
		TokenID:         12,
		Balance:         7,
	}

	buff, closeBuff, err := s.SerializerWithCloseable(tb)
	require.NoError(t, err)
	require.NotNil(t, buff)
	require.NotNil(t, closeBuff)

	var tb2 *TokenBalance
	err = s.Deserialize(buff, &tb2)
	require.NoError(t, err)

	closeBuff()

	assert.Equal(t, tb, tb2)
}
