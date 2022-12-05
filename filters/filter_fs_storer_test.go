package filters

import (
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFSFilterStorer(t *testing.T) {
	storer := NewFSFilterStorer(".")

	err := storer.Set([]byte("test_1"), []byte("value1"), bond.Sync)
	require.NoError(t, err)

	err = storer.Set([]byte("test_2"), []byte("value2"), bond.Sync)
	require.NoError(t, err)

	err = storer.Set([]byte("test_3"), []byte("value3"), bond.Sync)
	require.NoError(t, err)

	data, closer, err := storer.Get([]byte("test_1"))
	require.NoError(t, err)
	require.NotNil(t, closer)
	assert.Equal(t, []byte("value1"), data)

	data, closer, err = storer.Get([]byte("test_2"))
	require.NoError(t, err)
	require.NotNil(t, closer)
	assert.Equal(t, []byte("value2"), data)

	data, closer, err = storer.Get([]byte("test_3"))
	require.NoError(t, err)
	require.NotNil(t, closer)
	assert.Equal(t, []byte("value3"), data)

	err = storer.DeleteRange([]byte("test_1"), []byte("test_4"), bond.Sync)
	require.NoError(t, err)
}
