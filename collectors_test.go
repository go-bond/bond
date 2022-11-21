package bond

import (
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableIDCollector(t *testing.T) {
	collector := &_tableIDCollector{}

	err := collector.Add(sstable.InternalKey{UserKey: KeyEncode(Key{TableID: 1})}, nil)
	require.NoError(t, err)

	props := make(map[string]string)
	err = collector.Finish(props)
	require.NoError(t, err)

	assert.True(t, _filterTableID(1)(props))
	assert.False(t, _filterTableID(2)(props))
}

func TestIndexIDCollector(t *testing.T) {
	collector := &_indexIDCollector{}

	err := collector.Add(sstable.InternalKey{UserKey: KeyEncode(Key{IndexID: 1})}, nil)
	require.NoError(t, err)

	props := make(map[string]string)
	err = collector.Finish(props)
	require.NoError(t, err)

	assert.True(t, _filterIndexID(1)(props))
	assert.False(t, _filterIndexID(2)(props))
}
