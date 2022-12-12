package bond

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func encodeNum(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func key(tID TableID, i uint64) sstable.InternalKey {
	buf := encodeNum(i)
	k := KeyEncode(Key{
		PrimaryKey: buf,
		TableID:    tID,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
	})
	return sstable.InternalKey{
		UserKey: k,
		Trailer: (1 << 8) | uint64(pebble.InternalKeyKindSet),
	}
}

func TestCollector(t *testing.T) {

	collector := &BlockCollector{
		tableRange: NewKeyRange(),
		indexRange: NewKeyRange(),
		blockRange: NewKeyRange(),
	}

	require.NoError(t, collector.Add(key(1, 1), []byte{}))
	require.NoError(t, collector.Add(key(2, 2), []byte{}))
	require.NoError(t, collector.Add(key(1, 5), []byte{}))
	require.NoError(t, collector.Add(key(2, 8), []byte{}))
	require.NoError(t, collector.Add(key(3, 1), []byte{}))
	require.NoError(t, collector.Add(key(2, 9), []byte{}))
	require.NoError(t, collector.Add(key(1, 6), []byte{}))
	require.NoError(t, collector.Add(key(1, 8), []byte{}))

	_, err := collector.FinishDataBlock([]byte{})
	require.NoError(t, err)
	require.Equal(t, encodeNum(1), collector.blockRange.Ranges[1].Min)
	require.Equal(t, encodeNum(8), collector.blockRange.Ranges[1].Max)
	require.Equal(t, encodeNum(2), collector.blockRange.Ranges[2].Min)
	require.Equal(t, encodeNum(9), collector.blockRange.Ranges[2].Max)
	require.Equal(t, encodeNum(1), collector.blockRange.Ranges[3].Min)
	require.Equal(t, encodeNum(1), collector.blockRange.Ranges[3].Max)

	collector.AddPrevDataBlockToIndexBlock()
	require.Equal(t, len(collector.blockRange.Ranges), 0)
	require.Equal(t, encodeNum(1), collector.indexRange.Ranges[1].Min)
	require.Equal(t, encodeNum(8), collector.indexRange.Ranges[1].Max)
	require.Equal(t, encodeNum(2), collector.indexRange.Ranges[2].Min)
	require.Equal(t, encodeNum(9), collector.indexRange.Ranges[2].Max)
	require.Equal(t, encodeNum(1), collector.indexRange.Ranges[3].Min)
	require.Equal(t, encodeNum(1), collector.indexRange.Ranges[3].Max)

	require.NoError(t, collector.Add(key(1, 10), []byte{}))
	require.NoError(t, collector.Add(key(2, 10), []byte{}))
	require.NoError(t, collector.Add(key(1, 12), []byte{}))
	require.NoError(t, collector.Add(key(2, 13), []byte{}))
	require.NoError(t, collector.Add(key(3, 4), []byte{}))
	require.NoError(t, collector.Add(key(2, 16), []byte{}))
	require.NoError(t, collector.Add(key(1, 18), []byte{}))
	require.NoError(t, collector.Add(key(1, 20), []byte{}))

	_, err = collector.FinishDataBlock([]byte{})
	require.NoError(t, err)
	require.Equal(t, encodeNum(10), collector.blockRange.Ranges[1].Min)
	require.Equal(t, encodeNum(20), collector.blockRange.Ranges[1].Max)
	require.Equal(t, encodeNum(10), collector.blockRange.Ranges[2].Min)
	require.Equal(t, encodeNum(16), collector.blockRange.Ranges[2].Max)
	require.Equal(t, encodeNum(4), collector.blockRange.Ranges[3].Min)
	require.Equal(t, encodeNum(4), collector.blockRange.Ranges[3].Max)

	collector.AddPrevDataBlockToIndexBlock()
	require.Equal(t, len(collector.blockRange.Ranges), 0)
	require.Equal(t, encodeNum(1), collector.indexRange.Ranges[1].Min)
	require.Equal(t, encodeNum(20), collector.indexRange.Ranges[1].Max)
	require.Equal(t, encodeNum(2), collector.indexRange.Ranges[2].Min)
	require.Equal(t, encodeNum(16), collector.indexRange.Ranges[2].Max)
	require.Equal(t, encodeNum(1), collector.indexRange.Ranges[3].Min)
	require.Equal(t, encodeNum(4), collector.indexRange.Ranges[3].Max)

	_, err = collector.FinishIndexBlock([]byte{})
	require.NoError(t, err)
	require.Equal(t, len(collector.indexRange.Ranges), 0)
	require.Equal(t, encodeNum(1), collector.tableRange.Ranges[1].Min)
	require.Equal(t, encodeNum(20), collector.tableRange.Ranges[1].Max)
	require.Equal(t, encodeNum(2), collector.tableRange.Ranges[2].Min)
	require.Equal(t, encodeNum(16), collector.tableRange.Ranges[2].Max)
	require.Equal(t, encodeNum(1), collector.tableRange.Ranges[3].Min)
	require.Equal(t, encodeNum(4), collector.tableRange.Ranges[3].Max)

	require.NoError(t, collector.Add(key(1, 21), []byte{}))
	require.NoError(t, collector.Add(key(2, 17), []byte{}))
	require.NoError(t, collector.Add(key(1, 22), []byte{}))
	require.NoError(t, collector.Add(key(2, 19), []byte{}))
	require.NoError(t, collector.Add(key(3, 7), []byte{}))
	require.NoError(t, collector.Add(key(2, 21), []byte{}))
	require.NoError(t, collector.Add(key(1, 25), []byte{}))
	require.NoError(t, collector.Add(key(1, 26), []byte{}))

	_, err = collector.FinishDataBlock([]byte{})
	require.NoError(t, err)
	require.Equal(t, encodeNum(21), collector.blockRange.Ranges[1].Min)
	require.Equal(t, encodeNum(26), collector.blockRange.Ranges[1].Max)
	require.Equal(t, encodeNum(17), collector.blockRange.Ranges[2].Min)
	require.Equal(t, encodeNum(21), collector.blockRange.Ranges[2].Max)
	require.Equal(t, encodeNum(7), collector.blockRange.Ranges[3].Min)
	require.Equal(t, encodeNum(7), collector.blockRange.Ranges[3].Max)

	collector.AddPrevDataBlockToIndexBlock()
	_, err = collector.FinishTable([]byte{})
	require.NoError(t, err)
	require.Equal(t, len(collector.indexRange.Ranges), 0)
	require.Equal(t, encodeNum(1), collector.tableRange.Ranges[1].Min)
	require.Equal(t, encodeNum(26), collector.tableRange.Ranges[1].Max)
	require.Equal(t, encodeNum(2), collector.tableRange.Ranges[2].Min)
	require.Equal(t, encodeNum(21), collector.tableRange.Ranges[2].Max)
	require.Equal(t, encodeNum(1), collector.tableRange.Ranges[3].Min)
	require.Equal(t, encodeNum(7), collector.tableRange.Ranges[3].Max)
}

func TestRange(t *testing.T) {
	keyRange := NewKeyRange()
	keyRange.Ranges[1] = &Range{
		Min: encodeNum(1),
		Max: encodeNum(2),
	}
	keyRange.Ranges[2] = &Range{
		Min: encodeNum(3),
		Max: encodeNum(4),
	}

	encoded := keyRange.Encode([]byte{})

	newKeyRange := NewKeyRange()
	newKeyRange.Decode(encoded)
	require.Equal(t, 2, len(newKeyRange.Ranges))
	require.Equal(t, keyRange.Ranges[1].Min, newKeyRange.Ranges[1].Min)
	require.Equal(t, keyRange.Ranges[1].Max, newKeyRange.Ranges[1].Max)
	require.Equal(t, keyRange.Ranges[2].Min, newKeyRange.Ranges[2].Min)
	require.Equal(t, keyRange.Ranges[2].Max, newKeyRange.Ranges[2].Max)
}

func TestFilter(t *testing.T) {
	keyRange := NewKeyRange()
	keyRange.Ranges[1] = &Range{
		Min: encodeNum(1),
		Max: encodeNum(10),
	}
	keyRange.Ranges[2] = &Range{
		Min: encodeNum(3),
		Max: encodeNum(40),
	}
	encoded := keyRange.Encode([]byte{})

	filter := NewPrimaryKeyFilter(1, [][]byte{key(1, 3).UserKey})
	intersects, err := filter.Intersects(encoded)
	require.NoError(t, err)
	require.Equal(t, true, intersects)

	filter.Keys = [][]byte{key(1, 11).UserKey}
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, false, intersects)

	filter = NewPrimaryKeyFilter(2, [][]byte{key(2, 48).UserKey})
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.Equal(t, false, intersects)

	filter.Keys = [][]byte{key(3, 40).UserKey}
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, true, intersects)

	filter.Keys = [][]byte{key(2, 1).UserKey, key(2, 34).UserKey, key(2, 42).UserKey}
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, true, intersects)

	filter.Keys = [][]byte{key(2, 1).UserKey, key(2, 42).UserKey}
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, false, intersects)

	filter.Keys = [][]byte{key(2, 3).UserKey, key(2, 42).UserKey}
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, true, intersects)

	filter.Keys = [][]byte{key(3, 40).UserKey}
	filter.ID = 3
	intersects, err = filter.Intersects(encoded)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, false, intersects)
}
