package bond

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/cockroachdb/pebble/sstable"
)

var (
	BlockCollectorID = "bc"
)

type KeyMeta struct {
	Max []byte
	Min []byte
}

var keyMetaPool = &sync.Pool{
	New: func() any {
		return &KeyMeta{}
	},
}

type BlockMeta struct {
	Properties map[TableID]*KeyMeta
}

func NewBlockMeta() *BlockMeta {
	return &BlockMeta{
		Properties: make(map[TableID]*KeyMeta, 4),
	}
}

func (b *BlockMeta) Encode(buf []byte) []byte {
	// block meta encoded as:
	// TableID | minKeyLen | minKey | maxKeyLen | maxKey | TableID | ...
	lenBuf := make([]byte, 4)
	buff := bytes.NewBuffer(buf)
	for tableID, property := range b.Properties {
		buff.WriteByte(byte(tableID))
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Min)))
		buff.Write(lenBuf)
		buff.Write(property.Min)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Max)))
		buff.Write(lenBuf)
		buff.Write(property.Max)
		keyMetaPool.Put(property)
		delete(b.Properties, tableID)
	}

	return buff.Bytes()
}

func (b *BlockMeta) Decode(buf []byte) {
	if len(buf) == 0 {
		return
	}

	buff := bytes.NewBuffer(buf)

	for {
		tableID, err := buff.ReadByte()
		if err == io.EOF {
			return
		}
		meta := keyMetaPool.Get().(*KeyMeta)
		lenBuf := buff.Next(4)
		keyLen := binary.BigEndian.Uint32(lenBuf)
		meta.Min = buff.Next(int(keyLen))

		lenBuf = buff.Next(4)
		keyLen = binary.BigEndian.Uint32(lenBuf)
		meta.Max = buff.Next(int(keyLen))
		b.Properties[TableID(tableID)] = meta
	}

}

type BlockCollector struct {
	*BlockMeta
}

// Add implements sstable.BlockPropertyCollector
func (b *BlockCollector) Add(pebbleKey sstable.InternalKey, value []byte) error {
	if pebbleKey.Trailer != uint64(pebble.InternalKeyKindSet) {
		return nil
	}

	key := KeyBytes(pebbleKey.UserKey)
	if key.IndexID() != PrimaryIndexID {
		return nil
	}

	tableID := key.TableID()
	primaryKey := key.PrimaryKey()

	meta, ok := b.BlockMeta.Properties[tableID]
	if !ok {
		meta := keyMetaPool.Get().(*KeyMeta)
		meta.Min = primaryKey
		meta.Max = primaryKey
		b.BlockMeta.Properties[tableID] = meta
		return nil
	}

	if bytes.Compare(meta.Min, primaryKey) < 0 {
		meta.Max = primaryKey
		return nil
	}
	meta.Max = meta.Min
	meta.Min = primaryKey
	return nil
}

// AddPrevDataBlockToIndexBlock implements sstable.BlockPropertyCollector
func (*BlockCollector) AddPrevDataBlockToIndexBlock() {
}

// FinishDataBlock implements sstable.BlockPropertyCollector
func (b *BlockCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	return b.Encode(buf), nil
}

// FinishIndexBlock implements sstable.BlockPropertyCollector
func (*BlockCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	return buf, nil
}

// FinishTable implements sstable.BlockPropertyCollector
func (*BlockCollector) FinishTable(buf []byte) ([]byte, error) {
	return buf, nil
}

// Name implements sstable.BlockPropertyCollector
func (*BlockCollector) Name() string {
	return BlockCollectorID
}

var _ pebble.BlockPropertyCollector = &BlockCollector{}

type PrimaryKeyFilter struct {
	ID  TableID
	Key []byte
	*BlockMeta
}

// Intersects implements base.BlockPropertyFilter
func (p *PrimaryKeyFilter) Intersects(prop []byte) (bool, error) {
	p.Decode(prop)

	meta, ok := p.Properties[p.ID]
	if !ok {
		return false, nil
	}

	return (bytes.Compare(meta.Min, p.Key) <= 0 && bytes.Compare(meta.Max, p.Key) >= 0), nil
}

// Name implements base.BlockPropertyFilter
func (*PrimaryKeyFilter) Name() string {
	return BlockCollectorID
}

var _ pebble.BlockPropertyFilter = &PrimaryKeyFilter{}
