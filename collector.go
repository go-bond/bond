package bond

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"

	"github.com/cockroachdb/pebble/sstable"
)

var (
	BlockCollectorID = "bc"
)

type Range struct {
	Max []byte
	Min []byte
}

var primaryFilterPool = &sync.Pool{
	New: func() any {
		return &PrimaryKeyFilter{
			KeyRange: NewKeyRange(),
		}
	},
}

var rangePool = utils.SyncPoolWrapper[*Range]{
	Pool: sync.Pool{
		New: func() any {
			return &Range{}
		},
	},
}

type KeyRange struct {
	Ranges map[TableID]*Range
}

func NewKeyRange() *KeyRange {
	return &KeyRange{
		Ranges: make(map[TableID]*Range, 4),
	}
}

func (b *KeyRange) Encode(buf []byte) []byte {
	// block meta encoded as:
	// TableID | minKeyLen | minKey | maxKeyLen | maxKey | TableID | ...
	lenBuf := make([]byte, 4)
	buff := bytes.NewBuffer(buf)
	for tableID, property := range b.Ranges {
		buff.WriteByte(byte(tableID))
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Min)))
		buff.Write(lenBuf)
		buff.Write(property.Min)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Max)))
		buff.Write(lenBuf)
		buff.Write(property.Max)
	}
	return buff.Bytes()
}

func (b *KeyRange) Decode(buf []byte) {
	b.Ranges = make(map[TableID]*Range)
	if len(buf) == 0 {
		return
	}

	buff := bytes.NewBuffer(buf)

	for {
		tableID, err := buff.ReadByte()
		if err == io.EOF {
			return
		}
		keyRange := &Range{}
		lenBuf := buff.Next(4)
		keyLen := binary.BigEndian.Uint32(lenBuf)
		keyRange.Min = utils.Copy(keyRange.Min, buff.Next(int(keyLen)))

		lenBuf = buff.Next(4)
		keyLen = binary.BigEndian.Uint32(lenBuf)
		keyRange.Max = utils.Copy(keyRange.Max, buff.Next(int(keyLen)))
		b.Ranges[TableID(tableID)] = keyRange
	}
}

type BlockCollector struct {
	blockRange *KeyRange
	tableRange *KeyRange
}

// Add implements sstable.BlockPropertyCollector
func (b *BlockCollector) Add(pebbleKey sstable.InternalKey, value []byte) error {
	if pebbleKey.Kind() != pebble.InternalKeyKindSet {
		return nil
	}

	key := KeyBytes(pebbleKey.UserKey)
	if key.IndexID() != PrimaryIndexID {
		return nil
	}

	tableID := key.TableID()
	keyRange, ok := b.blockRange.Ranges[tableID]
	if !ok {
		keyRange := &Range{}
		keyRange.Min = utils.Copy(keyRange.Min, key.PrimaryKey())
		keyRange.Max = utils.Copy(keyRange.Max, key.PrimaryKey())
		b.blockRange.Ranges[tableID] = keyRange
		return nil
	}

	if bytes.Compare(keyRange.Min, key.PrimaryKey()) <= 0 {
		keyRange.Max = utils.Copy(keyRange.Max, key.PrimaryKey())
		b.blockRange.Ranges[tableID] = keyRange
		return nil
	}

	panic("Incoming key can't be a small key, since insertion at block happens in sorted order")
}

// AddPrevDataBlockToIndexBlock implements sstable.BlockPropertyCollector
func (b *BlockCollector) AddPrevDataBlockToIndexBlock() {
	for tableID, blockRange := range b.blockRange.Ranges {
		tableRange, ok := b.tableRange.Ranges[tableID]
		if !ok {
			b.tableRange.Ranges[tableID] = blockRange
			delete(b.blockRange.Ranges, tableID)
			continue
		}
		if bytes.Compare(tableRange.Min, blockRange.Min) > 0 {
			tableRange.Min = utils.Copy(tableRange.Min, blockRange.Min)
		}

		if bytes.Compare(tableRange.Max, blockRange.Max) < 0 {
			tableRange.Max = utils.Copy(tableRange.Max, blockRange.Max)
		}
		b.tableRange.Ranges[tableID] = tableRange
		delete(b.blockRange.Ranges, tableID)
	}
}

// FinishDataBlock implements sstable.BlockPropertyCollector
func (b *BlockCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	return b.blockRange.Encode(buf), nil
}

// FinishIndexBlock implements sstable.BlockPropertyCollector
func (*BlockCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	panic("index block need to be implemeted. But, this won't be called unless we tweek the configuration")
	return buf, nil
}

// FinishTable implements sstable.BlockPropertyCollector
func (b *BlockCollector) FinishTable(buf []byte) ([]byte, error) {
	return b.tableRange.Encode(buf), nil
}

// Name implements sstable.BlockPropertyCollector
func (*BlockCollector) Name() string {
	return BlockCollectorID
}

var _ pebble.BlockPropertyCollector = &BlockCollector{}

type PrimaryKeyFilter struct {
	ID  TableID
	Key []byte
	*KeyRange
}

// Intersects implements base.BlockPropertyFilter
func (p *PrimaryKeyFilter) Intersects(prop []byte) (bool, error) {
	p.Decode(prop)
	meta, ok := p.Ranges[p.ID]
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
