package bond

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"

	"github.com/cockroachdb/pebble/sstable"
)

var (
	BlockCollectorID = "bc"
	// Primary Key does not contain Index Key and Index Order Key. So, it's guaranteed that Primary Key
	// begin at the index `10`.
	PrimaryKeyStartIdx = 10
)

type Range struct {
	Max []byte
	Min []byte
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
		// minKeyLen | minKey
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Min)))
		buff.Write(lenBuf)
		buff.Write(property.Min)
		// maxKeyLen | maxKey
		binary.BigEndian.PutUint32(lenBuf, uint32(len(property.Max)))
		buff.Write(lenBuf)
		buff.Write(property.Max)
	}
	return buff.Bytes()
}

func (b *KeyRange) Merge(in *KeyRange) {
	for tableID, keyRange := range in.Ranges {
		localRange, ok := b.Ranges[tableID]
		if !ok {
			b.Ranges[tableID] = keyRange
			delete(in.Ranges, tableID)
			continue
		}

		if bytes.Compare(localRange.Min, keyRange.Min) > 0 {
			localRange.Min = utils.Copy(localRange.Min, keyRange.Min)
		}

		if bytes.Compare(localRange.Max, keyRange.Max) < 0 {
			localRange.Max = utils.Copy(localRange.Max, keyRange.Max)
		}

		b.Ranges[tableID] = localRange
		delete(in.Ranges, tableID)
	}
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
	indexRange *KeyRange
}

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
		keyRange.Min = utils.Copy(keyRange.Min, key[PrimaryKeyStartIdx:])
		keyRange.Max = utils.Copy(keyRange.Max, key[PrimaryKeyStartIdx:])
		b.blockRange.Ranges[tableID] = keyRange
		return nil
	}

	if bytes.Compare(keyRange.Min, key[PrimaryKeyStartIdx:]) <= 0 {
		keyRange.Max = utils.Copy(keyRange.Max, key[PrimaryKeyStartIdx:])
		b.blockRange.Ranges[tableID] = keyRange
		return nil
	}
	panic("Incoming key can't be a small key, since insertion at block happens in sorted order")
}

func (b *BlockCollector) AddPrevDataBlockToIndexBlock() {
	b.indexRange.Merge(b.blockRange)
}

func (b *BlockCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	return b.blockRange.Encode(buf), nil
}

func (b *BlockCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buf = b.indexRange.Encode(buf)
	b.tableRange.Merge(b.indexRange)
	return buf, nil
}

func (b *BlockCollector) FinishTable(buf []byte) ([]byte, error) {
	b.tableRange.Merge(b.indexRange)
	return b.tableRange.Encode(buf), nil
}

func (*BlockCollector) Name() string {
	return BlockCollectorID
}

var _ pebble.BlockPropertyCollector = &BlockCollector{}

type PrimaryKeyFilter struct {
	ID  TableID
	Key []byte
	*KeyRange
}

func NewPrimaryKeyFilter(id TableID) *PrimaryKeyFilter {
	return &PrimaryKeyFilter{
		KeyRange: NewKeyRange(),
		ID:       id,
		Key:      make([]byte, DataKeyBufferSize),
	}
}

func (p *PrimaryKeyFilter) Intersects(prop []byte) (bool, error) {
	if len(prop) == 0 {
		return false, nil
	}

	buff := bytes.NewBuffer(prop)
	for {
		tableID, err := buff.ReadByte()
		if err == io.EOF {
			return false, nil
		}

		lenBuf := buff.Next(4)
		keyLen := binary.BigEndian.Uint32(lenBuf)
		min := buff.Next(int(keyLen))

		lenBuf = buff.Next(4)
		keyLen = binary.BigEndian.Uint32(lenBuf)
		max := buff.Next(int(keyLen))

		if tableID != byte(p.ID) {
			continue
		}
		return (bytes.Compare(min, p.Key[PrimaryKeyStartIdx:]) <= 0 && bytes.Compare(max, p.Key[PrimaryKeyStartIdx:]) >= 0), nil
	}
}

func (*PrimaryKeyFilter) Name() string {
	return BlockCollectorID
}

var _ pebble.BlockPropertyFilter = &PrimaryKeyFilter{}
