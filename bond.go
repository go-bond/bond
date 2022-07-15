package bond

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble"
)

type Options struct {
	pebble.Options

	Serializer Serializer
}

type Key struct {
	TableID   TableID
	IndexID   IndexID
	IndexKey  []byte
	RecordKey []byte
}

func (k Key) ToTableKey() Key {
	return Key{
		TableID:   k.TableID,
		IndexID:   MainIndexID,
		IndexKey:  []byte{},
		RecordKey: k.RecordKey,
	}
}

func (k Key) ToPrefix() Key {
	return Key{
		TableID:   k.TableID,
		IndexID:   k.IndexID,
		IndexKey:  k.IndexKey,
		RecordKey: []byte{},
	}
}

func (k Key) IsTableKey() bool {
	return k.IndexID == MainIndexID && len(k.IndexKey) == 0
}

func (k Key) IsIndexKey() bool {
	return k.IndexID != MainIndexID && len(k.IndexKey) != 0
}

func (k Key) IsPrefix() bool {
	return len(k.RecordKey) == 0
}

func KeyEncode(key Key) []byte {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Write([]byte{byte(key.TableID)})
	buff.Write([]byte{byte(key.IndexID)})

	indexLenBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(indexLenBuff, uint32(len(key.IndexKey)))
	buff.Write(indexLenBuff)
	buff.Write(key.IndexKey)

	buff.Write(key.RecordKey)

	return buff.Bytes()
}

func KeyDecode(keyBytes []byte) Key {
	buff := bytes.NewBuffer(keyBytes)

	tableID, _ := buff.ReadByte()
	indexID, _ := buff.ReadByte()

	indexLenBuff := make([]byte, 4)
	_, _ = buff.Read(indexLenBuff)
	indexLen := binary.BigEndian.Uint32(indexLenBuff)

	indexKey := make([]byte, indexLen)
	_, _ = buff.Read(indexKey)

	recordKeyLen := len(keyBytes) - 6 - int(indexLen)
	recordKey := make([]byte, recordKeyLen)
	_, _ = buff.Read(recordKey)

	return Key{
		TableID:   TableID(tableID),
		IndexID:   IndexID(indexID),
		IndexKey:  indexKey,
		RecordKey: recordKey,
	}
}

func KeyPrefixSplitIndex(rawKey []byte) int {
	return 6 + int(binary.BigEndian.Uint32(rawKey[2:6]))
}

type DB struct {
	*pebble.DB

	serializer Serializer
}

func Open(dirname string, opts *Options) (*DB, error) {
	if opts.Comparer == nil {
		opts.Comparer = pebble.DefaultComparer
		opts.Comparer.Split = KeyPrefixSplitIndex
	}

	pdb, err := pebble.Open(dirname, &opts.Options)
	if err != nil {
		return nil, err
	}

	var serializer Serializer
	if opts.Serializer != nil {
		serializer = opts.Serializer
	} else {
		serializer = &JsonSerializer{}
	}

	return &DB{DB: pdb, serializer: serializer}, nil
}

func (db *DB) Serializer() Serializer {
	return db.serializer
}

func (db *DB) Close() error {
	return db.DB.Close()
}
