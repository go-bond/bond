package bond

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/pebble"
)

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

func KeyEncode(key Key, rawBuffs ...[]byte) []byte {
	var rawBuff []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		rawBuff = rawBuffs[0]
	}

	buff := bytes.NewBuffer(rawBuff)
	buff.Write([]byte{byte(key.TableID)})
	buff.Write([]byte{byte(key.IndexID)})

	var indexLenBuff [4]byte
	binary.BigEndian.PutUint32(indexLenBuff[:4], uint32(len(key.IndexKey)))
	buff.Write(indexLenBuff[:4])
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

func KeyBytesToTableKeyBytes(key []byte, rawBuffs ...[]byte) []byte {
	var rawBuff []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		rawBuff = rawBuffs[0]
	}

	buff := bytes.NewBuffer(rawBuff)

	buff.WriteByte(key[0])
	buff.WriteByte(0)

	buff.Write([]byte{0, 0, 0, 0})

	indexKeyLen := int(binary.BigEndian.Uint32(key[2:6]))
	buff.Write(key[6+indexKeyLen:])

	return buff.Bytes()
}

func KeyPrefixSplitIndex(rawKey []byte) int {
	return 6 + int(binary.BigEndian.Uint32(rawKey[2:6]))
}

type Options struct {
	pebble.Options

	Serializer Serializer
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

func (db *DB) getBatchOrDB(key []byte, batch *pebble.Batch) (data []byte, closer io.Closer, err error) {
	if batch != nil {
		data, closer, err = batch.Get(key)
	} else {
		data, closer, err = db.Get(key)
	}
	return
}
