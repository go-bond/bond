package bond

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
)

const (
	// BOND_DB_DATA_TABLE_ID ..
	BOND_DB_DATA_TABLE_ID = 0x0

	// BOND_DB_DATA_USER_SPACE_INDEX_ID
	BOND_DB_DATA_USER_SPACE_INDEX_ID = 0xFF
)

type WriteOptions struct {
	Sync bool
}

var (
	Sync   = WriteOptions{Sync: true}
	NoSync = WriteOptions{Sync: false}
)

type Getter interface {
	Get(key []byte, batch ...Batch) (data []byte, closer io.Closer, err error)
}

type DeferredSetter interface {
	SetDeferred(keyLen, valueLen int) *pebble.DeferredBatchOp
}

type Setter interface {
	Set(key []byte, value []byte, opt WriteOptions, batch ...Batch) error
}

type Deleter interface {
	Delete(key []byte, opt WriteOptions, batch ...Batch) error
}

type DeleterWithRange interface {
	DeleteRange(start []byte, end []byte, opt WriteOptions, batch ...Batch) error
}

type Batcher interface {
	Batch() Batch
	WriteBatch() Batch
}

type Iterationer interface {
	Iter(opt *IterOptions, batch ...Batch) Iterator
}

type Applier interface {
	Apply(b Batch, opt WriteOptions) error
}

type Closer io.Closer

type DB interface {
	Serializer() Serializer[any]

	Getter
	Setter
	Deleter
	DeleterWithRange
	Iterationer

	Batcher
	Applier

	Closer

	OnClose(func(db DB))
}

type _db struct {
	pebble *pebble.DB

	serializer Serializer[any]

	onCloseCallbacks []func(db DB)
}

func Open(dirname string, opts *Options) (DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if opts.PebbleOptions == nil {
		opts.PebbleOptions = DefaultPebbleOptions()
	}

	opts.PebbleOptions.Comparer = DefaultKeyComparer()

	pdb, err := pebble.Open(dirname, opts.PebbleOptions)
	if err != nil {
		return nil, err
	}

	var serializer Serializer[any]
	if opts.Serializer != nil {
		serializer = opts.Serializer
	} else {
		serializer = &serializers.JsonSerializer{}
	}

	db := &_db{pebble: pdb, serializer: serializer}

	if db.Version() == 0 {
		if err := db.initVersion(); err != nil {
			return nil, err
		}
	} else if db.Version() != BOND_DB_DATA_VERSION {
		return nil, fmt.Errorf("bond db version is %d but expecting %d", db.Version(), BOND_DB_DATA_VERSION)
	}

	return db, nil
}

func (db *_db) Serializer() Serializer[any] {
	return db.serializer
}

func (db *_db) Get(key []byte, batch ...Batch) (data []byte, closer io.Closer, err error) {
	if batch != nil && len(batch) > 0 && batch[0] != nil {
		data, closer, err = batch[0].Get(key)
	} else {
		data, closer, err = db.pebble.Get(key)
	}
	return
}

func (db *_db) Set(key []byte, value []byte, opt WriteOptions, batch ...Batch) error {
	if batch != nil && len(batch) > 0 && batch[0] != nil {
		return batch[0].Set(key, value, opt)
	} else {
		return db.pebble.Set(key, value, pebbleWriteOptions(opt))
	}
}

func (db *_db) Delete(key []byte, opts WriteOptions, batch ...Batch) error {
	if batch != nil && len(batch) > 0 && batch[0] != nil {
		return batch[0].Delete(key, opts)
	} else {
		return db.pebble.Delete(key, pebbleWriteOptions(opts))
	}
}

func (db *_db) DeleteRange(start []byte, end []byte, opt WriteOptions, batch ...Batch) error {
	if batch != nil && len(batch) > 0 && batch[0] != nil {
		return batch[0].DeleteRange(start, end, opt)
	} else {
		return db.pebble.DeleteRange(start, end, pebbleWriteOptions(opt))
	}
}

func (db *_db) Iter(opt *IterOptions, batch ...Batch) Iterator {
	if batch != nil && len(batch) > 0 && batch[0] != nil {
		return batch[0].Iter(opt)
	} else {
		return db.pebble.NewIter(pebbleIterOptions(opt))
	}
}

func (db *_db) Batch() Batch {
	return newBatch(db)
}

func (db *_db) WriteBatch() Batch {
	return newWriteBatch(db)
}

func (db *_db) Apply(b Batch, opt WriteOptions) error {
	return b.Commit(opt)
}

func (db *_db) Close() error {
	db.notifyOnClose()
	return db.pebble.Close()
}

func (db *_db) OnClose(f func(db DB)) {
	db.onCloseCallbacks = append(db.onCloseCallbacks, f)
}

func (db *_db) notifyOnClose() {
	for _, onClose := range db.onCloseCallbacks {
		onClose(db)
	}
}

func pebbleWriteOptions(opt WriteOptions) *pebble.WriteOptions {
	if opt == NoSync {
		return pebble.NoSync
	}
	return pebble.Sync
}
