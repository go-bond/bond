package bond

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/utils"
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
}

type Iterationer interface { // TODO: weird name
	Iter(opt *IterOptions, batch ...Batch) Iterator
}

type Applier interface {
	Apply(b Batch, opt WriteOptions) error
}

type Closer io.Closer

const DefaultKeyBufferSize = 2048
const DefaultValueBufferSize = 2048
const DefaultNumberOfKeyBuffersInMultiKeyBuffer = 1000

const DefaultNumberOfPreAllocKeyBuffers = 2 * persistentBatchSize
const DefaultNumberOfPreAllocMultiKeyBuffers = 10
const DefaultNumberOfPreAllocValueBuffers = 10 * DefaultScanPrefetchSize
const DefaultNumberOfPreAllocBytesArrays = 50

type internalPools interface {
	getKeyBufferPool() *utils.PreAllocatedPool[[]byte]
	getMultiKeyBufferPool() *utils.PreAllocatedPool[[]byte]
	getValueBufferPool() *utils.PreAllocatedPool[[]byte]
	getBytesArrayPool() *utils.PreAllocatedPool[[][]byte]
	getKeyArray(numOfKeys int) [][]byte
	putKeyArray(arr [][]byte)
	getValueArray(numOfValues int) [][]byte
	putValueArray(arr [][]byte)
}

type DB interface {
	internalPools

	Backend() *pebble.DB
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

	keyBufferPool      *utils.PreAllocatedPool[[]byte]
	multiKeyBufferPool *utils.PreAllocatedPool[[]byte]
	valueBufferPool    *utils.PreAllocatedPool[[]byte]
	byteArraysPool     *utils.PreAllocatedPool[[][]byte]

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

	db := &_db{
		pebble:     pdb,
		serializer: serializer,
		keyBufferPool: utils.NewPreAllocatedPool[[]byte](func() any {
			return make([]byte, 0, DefaultKeyBufferSize)
		}, DefaultNumberOfPreAllocKeyBuffers),
		multiKeyBufferPool: utils.NewPreAllocatedPool[[]byte](func() any {
			return make([]byte, 0, DefaultKeyBufferSize*DefaultNumberOfKeyBuffersInMultiKeyBuffer)
		}, DefaultNumberOfPreAllocMultiKeyBuffers),
		valueBufferPool: utils.NewPreAllocatedPool[[]byte](func() any {
			return make([]byte, 0, DefaultValueBufferSize)
		}, DefaultNumberOfPreAllocValueBuffers),
		byteArraysPool: utils.NewPreAllocatedPool[[][]byte](func() any {
			return make([][]byte, 0, persistentBatchSize)
		}, DefaultNumberOfPreAllocBytesArrays),
	}

	if db.Version() == 0 {
		if err := db.initVersion(); err != nil {
			return nil, err
		}
	} else if db.Version() != BOND_DB_DATA_VERSION {
		return nil, fmt.Errorf("bond db version is %d but expecting %d", db.Version(), BOND_DB_DATA_VERSION)
	}

	return db, nil
}

func (db *_db) Backend() *pebble.DB {
	return db.pebble
}

func (db *_db) Serializer() Serializer[any] {
	return db.serializer
}

func (db *_db) Get(key []byte, batch ...Batch) (data []byte, closer io.Closer, err error) {
	if len(batch) > 0 && batch[0] != nil {
		data, closer, err = batch[0].Get(key)
	} else {
		data, closer, err = db.pebble.Get(key)
	}
	return
}

func (db *_db) Set(key []byte, value []byte, opt WriteOptions, batch ...Batch) error {
	if len(batch) > 0 && batch[0] != nil {
		return batch[0].Set(key, value, opt)
	} else {
		return db.pebble.Set(key, value, pebbleWriteOptions(opt))
	}
}

func (db *_db) Delete(key []byte, opts WriteOptions, batch ...Batch) error {
	if len(batch) > 0 && batch[0] != nil {
		return batch[0].Delete(key, opts)
	} else {
		return db.pebble.Delete(key, pebbleWriteOptions(opts))
	}
}

func (db *_db) DeleteRange(start []byte, end []byte, opt WriteOptions, batch ...Batch) error {
	if len(batch) > 0 && batch[0] != nil {
		return batch[0].DeleteRange(start, end, opt)
	} else {
		return db.pebble.DeleteRange(start, end, pebbleWriteOptions(opt))
	}
}

func (db *_db) Iter(opt *IterOptions, batch ...Batch) Iterator {
	if len(batch) > 0 && batch[0] != nil {
		return batch[0].Iter(opt)
	} else {
		return newIterator(&_bondIterConstructor{pebbleConstructor: db.pebble}, opt)
	}
}

func (db *_db) Batch() Batch {
	return newBatch(db)
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

func (db *_db) getKeyBufferPool() *utils.PreAllocatedPool[[]byte] {
	return db.keyBufferPool
}

func (db *_db) getMultiKeyBufferPool() *utils.PreAllocatedPool[[]byte] {
	return db.multiKeyBufferPool
}

func (db *_db) getValueBufferPool() *utils.PreAllocatedPool[[]byte] {
	return db.valueBufferPool
}

func (db *_db) getBytesArrayPool() *utils.PreAllocatedPool[[][]byte] {
	return db.byteArraysPool
}

func (db *_db) getKeyArray(numOfKeys int) [][]byte {
	keys := db.byteArraysPool.Get()[:0]
	if cap(keys) < numOfKeys {
		keys = make([][]byte, 0, numOfKeys)
	}

	for i := 0; i < numOfKeys; i++ {
		keys = append(keys, db.keyBufferPool.Get()[:0])
	}
	return keys
}

func (db *_db) putKeyArray(arr [][]byte) {
	for _, key := range arr {
		db.keyBufferPool.Put(key[:0])
	}
	db.byteArraysPool.Put(arr[:0])
}

func (db *_db) getValueArray(numOfValues int) [][]byte {
	keys := db.byteArraysPool.Get()[:0]
	if cap(keys) < numOfValues {
		keys = make([][]byte, 0, numOfValues)
	}

	for i := 0; i < numOfValues; i++ {
		keys = append(keys, db.valueBufferPool.Get()[:0])
	}
	return keys
}

func (db *_db) putValueArray(arr [][]byte) {
	for _, value := range arr {
		db.valueBufferPool.Put(value[:0])
	}
	db.byteArraysPool.Put(arr[:0])
}

func pebbleWriteOptions(opt WriteOptions) *pebble.WriteOptions {
	if opt == NoSync {
		return pebble.NoSync
	}
	return pebble.Sync
}
