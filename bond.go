package bond

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-bond/bond/serializers"
	"github.com/go-bond/bond/utils"
	"golang.org/x/sync/errgroup"
)

const (
	// BOND_DB_DATA_TABLE_ID ..
	BOND_DB_DATA_TABLE_ID = 0x0

	// BOND_DB_DATA_USER_SPACE_INDEX_ID
	BOND_DB_DATA_USER_SPACE_INDEX_ID = 0xFF
)

const exportFileSize = 17 << 20 // 17 MB

const PebbleFormatFile = "PEBBLE_FORMAT_VERSION"

var (
	ErrNotFound = fmt.Errorf("bond: not found")
)

const DefaultKeyBufferSize = 2048
const DefaultValueBufferSize = 2048
const DefaultNumberOfKeyBuffersInMultiKeyBuffer = 1000

const DefaultNumberOfPreAllocKeyBuffers = 2 * persistentBatchSize
const DefaultNumberOfPreAllocMultiKeyBuffers = 10
const DefaultNumberOfPreAllocValueBuffers = 10 * DefaultScanPrefetchSize
const DefaultNumberOfPreAllocBytesArrays = 50

type DB interface {
	internalPools

	Backend() *pebble.DB
	Serializer() Serializer[any]

	Getter
	Setter
	Deleter
	DeleterWithRange
	Iterable

	Batcher
	Applier

	Closer
	Backup

	OnClose(func(db DB))
}

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
	Batch(bType BatchType) Batch
}

type Iterable interface {
	Iter(opt *IterOptions, batch ...Batch) Iterator
}

type Applier interface {
	Apply(b Batch, opt WriteOptions) error
}

type Backup interface {
	Dump(ctx context.Context, dir string, tables []TableID, withIndex bool) error
	Restore(ctx context.Context, dir string, tables []TableID, withIndex bool) error
}

type Closer io.Closer

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

	// expand the path if it is not absolute
	dirname, err := filepath.Abs(dirname)
	if err != nil {
		return nil, err
	}

	bondPath := filepath.Join(dirname, "bond")
	_, err = os.Stat(bondPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err != nil && os.IsNotExist(err) {
		// create dir if db dir didn't exit.
		if err := os.MkdirAll(bondPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	pebbelVersionPath := filepath.Join(bondPath, PebbleFormatFile)
	// retive the pebble version.
	version, err := os.ReadFile(pebbelVersionPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if err != nil && os.IsNotExist(err) {
		// create version for to check invariant in the
		// next open.
		if err := os.WriteFile(pebbelVersionPath,
			[]byte(fmt.Sprintf("%d", opts.PebbleOptions.FormatMajorVersion)),
			os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		existingVersion, err := strconv.ParseUint(string(version), 10, 64)
		if err != nil {
			return nil, err
		}
		if existingVersion != uint64(opts.PebbleOptions.FormatMajorVersion) {
			return nil, fmt.Errorf("the user trying to open pebble version in %d. but db is in %d",
				opts.PebbleOptions.FormatMajorVersion,
				existingVersion)
		}
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

func (db *_db) Batch(bType BatchType) Batch {
	if bType == BatchTypeWriteOnly {
		return newBatch(db, false)
	}
	return newBatch(db, true)
}

func (db *_db) BatchReadWrite() Batch {
	return newBatch(db, true)
}

func (db *_db) Apply(b Batch, opt WriteOptions) error {
	return b.Commit(opt)
}

// Compact the entire bond database
func (db *_db) Compact() error {
	// It is sufficient to compact till the maxKey
	// because most of the keys are less than TableID `0xff`.
	maxKey := KeyEncode(Key{
		TableID: 0xff,
		IndexID: 0xff,
	})
	return db.pebble.Compact(nil, maxKey, true)
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

func (db *_db) Dump(_ context.Context, path string, tables []TableID, withIndex bool) error {
	if err := os.Mkdir(path, 0755); err != nil {
		return err
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], BOND_DB_DATA_VERSION)
	if err := os.WriteFile(filepath.Join(path, "VERSION"), buf[:], 0755); err != nil {
		return err
	}

	snapshot := db.pebble.NewSnapshot()
	defer snapshot.Close()

	grp := new(errgroup.Group)

	// write all the table data to the sst file.
	for _, tableID := range tables {
		tablePath := filepath.Join(path, fmt.Sprintf("table_%d", tableID))
		if err := os.Mkdir(tablePath, 0755); err != nil {
			return err
		}
		itr, err := snapshot.NewIter(
			&pebble.IterOptions{
				LowerBound: []byte{byte(tableID), 0x00, 0x00, 0x00, 0x00, 0x00},
				UpperBound: []byte{byte(tableID), 0x01, 0x00, 0x00, 0x00, 0x00},
			},
		)
		if err != nil {
			return err
		}
		grp.Go(func(itr Iterator, path string) func() error {
			return func() error {
				return iteratorToSST(itr, path)
			}
		}(itr, tablePath))

		if !withIndex {
			continue
		}

		// write all the index data to sst file.
		indexes := db.getIndexIDS(tableID)
		for _, index := range indexes {
			indexPath := filepath.Join(path, fmt.Sprintf("table_%d_index_%d", tableID, index))
			if err := os.Mkdir(indexPath, 0755); err != nil {
				return err
			}
			itr, err := snapshot.NewIter(&pebble.IterOptions{
				LowerBound: []byte{byte(tableID), byte(index), 0x00, 0x00, 0x00, 0x00},
				UpperBound: []byte{byte(tableID), byte(index), 0xff, 0xff, 0xff, 0xff},
			},
			)
			if err != nil {
				return err
			}
			grp.Go(func(itr Iterator, path string) func() error {
				return func() error {
					return iteratorToSST(itr, path)
				}
			}(itr, indexPath))
		}
	}
	return grp.Wait()
}

func (db *_db) Restore(ctx context.Context, path string, tables []TableID, withIndex bool) error {
	buf, err := os.ReadFile(filepath.Join(path, "VERSION"))
	if err != nil {
		return err
	}
	if len(buf) < 4 {
		return fmt.Errorf("invalid VERSION file")
	}
	version := binary.BigEndian.Uint32(buf)
	if version != BOND_DB_DATA_VERSION {
		return fmt.Errorf("expecting version %d to restore, but found %d", BOND_DB_DATA_VERSION, version)
	}

	// The table directory, must be present for the bond to restore. return an error if it
	// doesn't exist
	for _, table := range tables {
		tableDir := filepath.Join(path, fmt.Sprintf("table_%d", table))
		_, err := os.Stat(tableDir)
		if err != nil {
			return err
		}
	}

	// ingest the required sst file.
	ssts := []string{}
	err = filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".sst" {
			return nil
		}
		// filter only relevant tables.
		filter := true
		for _, table := range tables {
			if strings.Contains(path, fmt.Sprintf("table_%d", table)) {
				filter = false
				break
			}
		}

		if filter {
			return nil
		}

		if !strings.Contains(path, "index") {
			ssts = append(ssts, path)
			return nil
		}
		if withIndex {
			ssts = append(ssts, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return db.pebble.Ingest(ctx, ssts)
}

func (db *_db) getIndexIDS(tableID TableID) []IndexID {
	prefix := []byte{byte(tableID), 1}
	indexIDS := []IndexID{}

	itr := db.Iter(&IterOptions{})
	for {
		if !itr.SeekGE(prefix) {
			break
		}
		indexTableID := TableID(itr.Key()[0])
		indexID := itr.Key()[1]
		if indexTableID != tableID {
			break
		}
		indexIDS = append(indexIDS, IndexID(indexID))
		if indexID == math.MaxUint8 {
			break
		}
		prefix[1] = indexID + 1
	}
	itr.Close()
	return indexIDS
}

// write all the key/value of iterator to the SST file.
func iteratorToSST(itr Iterator, path string) error {
	defer itr.Close()

	// sst reader
	currentFileID := 1
	file, err := vfs.Default.Create(filepath.Join(path, fmt.Sprintf("%d.sst", currentFileID)), vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}

	opts := sstable.WriterOptions{
		TableFormat: sstable.TableFormatPebblev2, Parallelism: true, Comparer: DefaultKeyComparer(),
	}
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(file), opts)

	for itr.First(); itr.Valid(); itr.Next() {
		if err := writer.Set(itr.Key(), itr.Value()); err != nil {
			writer.Close()
			return err
		}

		// Replace the old writer with new writer after the old writer reaches it's capacity.
		if writer.Raw().EstimatedSize() > exportFileSize {
			if err := writer.Close(); err != nil {
				return err
			}
			currentFileID++
			file, err = vfs.Default.Create(filepath.Join(path, fmt.Sprintf("%d.sst", currentFileID)), vfs.WriteCategoryUnspecified)
			if err != nil {
				return err
			}
			writer = sstable.NewWriter(objstorageprovider.NewFileWritable(file), opts)
		}
	}
	return writer.Close()
}

func pebbleWriteOptions(opt WriteOptions) *pebble.WriteOptions {
	if opt == NoSync {
		return pebble.NoSync
	}
	return pebble.Sync
}

func PebbleFormatVersion(dir string) (uint64, error) {
	pebbelVersionPath := filepath.Join(dir, "bond", PebbleFormatFile)
	buf, err := os.ReadFile(pebbelVersionPath)
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	// version file is not initialized yet.
	if os.IsNotExist(err) {
		return 0, nil
	}
	version, err := strconv.ParseUint(string(buf), 10, 64)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func MigratePebbleFormatVersion(dir string, upgradeVersion uint64) error {
	opt := DefaultPebbleOptions()
	opt.FormatMajorVersion = pebble.FormatMajorVersion(upgradeVersion)

	// expand the path if it is not absolute
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	db, err := pebble.Open(dir, opt)
	if err != nil {
		return err
	}
	defer db.Close()

	versionFile, err := os.OpenFile(filepath.Join(dir, "bond", PebbleFormatFile), os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}

	defer versionFile.Close()
	_, err = versionFile.Write([]byte(fmt.Sprintf("%d", upgradeVersion)))
	return err
}
