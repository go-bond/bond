package bond

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

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

const DefaultKeyBufferSize = 512
const DefaultValueBufferSize = 1024
const DefaultNumberOfKeyBuffersInMultiKeyBuffer = 1000

const DefaultNumberOfPreAllocKeyBuffers = 2 * persistentBatchSize
const DefaultNumberOfPreAllocMultiKeyBuffers = 10
const DefaultNumberOfPreAllocValueBuffers = 10 * DefaultScanPrefetchSize
const DefaultNumberOfPreAllocBytesArrays = 50

type DB interface {
	internalPools
	catalogAuthorization() catalogAuthorization

	Backend() *pebble.DB
	Catalog() *Catalog
	Serializer() Serializer[any]
	Dir() string
	StorageCompatibility() (StorageCompatibility, error)
	StorageDiagnostics() (StorageDiagnostics, error)
	Checkpoint(dirname string) error

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
	getKeyArray(numOfKeys int) [][]byte
	putKeyArray(arr [][]byte)
	getValueArray(numOfValues int) [][]byte
	putValueArray(arr [][]byte)

	getKeyBuffer() []byte
	putKeyBuffer(key []byte)
	getMultiKeyBuffer() []byte
	putMultiKeyBuffer(key []byte)
	getValueBuffer() []byte
	putValueBuffer(value []byte)
}

type _db struct {
	dir     string
	pebble  *pebble.DB
	storage productionSchemaRegistry
	catalog *Catalog

	serializer Serializer[any]

	keyBufferPool      utils.SyncPool[[]byte]
	multiKeyBufferPool utils.SyncPool[[]byte]
	valueBufferPool    utils.SyncPool[[]byte]
	byteArraysPool     utils.SyncPool[[][]byte]

	onCloseCallbacks []func(db DB)
}

type catalogAuthorization struct {
	owner   *_db
	catalog *Catalog
}

var (
	writeOpenStorageCompatibility     = WriteStorageCompatibility
	afterOpenPreparedPebble           = func(*pebble.DB) error { return nil }
	readOpenBondInitializationPending = readBondInitializationPending
	markOpenBondInitializationPending = markBondInitializationPending
)

func Open(dirname string, opts *Options, performanceProfile ...PerformanceProfile) (opened DB, retErr error) {
	if opts == nil {
		opts = DefaultOptions(performanceProfile...)
	}
	if opts.Catalog != nil {
		if err := opts.Catalog.Validate(); err != nil {
			return nil, fmt.Errorf("bond: validate catalog before open: %w", err)
		}
	}
	if opts.PebbleOptions == nil {
		opts.PebbleOptions = DefaultPebbleOptions(performanceProfile...)
	}
	pebbleOptions, storage, err := productionPebbleOptions(opts.PebbleOptions, false)
	if err != nil {
		return nil, err
	}

	// expand the path if it is not absolute
	dirname, err = filepath.Abs(dirname)
	if err != nil {
		return nil, err
	}
	dirname, err = canonicalizeDefaultFSDestination(pebbleOptions.FS, dirname)
	if err != nil {
		return nil, fmt.Errorf("bond: resolve database destination: %w", err)
	}
	transaction, err := inspectOpenDestination(pebbleOptions.FS, dirname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if releaseErr := transaction.claim.Close(); releaseErr != nil {
			if retErr == nil && opened != nil {
				retErr = errors.Join(retErr, opened.Close())
				opened = nil
			}
			retErr = errors.Join(retErr, fmt.Errorf("bond: release database open claim: %w", releaseErr))
		}
	}()
	if transaction.preexisting && !transaction.hasManifestPointer {
		return nil, errors.New("bond: pre-existing Pebble artifacts are present without a current manifest pointer; refusing to initialize a new store")
	}
	initializationIntent, err := inspectBondInitializationIntent(dirname)
	if err != nil {
		return nil, err
	}
	if !transaction.preexisting && initializationIntent == nil {
		initializationIntent, err = createBondInitializationIntent(dirname)
		if err != nil {
			return nil, err
		}
	}
	if err := validateStorageCompatibilitySidecar(dirname); err != nil {
		return nil, err
	}

	bondPath := filepath.Join(dirname, "bond")
	pebbelVersionPath := filepath.Join(bondPath, PebbleFormatFile)
	version, err := os.ReadFile(pebbelVersionPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	needsPebbleVersionSidecar := false
	if err != nil && os.IsNotExist(err) {
		needsPebbleVersionSidecar = true
	} else {
		existingVersion, err := strconv.ParseUint(string(version), 10, 64)
		if err != nil {
			return nil, err
		}
		if existingVersion != uint64(pebbleOptions.FormatMajorVersion) {
			return nil, fmt.Errorf("the user trying to open pebble version in %d. but db is in %d",
				pebbleOptions.FormatMajorVersion,
				existingVersion)
		}
	}

	pdb, err := openPreparedPebble(dirname, pebbleOptions)
	if err != nil {
		return nil, err
	}
	if err := afterOpenPreparedPebble(pdb); err != nil {
		_ = pdb.Close()
		return nil, err
	}
	existingBondVersion, hasBondVersion, err := readBondDataVersion(pdb)
	if err != nil {
		_ = pdb.Close()
		return nil, err
	}
	if hasBondVersion && existingBondVersion != BOND_DB_DATA_VERSION {
		_ = pdb.Close()
		return nil, fmt.Errorf("bond db version is %d but expecting %d", existingBondVersion, BOND_DB_DATA_VERSION)
	}
	var reconciliation catalogReconciliation
	newStore := false
	if hasBondVersion {
		reconciliation, err = inspectCatalogDefinition(pdb, opts.Catalog)
		if err != nil {
			_ = pdb.Close()
			return nil, err
		}
	} else {
		if initializationIntent == nil {
			_ = pdb.Close()
			return nil, errors.New("bond: database version metadata is missing from a pre-existing Pebble store without a valid external initialization intent")
		}
		initializationPending, err := readOpenBondInitializationPending(pdb)
		if err != nil {
			_ = pdb.Close()
			return nil, err
		}
		if !initializationPending {
			if err := markOpenBondInitializationPending(pdb); err != nil {
				_ = pdb.Close()
				return nil, err
			}
		}
		newStore = true
	}

	var serializer Serializer[any]
	if opts.Serializer != nil {
		serializer = opts.Serializer
	} else {
		serializer = &serializers.JsonSerializer{}
	}

	db := &_db{
		dir:        dirname,
		pebble:     pdb,
		storage:    storage,
		catalog:    opts.Catalog,
		serializer: serializer,
		keyBufferPool: utils.NewPreAllocatedSyncPool[[]byte](func() any {
			return make([]byte, 0, DefaultKeyBufferSize)
		}, DefaultNumberOfPreAllocKeyBuffers),
		multiKeyBufferPool: utils.NewPreAllocatedSyncPool[[]byte](func() any {
			return make([]byte, 0, DefaultKeyBufferSize*DefaultNumberOfKeyBuffersInMultiKeyBuffer)
		}, DefaultNumberOfPreAllocMultiKeyBuffers),
		valueBufferPool: utils.NewPreAllocatedSyncPool[[]byte](func() any {
			return make([]byte, 0, DefaultValueBufferSize)
		}, DefaultNumberOfPreAllocValueBuffers),
		byteArraysPool: utils.NewPreAllocatedSyncPool[[][]byte](func() any {
			return make([][]byte, 0, persistentBatchSize)
		}, DefaultNumberOfPreAllocBytesArrays),
	}

	compatibility, err := db.StorageCompatibility()
	if err != nil {
		_ = pdb.Close()
		return nil, err
	}
	if needsPebbleVersionSidecar {
		if err := os.MkdirAll(bondPath, os.ModePerm); err != nil {
			_ = pdb.Close()
			return nil, err
		}
		if err := utils.WriteFileWithSync(
			pebbelVersionPath,
			[]byte(fmt.Sprintf("%d", pebbleOptions.FormatMajorVersion)),
			os.ModePerm,
		); err != nil {
			_ = pdb.Close()
			return nil, err
		}
	}
	if err := writeOpenStorageCompatibility(dirname, compatibility); err != nil {
		_ = pdb.Close()
		return nil, err
	}
	if newStore {
		if err := initializeBondMetadata(pdb, opts.Catalog); err != nil {
			_ = pdb.Close()
			return nil, err
		}
	} else if err := reconciliation.commit(pdb); err != nil {
		_ = pdb.Close()
		return nil, err
	}

	return db, nil
}

func openPreparedPebble(dirname string, opts *pebble.Options) (*pebble.DB, error) {
	return pebble.Open(dirname, opts)
}

func (db *_db) Dir() string {
	return db.dir
}

func (db *_db) Backend() *pebble.DB {
	return db.pebble
}

func (db *_db) Catalog() *Catalog {
	return db.catalog
}

func (db *_db) catalogAuthorization() catalogAuthorization {
	return catalogAuthorization{owner: db, catalog: db.catalog}
}

func (db *_db) Serializer() Serializer[any] {
	return db.serializer
}

func (db *_db) StorageDiagnostics() (StorageDiagnostics, error) {
	return inspectPebbleStorage(
		db.pebble,
		db.pebble.FormatMajorVersion(),
		db.storage.active,
		db.storage.readerName,
	)
}

func (db *_db) StorageCompatibility() (StorageCompatibility, error) {
	diagnostics, err := db.StorageDiagnostics()
	if err != nil {
		return StorageCompatibility{}, err
	}
	compatibility := compatibilityFromDiagnostics(diagnostics)
	if err := ValidateStorageCompatibility(compatibility); err != nil {
		return StorageCompatibility{}, err
	}
	return compatibility, nil
}

// Checkpoint creates a Pebble checkpoint together with the storage-reader
// metadata required to validate it before a future open or restore.
func (db *_db) Checkpoint(dirname string) error {
	if err := db.pebble.Checkpoint(dirname); err != nil {
		return fmt.Errorf("pebble checkpoint: %w", err)
	}
	compatibility, err := db.StorageCompatibility()
	if err != nil {
		return err
	}
	if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
		return err
	}
	return utils.WriteFileWithSync(
		filepath.Join(dirname, "bond", PebbleFormatFile),
		[]byte(fmt.Sprintf("%d", compatibility.FormatMajor)),
		0o644,
	)
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
func (db *_db) Compact(ctx context.Context) error {
	// It is sufficient to compact till the maxKey
	// because most of the keys are less than TableID `0xff`.
	maxKey := KeyEncode(Key{
		TableID: 0xff,
		IndexID: 0xff,
	})
	return db.pebble.Compact(ctx, nil, maxKey, true)
}

func (db *_db) Close() error {
	db.notifyOnClose()

	if err := db.pebble.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	if err := db.pebble.Close(); err != nil {
		return fmt.Errorf("pebble close: %w", err)
	}

	return nil
}

func (db *_db) OnClose(f func(db DB)) {
	db.onCloseCallbacks = append(db.onCloseCallbacks, f)
}

func (db *_db) notifyOnClose() {
	for _, onClose := range db.onCloseCallbacks {
		onClose(db)
	}
}

func (db *_db) getKeyArray(numOfKeys int) [][]byte {
	keys := db.byteArraysPool.Get()
	if cap(keys) < numOfKeys {
		keys = make([][]byte, 0, numOfKeys)
	}

	for i := 0; i < numOfKeys; i++ {
		keys = append(keys, db.keyBufferPool.Get())
	}
	return keys
}

func (db *_db) putKeyArray(arr [][]byte) {
	for _, key := range arr {
		db.putKeyBuffer(key[:0])
	}
	db.byteArraysPool.Put(arr[:0])
}

func (db *_db) getValueArray(numOfValues int) [][]byte {
	keys := db.byteArraysPool.Get()
	if cap(keys) < numOfValues {
		keys = make([][]byte, 0, numOfValues)
	}

	for i := 0; i < numOfValues; i++ {
		keys = append(keys, db.valueBufferPool.Get())
	}
	return keys
}

func (db *_db) putValueArray(arr [][]byte) {
	for _, value := range arr {
		db.putValueBuffer(value)
	}
	db.byteArraysPool.Put(arr[:0])
}

// getKeyBuffer gets a key buffer from the pool.
func (db *_db) getKeyBuffer() []byte {
	return db.keyBufferPool.Get()
}

// putKeyBuffer puts the key buffer back to the pool if it is small enough.
// otherwise, discard it to avoid memory bloat.
func (db *_db) putKeyBuffer(key []byte) {
	if cap(key) <= 4*DefaultKeyBufferSize {
		db.keyBufferPool.Put(key[:0])
	}
}

// getMultiKeyBuffer gets a multi key buffer from the pool.
func (db *_db) getMultiKeyBuffer() []byte {
	return db.multiKeyBufferPool.Get()
}

// putMultiKeyBuffer puts the multi key buffer back to the pool if it is small enough.
// otherwise, discard it to avoid memory bloat.
func (db *_db) putMultiKeyBuffer(key []byte) {
	if cap(key) <= 4*DefaultKeyBufferSize*DefaultNumberOfKeyBuffersInMultiKeyBuffer {
		db.multiKeyBufferPool.Put(key[:0])
	}
}

// getValueBuffer gets a value buffer from the pool.
func (db *_db) getValueBuffer() []byte {
	return db.valueBufferPool.Get()
}

// putValueBuffer puts the value buffer back to the pool if it is small enough.
// otherwise, discard it to avoid memory bloat.
func (db *_db) putValueBuffer(value []byte) {
	if cap(value) <= 2*DefaultValueBufferSize {
		db.valueBufferPool.Put(value[:0])
	}
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

// iteratorToSST is used by Dump method to write all the key/value of
// iterator to the SST file.
func iteratorToSST(itr Iterator, path string) error {
	defer itr.Close()

	// sst reader
	currentFileID := 1
	file, err := vfs.Default.Create(filepath.Join(path, fmt.Sprintf("%d.sst", currentFileID)), vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}

	opts := sstable.WriterOptions{
		TableFormat: sstable.TableFormatPebblev2, Comparer: DefaultKeyComparer(),
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
	if upgradeVersion != uint64(PebbleDBFormat) {
		return fmt.Errorf("bond: Pebble format migrations must target FormatNewest (%d), got %d", PebbleDBFormat, upgradeVersion)
	}
	// expand the path if it is not absolute
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if err := validateStorageCompatibilitySidecar(dir); err != nil {
		return err
	}

	currentVersion, err := PebbleFormatVersion(dir)
	if err != nil {
		return err
	}
	if currentVersion > upgradeVersion {
		return fmt.Errorf("cannot downgrade pebble format from %d to %d", currentVersion, upgradeVersion)
	}

	opt, storage, err := productionPebbleOptions(DefaultPebbleOptions(), false)
	if err != nil {
		return err
	}
	db, err := openPreparedPebble(dir, opt)
	if err != nil {
		return err
	}
	defer db.Close()

	actualVersion := db.FormatMajorVersion()
	if actualVersion != pebble.FormatMajorVersion(upgradeVersion) {
		return fmt.Errorf("pebble format migration requested %d but opened at %d", upgradeVersion, actualVersion)
	}
	if err := utils.ReplaceFileWithSync(
		filepath.Join(dir, "bond", PebbleFormatFile),
		[]byte(fmt.Sprintf("%d", actualVersion)),
		os.ModePerm,
	); err != nil {
		return err
	}
	diagnostics, err := inspectPebbleStorage(db, actualVersion, storage.active, storage.readerName)
	if err != nil {
		return err
	}
	return WriteStorageCompatibility(dir, compatibilityFromDiagnostics(diagnostics))
}

// InspectStorageDirectory reads SST properties from an offline Bond directory
// without opening the database or initializing key seekers.
func InspectStorageDirectory(dirname string) (StorageDiagnostics, error) {
	dirname, err := filepath.Abs(dirname)
	if err != nil {
		return StorageDiagnostics{}, err
	}
	version, err := PebbleFormatVersion(dirname)
	if err != nil {
		return StorageDiagnostics{}, err
	}
	if version < uint64(pebble.FormatMinSupported) || version > uint64(PebbleDBFormat) {
		return StorageDiagnostics{}, fmt.Errorf(
			"bond: inspect supports Pebble formats %d through %d, database sidecar records %d",
			pebble.FormatMinSupported, PebbleDBFormat, version,
		)
	}
	storage := newProductionSchemaRegistry(DefaultKeyComparer())
	return inspectPebbleStorageFiles(
		dirname, pebble.FormatMajorVersion(version), storage.active, storage.readerName,
	)
}

func validateStorageCompatibilitySidecar(dirname string) error {
	compatibility, err := ReadStorageCompatibility(dirname)
	if err != nil {
		return err
	}
	if compatibility == nil {
		return nil
	}
	return ValidateStorageCompatibility(*compatibility)
}

// StringToBytes converts a string to a byte slice without copying.
// IMPORTANT: The returned byte slice must NOT be modified, as this will
// corrupt the original string. Only use this for READ-ONLY operations.
func StringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToString converts a byte slice to a string without copying.
// IMPORTANT: The original byte slice should not be modified after this conversion.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
