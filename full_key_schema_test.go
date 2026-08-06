package bond

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
	"github.com/stretchr/testify/require"
)

func TestFullKeySchemaValidationAndImmutableNames(t *testing.T) {
	comparer := DefaultKeyComparer()
	for bundleSize, wantName := range map[int]string{
		16: fullkeyexperiment.NameB16,
		32: fullkeyexperiment.NameB32,
		64: fullkeyexperiment.NameB64,
	} {
		schema, err := fullkeyexperiment.New(comparer, bundleSize)
		require.NoError(t, err)
		require.Equal(t, wantName, schema.Name)
		require.Equal(t, []colblk.DataType{colblk.DataTypePrefixBytes}, schema.ColumnTypes)
		require.Equal(t, bundleSize, mustFullKeyBundleSize(t, schema.Name))
	}

	_, err := fullkeyexperiment.New(nil, 16)
	require.ErrorContains(t, err, "requires a comparer")
	for _, bundleSize := range []int{-1, 0, 1, 15, 17, 128} {
		_, err := fullkeyexperiment.New(comparer, bundleSize)
		require.ErrorContains(t, err, "must be 16, 32, or 64")
	}
}

func TestFullKeySchemaWriterComparisonAndReset(t *testing.T) {
	schema, err := fullkeyexperiment.New(DefaultKeyComparer(), 16)
	require.NoError(t, err)
	writer := schema.NewKeyWriter()
	keys := [][]byte{
		{1},
		secondaryTestKey(1, 1, []byte("index"), []byte("aa"), []byte("pk-1")),
		secondaryTestKey(1, 1, []byte("index"), []byte("aa"), []byte("pk-1")),
		secondaryTestKey(1, 1, []byte("index"), []byte("ab"), []byte("pk-2")),
		primaryTestKey(2, []byte("primary")),
	}
	slices.SortFunc(keys, bytes.Compare)

	var previous []byte
	for row, key := range keys {
		comparison := writer.ComparePrev(key)
		require.Equal(t, int32(DefaultKeyComparer().Split(key)), comparison.PrefixLen)
		if row == 0 {
			require.Equal(t, int32(1), comparison.UserKeyComparison)
		} else {
			wantCommon := testCommonPrefixLength(
				key[:DefaultKeyComparer().Split(key)],
				previous[:DefaultKeyComparer().Split(previous)],
			)
			require.Equal(t, int32(wantCommon), comparison.CommonPrefixLen)
			require.Equal(t, int32(bytes.Compare(key, previous)), comparison.UserKeyComparison)
		}
		writer.WriteKey(row, key, comparison.PrefixLen, comparison.CommonPrefixLen)
		require.True(t, bytes.Equal(key, writer.MaterializeKey(nil, row)))
		previous = key
	}

	writer.Reset()
	comparison := writer.ComparePrev([]byte{0})
	require.Equal(t, int32(1), comparison.PrefixLen)
	require.Equal(t, int32(1), comparison.UserKeyComparison)
	writer.WriteKey(0, []byte{0}, comparison.PrefixLen, comparison.CommonPrefixLen)
	require.Equal(t, []byte{0}, writer.MaterializeKey(nil, 0))
}

func TestFullKeySchemaRoundTrip(t *testing.T) {
	keys := adversarialFullKeyCorpus(0x5eed, 400)
	for _, bundleSize := range []int{16, 32, 64} {
		t.Run(fmt.Sprintf("b%d", bundleSize), func(t *testing.T) {
			schema := mustFullKeySchema(t, bundleSize)
			blockData, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
			require.NotEmpty(t, blockData)
			seeker := newFullKeySeeker(t, &schema, decoder, decodedBlock)
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+16)
			for row, key := range keys {
				got := seeker.MaterializeUserKey(&keyIter, row-1, row)
				require.True(t, bytes.Equal(key, got), "row %d: got %x, want %x", row, got, key)
				require.Equal(t, DefaultKeyComparer().Split(key), DefaultKeyComparer().Split(got))
			}
		})
	}
}

func TestFullKeySchemaOrderingAndSeek(t *testing.T) {
	keys := adversarialFullKeyCorpus(0xc0ffee, 600)
	targets := append([][]byte(nil), keys...)
	targets = append(targets,
		nil,
		[]byte{0},
		[]byte{0xff},
		secondaryTestKey(4, 7, []byte("common"), []byte("between"), []byte("missing")),
	)
	for _, key := range keys[1:] {
		if len(key) > 0 {
			targets = append(targets, append([]byte(nil), key[:len(key)-1]...))
		}
	}

	for _, bundleSize := range []int{16, 32, 64} {
		t.Run(fmt.Sprintf("b%d", bundleSize), func(t *testing.T) {
			schema := mustFullKeySchema(t, bundleSize)
			_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
			seeker := newFullKeySeeker(t, &schema, decoder, decodedBlock)
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+16)
			for _, target := range targets {
				want := slices.IndexFunc(keys, func(key []byte) bool {
					return bytes.Compare(key, target) >= 0
				})
				if want < 0 {
					want = len(keys)
				}
				for _, hint := range []struct {
					bound int
					dir   int8
				}{{-1, 0}, {len(keys) / 2, -1}, {len(keys) / 2, 1}} {
					row, equalPrefix := seeker.SeekGE(target, hint.bound, hint.dir)
					require.Equal(t, want, row, "target %x", target)
					if row == len(keys) {
						require.False(t, equalPrefix)
						continue
					}
					got := seeker.MaterializeUserKey(&keyIter, -1, row)
					require.True(t, bytes.Equal(keys[row], got), "row %d: got %x, want %x", row, got, keys[row])
					require.Equal(t, equalLogicalPrefix(DefaultKeyComparer(), target, keys[row]), equalPrefix)
				}
			}
		})
	}
}

func TestFullKeySchemaSplitAndPrefixIteration(t *testing.T) {
	for _, bundleSize := range []int{16, 32, 64} {
		t.Run(fmt.Sprintf("b%d", bundleSize), func(t *testing.T) {
			writerName, err := fullkeyexperiment.Name(bundleSize)
			require.NoError(t, err)
			db := openFullKeyTestDB(t, t.TempDir(), writerName, bundleSize)
			prefix := secondaryTestPrefix(7, 2, []byte("status:active"))
			keys := [][]byte{
				append(append([]byte(nil), prefix...), []byte("a")...),
				append(append([]byte(nil), prefix...), []byte("b")...),
				append(append([]byte(nil), prefix...), []byte("c")...),
				secondaryTestKey(7, 3, []byte("other"), nil, []byte("z")),
			}
			for i, key := range keys {
				require.NoError(t, db.Set(key, []byte{byte(i)}, pebble.NoSync))
			}
			require.NoError(t, db.Flush())

			iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixSuccessor(prefix)})
			require.NoError(t, err)
			defer iter.Close()
			var got [][]byte
			for valid := iter.SeekPrefixGE(prefix); valid; valid = iter.Next() {
				got = append(got, append([]byte(nil), iter.Key()...))
			}
			require.NoError(t, iter.Error())
			require.Equal(t, keys[:3], got)
			for _, key := range got {
				require.Equal(t, len(prefix), DefaultKeyComparer().Split(key))
			}
		})
	}
}

func TestFullKeySchemaReverseAndSyntheticSuffix(t *testing.T) {
	keys := adversarialFullKeyCorpus(99, 100)
	schema := mustFullKeySchema(t, 32)
	_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
	seeker := newFullKeySeeker(t, &schema, decoder, decodedBlock)
	var keyIter colblk.PrefixBytesIter
	keyIter.Buf = make([]byte, maxKeyLength+32)
	for row := len(keys) - 1; row >= 0; row-- {
		got := seeker.MaterializeUserKey(&keyIter, -1, row)
		require.True(t, bytes.Equal(keys[row], got), "row %d: got %x, want %x", row, got, keys[row])
	}

	suffix := []byte("synthetic-suffix")
	firstPrefixLen := DefaultKeyComparer().Split(keys[0])
	effectiveFirst := append(append([]byte(nil), keys[0][:firstPrefixLen]...), suffix...)
	for row, key := range keys {
		got := seeker.MaterializeUserKeyWithSyntheticSuffix(&keyIter, suffix, row-1, row)
		prefixLen := DefaultKeyComparer().Split(key)
		want := append(append([]byte(nil), key[:prefixLen]...), suffix...)
		require.Equal(t, want, got)
		require.Equal(t, bytes.Compare(effectiveFirst, []byte{0x80}) >= 0, seeker.IsLowerBound([]byte{0x80}, suffix))
	}

	syntheticPrefix := []byte("virtual/")
	var iter colblk.DataBlockIter
	iter.InitOnce(&schema, DefaultKeyComparer(), nil, colblk.NoTieringColumns())
	require.NoError(t, iter.Init(decoder, decodedBlock, blockiter.Transforms{
		SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(syntheticPrefix, suffix),
	}, colblk.NoTieringColumns()))
	defer iter.Close()
	row := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		prefixLen := DefaultKeyComparer().Split(keys[row])
		want := append(append(append([]byte(nil), syntheticPrefix...), keys[row][:prefixLen]...), suffix...)
		require.Equal(t, want, kv.K.UserKey)
		row++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, len(keys), row)
}

func TestFullKeySchemaDuplicateUserKeysWithInternalTrailers(t *testing.T) {
	duplicate := secondaryTestKey(3, 2, []byte("duplicate"), []byte("order"), []byte("pk"))
	keys := [][]byte{duplicate, duplicate, duplicate, append(append([]byte(nil), duplicate...), 0xff)}
	schema := mustFullKeySchema(t, 16)
	_, decoder, decodedBlock, _ := encodeFullKeyBlock(t, &schema, keys)
	var iter colblk.DataBlockIter
	iter.InitOnce(&schema, DefaultKeyComparer(), nil, colblk.NoTieringColumns())
	require.NoError(t, iter.Init(decoder, decodedBlock, blockiter.NoTransforms, colblk.NoTieringColumns()))
	defer iter.Close()
	row := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		require.Equal(t, keys[row], kv.K.UserKey)
		require.Equal(t, pebble.SeqNum(len(keys)-row), kv.K.SeqNum())
		row++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, len(keys), row)
}

func TestFullKeySchemaConcurrentSeekers(t *testing.T) {
	keys := adversarialFullKeyCorpus(1234, 800)
	schema := mustFullKeySchema(t, 64)
	_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
	seeker := newFullKeySeeker(t, &schema, decoder, decodedBlock)

	var wait sync.WaitGroup
	for worker := range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+8)
			for iteration := range 500 {
				row := (worker*97 + iteration*31) % len(keys)
				found, _ := seeker.SeekGE(keys[row], -1, 0)
				if found != row {
					t.Errorf("seek %d: got row %d, want %d", worker, found, row)
					return
				}
				if got := seeker.MaterializeUserKey(&keyIter, -1, found); !bytes.Equal(got, keys[row]) {
					t.Errorf("materialize %d: got %x, want %x", worker, got, keys[row])
					return
				}
			}
		}()
	}
	wait.Wait()
}

func TestFullKeySchemaCorruption(t *testing.T) {
	schema := mustFullKeySchema(t, 32)
	blockData, _, _, _ := encodeFullKeyBlock(t, &schema, adversarialFullKeyCorpus(44, 200))

	var metadata block.Metadata
	require.NoError(t, colblk.InitDataBlockMetadata(&schema, &metadata, blockData))
	for _, length := range []int{0, 1, 7, len(blockData) / 3, len(blockData) - 1} {
		t.Run(fmt.Sprintf("truncated-at-%d", length), func(t *testing.T) {
			var corruptMetadata block.Metadata
			err := colblk.InitDataBlockMetadata(&schema, &corruptMetadata, blockData[:length])
			require.Error(t, err)
			require.ErrorContains(t, err, "initializing data block metadata")
		})
	}

	corruptPrefixBytes := slices.Clone(blockData)
	prefixBytesOffset := fullKeyPrefixBytesPageOffset(t, &schema, corruptPrefixBytes)
	corruptPrefixBytes[prefixBytesOffset+1] = 0xff
	var corruptMetadata block.Metadata
	err := colblk.InitDataBlockMetadata(&schema, &corruptMetadata, corruptPrefixBytes)
	require.Error(t, err)
	require.ErrorContains(t, err, "initializing data block metadata")
}

func TestFullKeySchemaCorruptSSTDataBlock(t *testing.T) {
	schema := mustFullKeySchema(t, 32)
	obj := &objstorage.MemObj{}
	w := sstable.NewWriter(obj, sstable.WriterOptions{
		Comparer:    DefaultKeyComparer(),
		Compression: block.NoCompression,
		TableFormat: sstable.TableFormatPebblev5,
		KeySchema:   &schema,
		Checksum:    block.ChecksumTypeCRC32c,
	})
	require.NoError(t, w.Set([]byte("alpha"), []byte("one")))
	require.NoError(t, w.Set([]byte("omega"), []byte("two")))
	require.NoError(t, w.Close())

	readerOptions := sstable.ReaderOptions{
		Comparer:   DefaultKeyComparer(),
		KeySchemas: sstable.MakeKeySchemas(&schema),
	}
	data := slices.Clone(obj.Data())
	reader, err := sstable.NewMemReader(data, readerOptions)
	require.NoError(t, err)
	layout, err := reader.Layout()
	require.NoError(t, err)
	require.Len(t, layout.Data, 1)
	require.NoError(t, reader.Close())

	handle := layout.Data[0].Handle
	dataBlock := data[handle.Offset : handle.Offset+handle.Length]
	prefixBytesOffset := fullKeyPrefixBytesPageOffset(t, &schema, dataBlock)
	dataBlock[prefixBytesOffset+1] = 0xff
	blockTypeOffset := handle.Offset + handle.Length
	var checksummer block.Checksummer
	checksummer.Init(block.ChecksumTypeCRC32c)
	checksum := checksummer.Checksum(dataBlock, data[blockTypeOffset])
	binary.LittleEndian.PutUint32(data[blockTypeOffset+1:blockTypeOffset+5], checksum)

	reader, err = sstable.NewMemReader(data, readerOptions)
	require.NoError(t, err, "the SST footer, index, properties, and block checksum remain readable")
	defer reader.Close()
	iter, err := reader.NewIter(blockiter.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		require.ErrorContains(t, err, "initializing data block metadata")
		return
	}
	defer iter.Close()
	require.Nil(t, iter.First())
	require.ErrorContains(t, iter.Error(), "initializing data block metadata")
}

func TestExperimentalSchemaSelectionIsNotInProductionOptions(t *testing.T) {
	base, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
	require.NoError(t, err)
	require.Empty(t, base.KeySchema)
	require.Empty(t, base.KeySchemas)
	production, _, err := productionPebbleOptions(base, false)
	require.NoError(t, err)
	require.Contains(t, production.KeySchema, "DefaultKeySchema(")
	require.Len(t, production.KeySchemas, 1)
	require.NotContains(t, production.KeySchemas, fullkeyexperiment.NameB16)
	require.NotContains(t, production.KeySchemas, fullkeyexperiment.NameB32)
	require.NotContains(t, production.KeySchemas, fullkeyexperiment.NameB64)

	wantReaders := []string{
		production.KeySchema,
		fullkeyexperiment.NameB16,
		fullkeyexperiment.NameB32,
		fullkeyexperiment.NameB64,
	}
	for _, name := range wantReaders {
		opts, err := experimentalPebbleOptions(name)
		require.NoError(t, err)
		require.Equal(t, name, opts.KeySchema)
		require.Len(t, opts.KeySchemas, len(wantReaders))
		require.NoError(t, opts.Validate())
	}

	_, err = experimentalPebbleOptions("bond/full-key/v1-unknown")
	require.ErrorContains(t, err, "unknown experimental Pebble writer schema")
}

func TestProductionLegacySchemaBondTableLifecycle(t *testing.T) {
	writers := []string{""}
	for _, writer := range writers {
		name := writer
		if name == "" {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			open := func() DB {
				pebbleOptions, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
				require.NoError(t, err)
				db, err := Open(directory, &Options{PebbleOptions: pebbleOptions})
				require.NoError(t, err)
				return db
			}
			bind := func(db DB) (Table[*TokenBalance], *Index[*TokenBalance]) {
				table := NewTable(TableOptions[*TokenBalance]{
					DB:        db,
					TableID:   33,
					TableName: "full_key_balances",
					TablePrimaryKeyFunc: func(builder KeyBuilder, balance *TokenBalance) []byte {
						return builder.AddUint64Field(balance.ID).Bytes()
					},
				})
				index := NewIndex(IndexOptions[*TokenBalance]{
					IndexID:   PrimaryIndexID + 1,
					IndexName: "account_address_idx",
					IndexKeyFunc: func(builder KeyBuilder, balance *TokenBalance) []byte {
						return builder.AddStringField(balance.AccountAddress).Bytes()
					},
					IndexOrderFunc: IndexOrderDefault[*TokenBalance],
				})
				require.NoError(t, table.AddIndex([]*Index[*TokenBalance]{index}))
				return table, index
			}

			db := open()
			table, index := bind(db)
			records := []*TokenBalance{
				{ID: 1, AccountID: 1, AccountAddress: "account-a", ContractAddress: "contract", Balance: 10},
				{ID: 2, AccountID: 1, AccountAddress: "account-a", ContractAddress: "contract", Balance: 20},
				{ID: 3, AccountID: 2, AccountAddress: "account-b", ContractAddress: "contract", Balance: 30},
			}
			batch := db.Batch(BatchTypeWriteOnly)
			require.NoError(t, table.Insert(context.Background(), records, batch))
			require.NoError(t, batch.Commit(NoSync))
			require.NoError(t, batch.Close())
			got, err := table.GetPoint(context.Background(), &TokenBalance{ID: 2})
			require.NoError(t, err)
			require.Equal(t, records[1], got)
			var indexed []*TokenBalance
			require.NoError(t, table.ScanIndex(
				context.Background(), index,
				NewSelectorPoint(&TokenBalance{AccountAddress: "account-a"}),
				&indexed, false,
			))
			require.Equal(t, records[:2], indexed)

			records[1].AccountAddress = "account-b"
			records[1].Balance = 25
			require.NoError(t, table.Update(context.Background(), []*TokenBalance{records[1]}))
			require.NoError(t, db.Backend().Flush())
			require.NoError(t, db.Close())

			db = open()
			table, index = bind(db)
			got, err = table.GetPoint(context.Background(), &TokenBalance{ID: 2})
			require.NoError(t, err)
			require.Equal(t, records[1], got)
			indexed = nil
			require.NoError(t, table.ScanIndex(
				context.Background(), index,
				NewSelectorPoint(&TokenBalance{AccountAddress: "account-a"}),
				&indexed, true,
			))
			require.Equal(t, []*TokenBalance{records[0]}, indexed)
			require.NoError(t, table.Delete(context.Background(), []*TokenBalance{records[0]}))
			_, err = table.GetPoint(context.Background(), &TokenBalance{ID: 1})
			require.ErrorIs(t, err, ErrNotFound)
			require.NoError(t, db.Close())
		})
	}
}

func TestProductionEmptyPointMutationsRemainAccepted(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{PebbleOptions: LowPerformancePebbleOptions()})
	require.NoError(t, err)
	defer db.Close()

	batch := db.Batch(BatchTypeWriteOnly)
	defer batch.Close()
	require.NoError(t, db.Set(nil, []byte("database-set"), NoSync, batch))
	require.NoError(t, db.Delete(nil, NoSync, batch))
	require.NoError(t, batch.Set(nil, []byte("batch"), NoSync))
	require.NoError(t, batch.Delete(nil, NoSync))
	require.False(t, batch.Empty())
	batch.Reset()
	require.True(t, batch.Empty())
}

func TestFullKeySchemaRangeDeletionsLifecycle(t *testing.T) {
	writers := []string{"", fullkeyexperiment.NameB16, fullkeyexperiment.NameB32, fullkeyexperiment.NameB64}
	for _, writer := range writers {
		name := writer
		if name == "" {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			open := func() *pebble.DB {
				bundleSize := 16
				if writer != "" {
					bundleSize = mustFullKeyBundleSize(t, writer)
				}
				return openFullKeyTestDB(t, directory, writer, bundleSize)
			}

			allKeys := [][]byte{{0}, []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("z")}
			oracle := make(map[string][]byte, len(allKeys))
			db := open()
			for i, key := range allKeys {
				value := []byte(fmt.Sprintf("value-%d", i))
				require.NoError(t, db.Set(key, value, pebble.NoSync))
				oracle[string(key)] = value
			}
			require.NoError(t, db.Flush())

			require.NoError(t, db.DeleteRange([]byte{0}, []byte("b"), pebble.NoSync))
			require.NoError(t, db.DeleteRange([]byte("c"), []byte("e"), pebble.NoSync))
			require.NoError(t, db.Flush())
			delete(oracle, "\x00")
			delete(oracle, "a")
			delete(oracle, "c")
			delete(oracle, "d")
			verifyDBOracle(t, db, oracle)
			verifyMissingKeys(t, db, allKeys, oracle)
			require.NoError(t, db.Close())

			db = open()
			verifyDBOracle(t, db, oracle)
			require.NoError(t, db.DeleteRange([]byte("b"), []byte("f"), pebble.NoSync))
			require.NoError(t, db.Flush())
			delete(oracle, "b")
			delete(oracle, "e")
			verifyDBOracle(t, db, oracle)
			verifyMissingKeys(t, db, allKeys, oracle)

			require.NoError(t, db.Compact(context.Background(), []byte{0}, []byte{0xff}, false))
			verifyDBOracle(t, db, oracle)
			verifyMissingKeys(t, db, allKeys, oracle)
			require.NoError(t, db.Close())

			db = open()
			verifyDBOracle(t, db, oracle)
			verifyMissingKeys(t, db, allKeys, oracle)
			require.NoError(t, db.Close())
		})
	}
}

func TestMixedSchemasReopenCompactRollback(t *testing.T) {
	directory := t.TempDir()
	oracle := make(map[string][]byte)
	writeGeneration := func(writer string, first, last int) {
		db := openFullKeyTestDB(t, directory, writer, 32)
		for i := first; i < last; i++ {
			key := secondaryTestKey(11, 3, []byte("mixed"), []byte{byte(i / 8)}, uint64Bytes(uint64(i)))
			value := []byte(fmt.Sprintf("value-%04d", i))
			require.NoError(t, db.Set(key, value, pebble.NoSync))
			oracle[string(key)] = append([]byte(nil), value...)
		}
		require.NoError(t, db.Flush())
		verifyDBOracle(t, db, oracle)
		require.NoError(t, db.Close())
	}

	writeGeneration("", 0, 80)
	writeGeneration(fullkeyexperiment.NameB32, 80, 160)

	db := openFullKeyTestDB(t, directory, fullkeyexperiment.NameB32, 32)
	require.Equal(t, map[string]int{
		"DefaultKeySchema(leveldb.BytewiseComparator,16)": 1,
		fullkeyexperiment.NameB32:                         1,
	}, schemaFileCounts(t, db))
	verifyDBOracle(t, db, oracle)
	require.NoError(t, db.Compact(context.Background(), []byte{0}, []byte{0xff}, false))
	verifyDBOracle(t, db, oracle)
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, db.Checkpoint(checkpoint))
	require.NoError(t, db.Close())

	checkpointDB := openFullKeyTestDB(t, checkpoint, "", 16)
	verifyDBOracle(t, checkpointDB, oracle)
	require.NoError(t, checkpointDB.Close())

	writeGeneration("", 160, 200)
	rolledBack := openFullKeyTestDB(t, directory, "", 16)
	verifyDBOracle(t, rolledBack, oracle)
	counts := schemaFileCounts(t, rolledBack)
	require.Positive(t, counts[fullkeyexperiment.NameB32])
	require.Positive(t, counts["DefaultKeySchema(leveldb.BytewiseComparator,16)"])
	experimentalOptions, err := experimentalPebbleOptions("")
	require.NoError(t, err)
	readers := make([]string, 0, len(experimentalOptions.KeySchemas))
	for name := range experimentalOptions.KeySchemas {
		readers = append(readers, name)
	}
	diagnostics, err := inspectPebbleStorage(
		rolledBack,
		rolledBack.FormatMajorVersion(),
		experimentalOptions.KeySchema,
		readers,
	)
	require.NoError(t, err)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 2)
	require.Empty(t, diagnostics.UnknownSchemas)
	require.Equal(t, experimentalOptions.KeySchema, diagnostics.ActiveWriter)
	productionRegistry := newProductionSchemaRegistry(DefaultKeyComparer())
	productionView, err := inspectPebbleStorage(
		rolledBack,
		rolledBack.FormatMajorVersion(),
		productionRegistry.active,
		productionRegistry.readerName,
	)
	require.NoError(t, err)
	require.Equal(t, []string{fullkeyexperiment.NameB32}, productionView.UnknownSchemas)
	require.ErrorContains(t,
		ValidateStorageCompatibility(compatibilityFromDiagnostics(diagnostics)),
		"unregistered key schemas",
	)
	require.NoError(t, rolledBack.Close())
}

func FuzzFullKeySchemaRoundTripAndSeek(f *testing.F) {
	f.Add([]byte("alpha"), []byte("omega"))
	f.Add([]byte{}, []byte{0xff})
	f.Add(secondaryTestKey(1, 2, []byte("idx"), []byte("ord"), []byte("pk")), []byte("gap"))
	f.Fuzz(func(t *testing.T, rawKeys, target []byte) {
		keys := make([][]byte, 0, 16)
		for offset := 0; offset <= len(rawKeys); {
			end := min(offset+1+(offset%31), len(rawKeys))
			if end > offset {
				keys = append(keys, append([]byte(nil), rawKeys[offset:end]...))
			}
			if end == len(rawKeys) {
				break
			}
			offset = end
		}
		if len(keys) == 0 {
			keys = append(keys, []byte{0})
		}
		slices.SortFunc(keys, bytes.Compare)
		keys = slices.CompactFunc(keys, bytes.Equal)
		for _, bundleSize := range []int{16, 32, 64} {
			schema := mustFullKeySchema(t, bundleSize)
			_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
			seeker := newFullKeySeeker(t, &schema, decoder, decodedBlock)
			row, _ := seeker.SeekGE(target, -1, 0)
			want := slices.IndexFunc(keys, func(key []byte) bool { return bytes.Compare(key, target) >= 0 })
			if want < 0 {
				want = len(keys)
			}
			require.Equal(t, want, row)
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+1)
			for i, key := range keys {
				got := seeker.MaterializeUserKey(&keyIter, -1, i)
				require.True(t, bytes.Equal(key, got), "row %d: got %x, want %x", i, got, key)
				require.Equal(t, DefaultKeyComparer().Split(key), DefaultKeyComparer().Split(got))
			}
		}
	})
}

func encodeFullKeyBlock(
	t *testing.T, schema *pebble.KeySchema, keys [][]byte,
) ([]byte, *colblk.DataBlockDecoder, colblk.BlockDecoder, int) {
	t.Helper()
	var encoder colblk.DataBlockEncoder
	encoder.Init(schema, colblk.NoTieringColumns())
	maxKeyLength := 0
	for row, key := range keys {
		comparison := encoder.KeyWriter.ComparePrev(key)
		encoder.Add(
			pebble.InternalKey{UserKey: key, Trailer: pebble.MakeInternalKeyTrailer(pebble.SeqNum(len(keys)-row), pebble.InternalKeyKindSet)},
			key,
			block.InPlaceValuePrefix(comparison.PrefixEqual()),
			comparison,
			false,
			sstable.KVMeta{},
		)
		maxKeyLength = max(maxKeyLength, len(key))
	}
	data, _ := encoder.Finish(len(keys), encoder.Size())
	decoder := &colblk.DataBlockDecoder{}
	decodedBlock := decoder.Init(schema, data)
	return data, decoder, decodedBlock, maxKeyLength
}

func newFullKeySeeker(
	t *testing.T, schema *pebble.KeySchema, decoder *colblk.DataBlockDecoder, decodedBlock colblk.BlockDecoder,
) colblk.KeySeeker {
	t.Helper()
	metadata := fullkeyexperiment.NewKeySeekerMetadata()
	schema.InitKeySeekerMetadata(metadata, decoder, decodedBlock)
	return schema.KeySeeker(metadata)
}

func fullKeyPrefixBytesPageOffset(t testing.TB, schema *pebble.KeySchema, data []byte) int {
	t.Helper()
	const blockHeaderBaseSize = 7
	const columnTypeSize = 1
	offsetPosition := int(colblk.DataBlockCustomHeaderSize+schema.HeaderSize) + blockHeaderBaseSize + columnTypeSize
	require.GreaterOrEqual(t, len(data), offsetPosition+4)
	pageOffset := int(binary.LittleEndian.Uint32(data[offsetPosition : offsetPosition+4]))
	require.Less(t, pageOffset+1, len(data))
	return pageOffset
}

func openFullKeyTestDB(t *testing.T, directory, writer string, bundleSize int) *pebble.DB {
	t.Helper()
	if writer != "" {
		require.Equal(t, bundleSize, mustFullKeyBundleSize(t, writer))
	}
	opts, err := experimentalPebbleOptions(writer)
	require.NoError(t, err)
	opts.DisableAutomaticCompactions = true
	db, err := pebble.Open(directory, opts)
	require.NoError(t, err)
	return db
}

func schemaFileCounts(t *testing.T, db *pebble.DB) map[string]int {
	t.Helper()
	levels, err := db.SSTables(pebble.WithProperties())
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, level := range levels {
		for _, table := range level {
			require.NotNil(t, table.Properties)
			counts[table.Properties.KeySchemaName]++
		}
	}
	return counts
}

func verifyDBOracle(t *testing.T, db *pebble.DB, oracle map[string][]byte) {
	t.Helper()
	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	keys := make([]string, 0, len(oracle))
	for key := range oracle {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	row := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		require.Less(t, row, len(keys))
		require.True(t, bytes.Equal([]byte(keys[row]), iter.Key()), "row %d: got %x, want %x", row, iter.Key(), keys[row])
		require.Equal(t, oracle[keys[row]], iter.Value())
		row++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, len(keys), row)

	row = len(keys) - 1
	for valid := iter.Last(); valid; valid = iter.Prev() {
		require.GreaterOrEqual(t, row, 0)
		require.True(t, bytes.Equal([]byte(keys[row]), iter.Key()), "row %d: got %x, want %x", row, iter.Key(), keys[row])
		row--
	}
	require.Equal(t, -1, row)
}

func verifyMissingKeys(t *testing.T, db *pebble.DB, allKeys [][]byte, oracle map[string][]byte) {
	t.Helper()
	for _, key := range allKeys {
		value, closer, err := db.Get(key)
		if want, ok := oracle[string(key)]; ok {
			require.NoError(t, err, "key %x", key)
			require.Equal(t, want, value, "key %x", key)
			require.NoError(t, closer.Close())
			continue
		}
		require.ErrorIs(t, err, pebble.ErrNotFound, "key %x", key)
		require.Nil(t, closer)
	}
}

func adversarialFullKeyCorpus(seed int64, randomKeys int) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	keys := [][]byte{
		{0},
		{1},
		{1, 0},
		{1, 1},
		{1, 1, 0, 0, 0},
		{1, 1, 0, 0, 0, 9, 'x'},
		primaryTestKey(1, nil),
		primaryTestKey(1, []byte("pk")),
		secondaryTestKey(1, 1, nil, nil, nil),
		secondaryTestKey(1, 1, []byte("common"), nil, []byte("pk")),
		secondaryTestKey(1, 1, []byte("common"), []byte("order"), []byte("pk")),
		secondaryTestKey(255, 255, bytes.Repeat([]byte{'x'}, 256), bytes.Repeat([]byte{'y'}, 256), bytes.Repeat([]byte{'z'}, 256)),
	}
	for range randomKeys {
		length := 1 + rng.Intn(160)
		key := make([]byte, length)
		_, _ = rng.Read(key)
		keys = append(keys, key)
	}
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}

func primaryTestKey(table byte, primary []byte) []byte {
	return append([]byte{table, byte(PrimaryIndexID)}, primary...)
}

func secondaryTestPrefix(table, index byte, indexValue []byte) []byte {
	key := []byte{table, index, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(key[2:6], uint32(len(indexValue)))
	return append(key, indexValue...)
}

func secondaryTestKey(table, index byte, indexValue, order, primary []byte) []byte {
	key := secondaryTestPrefix(table, index, indexValue)
	var orderLength [4]byte
	binary.BigEndian.PutUint32(orderLength[:], uint32(len(order)))
	key = append(key, orderLength[:]...)
	key = append(key, order...)
	return append(key, primary...)
}

func prefixSuccessor(prefix []byte) []byte {
	successor := append([]byte(nil), prefix...)
	for i := len(successor) - 1; i >= 0; i-- {
		if successor[i] != 0xff {
			successor[i]++
			return successor[:i+1]
		}
	}
	return nil
}

func uint64Bytes(value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded[:]
}

func equalLogicalPrefix(comparer *pebble.Comparer, a, b []byte) bool {
	return bytes.Equal(a[:comparer.Split(a)], b[:comparer.Split(b)])
}

func mustFullKeySchema(t testing.TB, bundleSize int) pebble.KeySchema {
	t.Helper()
	schema, err := fullkeyexperiment.New(DefaultKeyComparer(), bundleSize)
	require.NoError(t, err)
	return schema
}

func mustFullKeyBundleSize(t testing.TB, name string) int {
	t.Helper()
	bundleSize, ok := fullkeyexperiment.BundleSize(name)
	require.True(t, ok)
	return bundleSize
}

func experimentalPebbleOptions(writer string) (*pebble.Options, error) {
	opts, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
	if err != nil {
		return nil, err
	}
	if err := fullkeyexperiment.Configure(opts, writer); err != nil {
		return nil, err
	}
	return opts, nil
}

func testCommonPrefixLength(a, b []byte) int {
	limit := min(len(a), len(b))
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
}

func TestFullKeySchemaDataBlockForwardReverse(t *testing.T) {
	keys := adversarialFullKeyCorpus(7, 100)
	schema := mustFullKeySchema(t, 16)
	_, decoder, decodedBlock, _ := encodeFullKeyBlock(t, &schema, keys)
	var iter colblk.DataBlockIter
	iter.InitOnce(&schema, DefaultKeyComparer(), nil, colblk.NoTieringColumns())
	require.NoError(t, iter.Init(decoder, decodedBlock, blockiter.NoTransforms, colblk.NoTieringColumns()))
	defer iter.Close()

	row := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		require.True(t, bytes.Equal(keys[row], kv.K.UserKey), "row %d: got %x, want %x", row, kv.K.UserKey, keys[row])
		row++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, len(keys), row)

	row = len(keys) - 1
	for kv := iter.Last(); kv != nil; kv = iter.Prev() {
		require.True(t, bytes.Equal(keys[row], kv.K.UserKey), "row %d: got %x, want %x", row, kv.K.UserKey, keys[row])
		row--
	}
	require.NoError(t, iter.Error())
	require.Equal(t, -1, row)
}
