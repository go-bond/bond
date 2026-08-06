package bond

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/go-bond/bond/internal/typedschemaexperiment"
	"github.com/stretchr/testify/require"
)

type typedFamilyTestCase struct {
	name    string
	family  typedschemaexperiment.Family
	primary func(int) []byte
}

func typedFamilyTestCases() []typedFamilyTestCase {
	return []typedFamilyTestCase{
		{
			name:   typedschemaexperiment.NameUint64,
			family: typedschemaexperiment.FamilyUint64,
			primary: func(row int) []byte {
				return NewKeyBuilder(nil).AddUint64Field(uint64(row*7919 + 17)).Bytes()
			},
		},
		{
			name:   typedschemaexperiment.NameUint32,
			family: typedschemaexperiment.FamilyUint32,
			primary: func(row int) []byte {
				return NewKeyBuilder(nil).AddUint32Field(uint32(row*3571 + 11)).Bytes()
			},
		},
		{
			name:   typedschemaexperiment.NameBytes,
			family: typedschemaexperiment.FamilyBytes,
			primary: func(row int) []byte {
				var encoded [16]byte
				binary.BigEndian.PutUint64(encoded[:8], uint64(row))
				binary.BigEndian.PutUint64(encoded[8:], uint64(row*104729+23))
				return NewKeyBuilder(nil).AddBytesField(encoded[:]).Bytes()
			},
		},
	}
}

func TestTypedFamilyValidationAndImmutableNames(t *testing.T) {
	for _, tc := range typedFamilyTestCases() {
		schema, err := typedschemaexperiment.New(DefaultKeyComparer(), tc.family)
		require.NoError(t, err)
		require.Equal(t, tc.name, schema.Name)
		require.Len(t, schema.ColumnTypes, 3)
		require.Equal(t, colblk.DataTypePrefixBytes, schema.ColumnTypes[0])
		require.Equal(t, colblk.DataTypeBytes, schema.ColumnTypes[1])
		if tc.family == typedschemaexperiment.FamilyBytes {
			require.Equal(t, colblk.DataTypeBytes, schema.ColumnTypes[2])
		} else {
			require.Equal(t, colblk.DataTypeUint, schema.ColumnTypes[2])
		}
	}

	_, err := typedschemaexperiment.New(nil, typedschemaexperiment.FamilyUint64)
	require.ErrorContains(t, err, "requires a comparer")
	_, err = typedschemaexperiment.New(DefaultKeyComparer(), typedschemaexperiment.Family(255))
	require.ErrorContains(t, err, "unknown typed key family")
	require.False(t, typedschemaexperiment.IsName("bond/pk-u64/v2"))

	opts := BuildPebbleOptions(LowPerformance)
	require.ErrorContains(t, typedschemaexperiment.Configure(opts, "bond/pk-u64/v2"), "unknown experimental typed")
}

func TestTypedFamilyRoundTrip(t *testing.T) {
	for _, tc := range typedFamilyTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			keys := typedFamilyCorpus(tc, 160)
			schema := mustTypedFamilySchema(t, tc.family)
			_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
			seeker := newTypedFamilySeeker(&schema, decoder, decodedBlock)
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+16)
			for row, key := range keys {
				got := seeker.MaterializeUserKey(&keyIter, row-1, row)
				require.Equal(t, key, got, "row %d", row)
				require.Equal(t, DefaultKeyComparer().Split(key), DefaultKeyComparer().Split(got))
			}
		})
	}
}

func TestTypedFamilyOrderingAndSeek(t *testing.T) {
	for _, tc := range typedFamilyTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			keys := typedFamilyCorpus(tc, 240)
			schema := mustTypedFamilySchema(t, tc.family)
			_, decoder, decodedBlock, _ := encodeFullKeyBlock(t, &schema, keys)
			seeker := newTypedFamilySeeker(&schema, decoder, decodedBlock)

			targets := [][]byte{
				nil,
				{0},
				{1},
				{1, 1},
				{1, 1, 0, 0, 0},
				{1, 1, 0, 0, 0, 4, 'g', 'a', 'p'},
				{0xff},
			}
			targets = append(targets, keys...)
			for _, key := range keys[1:] {
				targets = append(targets, bytes.Clone(key[:len(key)-1]))
			}

			for _, target := range targets {
				want := sort.Search(len(keys), func(row int) bool {
					return bytes.Compare(keys[row], target) >= 0
				})
				row, equalPrefix := seeker.SeekGE(target, 0, 0)
				require.Equal(t, want, row, "target %x", target)
				wantEqualPrefix := want < len(keys) && equalLogicalPrefix(DefaultKeyComparer(), keys[want], target)
				require.Equal(t, wantEqualPrefix, equalPrefix, "target %x", target)
			}

			first := keys[0]
			require.Equal(t, bytes.Compare(first, nil) >= 0, seeker.IsLowerBound(nil, nil))
			prefixLen := DefaultKeyComparer().Split(first)
			syntheticSuffix := []byte("synthetic")
			effectiveFirst := append(bytes.Clone(first[:prefixLen]), syntheticSuffix...)
			require.Equal(
				t, bytes.Compare(effectiveFirst, first) >= 0,
				seeker.IsLowerBound(first, syntheticSuffix),
			)
		})
	}
}

func TestTypedFamilyDataBlockForwardReverse(t *testing.T) {
	for _, tc := range typedFamilyTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			keys := typedFamilyCorpus(tc, 100)
			schema := mustTypedFamilySchema(t, tc.family)
			_, decoder, decodedBlock, _ := encodeFullKeyBlock(t, &schema, keys)
			var iter colblk.DataBlockIter
			iter.InitOnce(&schema, DefaultKeyComparer(), nil, colblk.NoTieringColumns())
			require.NoError(t, iter.Init(decoder, decodedBlock, blockiter.NoTransforms, colblk.NoTieringColumns()))
			defer iter.Close()

			row := 0
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				require.Equal(t, keys[row], kv.K.UserKey)
				row++
			}
			require.NoError(t, iter.Error())
			require.Equal(t, len(keys), row)

			row = len(keys) - 1
			for kv := iter.Last(); kv != nil; kv = iter.Prev() {
				require.Equal(t, keys[row], kv.K.UserKey)
				row--
			}
			require.NoError(t, iter.Error())
			require.Equal(t, -1, row)
		})
	}
}

func TestTypedFamilyConcurrentSeekers(t *testing.T) {
	tc := typedFamilyTestCases()[0]
	keys := typedFamilyCorpus(tc, 300)
	schema := mustTypedFamilySchema(t, tc.family)
	_, decoder, decodedBlock, maxKeyLength := encodeFullKeyBlock(t, &schema, keys)
	seekers := []colblk.KeySeeker{
		newTypedFamilySeeker(&schema, decoder, decodedBlock),
		newTypedFamilySeeker(&schema, decoder, decodedBlock),
	}

	var wait sync.WaitGroup
	errors := make(chan error, len(seekers))
	for worker, seeker := range seekers {
		wait.Add(1)
		go func(worker int, seeker colblk.KeySeeker) {
			defer wait.Done()
			var keyIter colblk.PrefixBytesIter
			keyIter.Buf = make([]byte, maxKeyLength+16)
			for row := worker; row < len(keys); row += len(seekers) {
				found, _ := seeker.SeekGE(keys[row], 0, 0)
				if found != row {
					errors <- fmt.Errorf("worker %d seek row %d returned %d", worker, row, found)
					return
				}
				if got := seeker.MaterializeUserKey(&keyIter, -1, row); !bytes.Equal(got, keys[row]) {
					errors <- fmt.Errorf("worker %d row %d materialized %x", worker, row, got)
					return
				}
			}
		}(worker, seeker)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestTypedFamilyRangeBoundsReopen(t *testing.T) {
	for _, tc := range typedFamilyTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			open := func() *pebble.DB {
				opts, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
				require.NoError(t, err)
				require.NoError(t, typedschemaexperiment.Configure(opts, tc.name))
				opts.DisableAutomaticCompactions = true
				db, err := pebble.Open(directory, opts)
				require.NoError(t, err)
				return db
			}

			keys := typedFamilyCorpus(tc, 80)
			db := open()
			for row, key := range keys {
				require.NoError(t, db.Set(key, binary.BigEndian.AppendUint64(nil, uint64(row)), pebble.NoSync))
			}
			// These deliberately short bounds cover the complete table-1/index-1
			// range and are not themselves typed point keys.
			require.NoError(t, db.DeleteRange([]byte{1, 1}, []byte{1, 2}, pebble.NoSync))
			require.NoError(t, db.Flush())
			require.NoError(t, db.Compact(context.Background(), []byte{0}, []byte{0xff}, true))
			require.NoError(t, db.Close())

			db = open()
			defer db.Close()
			iter, err := db.NewIter(nil)
			require.NoError(t, err)
			defer iter.Close()
			var got [][]byte
			for valid := iter.First(); valid; valid = iter.Next() {
				got = append(got, bytes.Clone(iter.Key()))
			}
			require.NoError(t, iter.Error())
			var want [][]byte
			for _, key := range keys {
				if bytes.Compare(key, []byte{1, 1}) < 0 || bytes.Compare(key, []byte{1, 2}) >= 0 {
					want = append(want, key)
				}
			}
			require.Equal(t, want, got)
		})
	}
}

func TestTypedSchemaSelectionIsNotInProductionOptions(t *testing.T) {
	opts, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
	require.NoError(t, err)
	prepared, _, err := productionPebbleOptions(opts, false)
	require.NoError(t, err)
	require.Equal(t, DefaultKeySchemaName(), prepared.KeySchema)
	for _, name := range []string{
		typedschemaexperiment.NameUint64,
		typedschemaexperiment.NameUint32,
		typedschemaexperiment.NameBytes,
	} {
		require.NotContains(t, prepared.KeySchemas, name)
	}
}

func TestStockPebbleRangeSchemaCapability(t *testing.T) {
	spanPolicyType := reflect.TypeOf(pebble.SpanPolicy{})
	for _, unsupportedField := range []string{"KeySchema", "KeySchemaName", "WriterSchema"} {
		_, exists := spanPolicyType.FieldByName(unsupportedField)
		require.Falsef(t, exists, "stock Pebble unexpectedly exposes SpanPolicy.%s; re-audit routing", unsupportedField)
	}

	opts, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: LowPerformance})
	require.NoError(t, err)
	require.NoError(t, typedschemaexperiment.Configure(opts, typedschemaexperiment.NameUint64))
	opts.SpanPolicyFunc = func(bounds pebble.UserKeyBounds) (pebble.SpanPolicy, error) {
		return pebble.SpanPolicy{KeyRange: pebble.KeyRange{Start: bounds.Start, End: []byte{2}}}, nil
	}
	policy, err := opts.SpanPolicyFunc(pebble.UserKeyBounds{Start: []byte{1}})
	require.NoError(t, err)
	require.Equal(t, []byte{2}, policy.KeyRange.End)
	writerOptions := opts.MakeWriterOptions(0, pebble.FormatNewest.MaxTableFormat())
	require.NotNil(t, writerOptions.KeySchema)
	require.Equal(t, opts.KeySchema, writerOptions.KeySchema.Name)
	require.Equal(t, typedschemaexperiment.NameUint64, writerOptions.KeySchema.Name)
}

func typedFamilyCorpus(tc typedFamilyTestCase, rows int) [][]byte {
	rng := rand.New(rand.NewSource(20260806 + int64(tc.family)))
	keys := make([][]byte, 0, rows*4+4)
	for row := range rows {
		primary := tc.primary(row)
		keys = append(keys, KeyEncode(Key{TableID: 1, PrimaryKey: primary}))
		for index := 1; index <= 3; index++ {
			indexValue := NewKeyBuilder(nil).
				AddStringField(fmt.Sprintf("index-%d/%02d", index, row%17)).
				AddUint32Field(rng.Uint32()).Bytes()
			var order []byte
			if index != 1 {
				order = NewKeyBuilder(nil).AddUint64Field(uint64(row*100 + index)).Bytes()
			}
			keys = append(keys, KeyEncode(Key{
				TableID: 1, IndexID: IndexID(index), Index: indexValue,
				IndexOrder: order, PrimaryKey: primary,
			}))
		}
	}
	// Exercise the total opaque path for primary rows, table/index prefix keys,
	// and a secondary row whose primary shape belongs to another family.
	opaquePrimary := NewKeyBuilder(nil).AddStringField("tenant").AddUint64Field(9).Bytes()
	keys = append(keys,
		KeyEncode(Key{TableID: 1, PrimaryKey: opaquePrimary}),
		KeyEncode(Key{TableID: 1, IndexID: 4, Index: []byte("prefix")}.ToKeyPrefix()),
		KeyEncode(Key{TableID: 1, IndexID: 4, Index: []byte("opaque"), PrimaryKey: opaquePrimary}),
	)
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}

func mustTypedFamilySchema(t testing.TB, family typedschemaexperiment.Family) pebble.KeySchema {
	t.Helper()
	schema, err := typedschemaexperiment.New(DefaultKeyComparer(), family)
	require.NoError(t, err)
	return schema
}

func newTypedFamilySeeker(
	schema *pebble.KeySchema, decoder *colblk.DataBlockDecoder, block colblk.BlockDecoder,
) colblk.KeySeeker {
	metadata := typedschemaexperiment.NewKeySeekerMetadata()
	schema.InitKeySeekerMetadata(metadata, decoder, block)
	return schema.KeySeeker(metadata)
}
