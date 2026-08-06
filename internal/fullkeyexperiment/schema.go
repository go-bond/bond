// Package fullkeyexperiment contains the rejected Phase 3 complete-key
// KeySchema experiment. It is internal so production callers cannot select a
// schema that does not support Bond's existing empty-key contract.
package fullkeyexperiment

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	NameB16 = "bond/full-key/v1-b16"
	NameB32 = "bond/full-key/v1-b32"
	NameB64 = "bond/full-key/v1-b64"
)

var columnTypes = []colblk.DataType{colblk.DataTypePrefixBytes}

// New constructs an immutable v1 complete-key physical schema for controlled
// tests and benchmarks. PrefixBytes and Pebble's columnar iterator require
// non-empty stored keys, which is why this rejected experiment is unavailable
// through Bond's production options.
func New(comparer *pebble.Comparer, bundleSize int) (pebble.KeySchema, error) {
	if comparer == nil {
		return pebble.KeySchema{}, fmt.Errorf("bond: full-key schema requires a comparer")
	}
	if comparer.Compare == nil || comparer.Split == nil {
		return pebble.KeySchema{}, fmt.Errorf("bond: full-key schema requires comparer Compare and Split functions")
	}
	name, err := Name(bundleSize)
	if err != nil {
		return pebble.KeySchema{}, err
	}
	return pebble.KeySchema{
		Name:        name,
		ColumnTypes: append([]colblk.DataType(nil), columnTypes...),
		NewKeyWriter: func() colblk.KeyWriter {
			writer := &keyWriter{comparer: comparer}
			writer.keys.Init(bundleSize)
			return writer
		},
		InitKeySeekerMetadata: func(
			meta *colblk.KeySeekerMetadata, _ *colblk.DataBlockDecoder, block colblk.BlockDecoder,
		) {
			seeker := (*keySeeker)(unsafe.Pointer(&meta[0]))
			seeker.comparer = comparer
			seeker.keys = block.PrefixBytes(0)
		},
		KeySeeker: func(meta *colblk.KeySeekerMetadata) colblk.KeySeeker {
			return (*keySeeker)(unsafe.Pointer(&meta[0]))
		},
	}, nil
}

// Configure installs every experimental reader and selects writerName on opts.
// Its internal-package boundary restricts activation to repository tests and
// benchmarks. An empty writerName selects Pebble's legacy schema.
func Configure(opts *pebble.Options, writerName string) error {
	if opts == nil || opts.Comparer == nil {
		return fmt.Errorf("bond: full-key experiment requires options with a comparer")
	}
	legacy := colblk.DefaultKeySchema(opts.Comparer, 16)
	all := []*pebble.KeySchema{&legacy}
	for _, bundleSize := range []int{16, 32, 64} {
		schema, err := New(opts.Comparer, bundleSize)
		if err != nil {
			return err
		}
		all = append(all, &schema)
	}
	if writerName == "" {
		writerName = legacy.Name
	}
	keySchemas := sstable.MakeKeySchemas(all...)
	if _, ok := keySchemas[writerName]; !ok {
		return fmt.Errorf("bond: unknown experimental Pebble writer schema %q", writerName)
	}
	opts.KeySchemas = keySchemas
	opts.KeySchema = writerName
	return nil
}

// Name returns the immutable schema name for a supported bundle size.
func Name(bundleSize int) (string, error) {
	switch bundleSize {
	case 16:
		return NameB16, nil
	case 32:
		return NameB32, nil
	case 64:
		return NameB64, nil
	default:
		return "", fmt.Errorf("bond: full-key schema bundle size must be 16, 32, or 64, got %d", bundleSize)
	}
}

// BundleSize returns the bundle size encoded in an experimental schema name.
func BundleSize(name string) (int, bool) {
	switch name {
	case NameB16:
		return 16, true
	case NameB32:
		return 32, true
	case NameB64:
		return 64, true
	default:
		return 0, false
	}
}

// NewKeySeekerMetadata allocates metadata with the concrete seeker's pointer
// layout so the garbage collector keeps decoded PrefixBytes storage alive.
// It is intended for direct internal schema tests.
func NewKeySeekerMetadata() *colblk.KeySeekerMetadata {
	seeker := &keySeeker{}
	return (*colblk.KeySeekerMetadata)(unsafe.Pointer(seeker))
}

type keyWriter struct {
	comparer *pebble.Comparer
	keys     colblk.PrefixBytesBuilder
}

var _ colblk.KeyWriter = (*keyWriter)(nil)

func (w *keyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	prefixLen := w.comparer.Split(key)
	comparison := colblk.KeyComparison{PrefixLen: int32(prefixLen)}
	if w.keys.Rows() == 0 {
		comparison.UserKeyComparison = 1
		return comparison
	}

	previous := w.keys.UnsafeGet(w.keys.Rows() - 1)
	previousPrefixLen := w.comparer.Split(previous)
	comparison.CommonPrefixLen = int32(commonPrefixLength(
		key[:prefixLen], previous[:previousPrefixLen],
	))
	comparison.UserKeyComparison = int32(w.comparer.Compare(key, previous))
	return comparison
}

func (w *keyWriter) WriteKey(_ int, key []byte, _, _ int32) {
	shared := 0
	if w.keys.Rows() > 0 {
		shared = commonPrefixLength(key, w.keys.UnsafeGet(w.keys.Rows()-1))
	}
	w.keys.Put(key, shared)
}

func (w *keyWriter) MaterializeKey(dst []byte, row int) []byte {
	return append(dst, w.keys.UnsafeGet(row)...)
}

func (w *keyWriter) NumColumns() int { return 1 }

func (w *keyWriter) DataType(column int) colblk.DataType {
	if column != 0 {
		panic(fmt.Sprintf("bond: unknown full-key schema column %d", column))
	}
	return colblk.DataTypePrefixBytes
}

func (w *keyWriter) Reset() { w.keys.Reset() }

func (w *keyWriter) Size(rows int, offset uint32) uint32 {
	return w.keys.Size(rows, offset)
}

func (w *keyWriter) Finish(column, rows int, offset uint32, buffer []byte) uint32 {
	if column != 0 {
		panic(fmt.Sprintf("bond: unknown full-key schema column %d", column))
	}
	return w.keys.Finish(0, rows, offset, buffer)
}

func (w *keyWriter) FinishHeader([]byte) {}

func (w *keyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "0: full keys: ")
	w.keys.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

type keySeeker struct {
	comparer *pebble.Comparer
	keys     colblk.PrefixBytes
}

var _ colblk.KeySeeker = (*keySeeker)(nil)
var _ uint = colblk.KeySeekerMetadataSize - uint(unsafe.Sizeof(keySeeker{}))

func (s *keySeeker) IsLowerBound(key []byte, syntheticSuffix []byte) bool {
	if s.keys.Rows() == 0 {
		return true
	}
	first := s.keys.UnsafeFirstSlice()
	if len(syntheticSuffix) == 0 {
		return s.comparer.Compare(first, key) >= 0
	}
	prefixLen := s.comparer.Split(first)
	effectiveFirst := make([]byte, 0, prefixLen+len(syntheticSuffix))
	effectiveFirst = append(effectiveFirst, first[:prefixLen]...)
	effectiveFirst = append(effectiveFirst, syntheticSuffix...)
	return s.comparer.Compare(effectiveFirst, key) >= 0
}

func (s *keySeeker) SeekGE(key []byte, _ int, _ int8) (row int, equalPrefix bool) {
	row, exact := s.keys.Search(key)
	if exact {
		return row, true
	}
	if row >= s.keys.Rows() {
		return row, false
	}
	found := s.keys.At(row)
	keyPrefixLen := s.comparer.Split(key)
	foundPrefixLen := s.comparer.Split(found)
	return row, bytes.Equal(key[:keyPrefixLen], found[:foundPrefixLen])
}

func (s *keySeeker) MaterializeUserKey(keyIter *colblk.PrefixBytesIter, previousRow, row int) []byte {
	if previousRow >= 0 && row == previousRow+1 {
		s.keys.SetNext(keyIter)
	} else {
		s.keys.SetAt(keyIter, row)
	}
	return keyIter.Buf
}

func (s *keySeeker) MaterializeUserKeyWithSyntheticSuffix(
	keyIter *colblk.PrefixBytesIter, suffix []byte, _ int, row int,
) []byte {
	// Reposition unconditionally. Appending the synthetic suffix overwrites the
	// physical full key in keyIter.Buf, so it cannot serve as SetNext's base.
	s.keys.SetAt(keyIter, row)
	originalKey := s.keys.At(row)
	prefixLen := s.comparer.Split(originalKey)
	syntheticPrefixLen := len(keyIter.Buf) - len(originalKey)
	return append(keyIter.Buf[:syntheticPrefixLen+prefixLen], suffix...)
}

func commonPrefixLength(a, b []byte) int {
	limit := min(len(a), len(b))
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
}
