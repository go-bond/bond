// Package typedschemaexperiment contains the Phase 6 typed primary-key-tail
// KeySchema experiments. The schemas are internal because stock Pebble can
// select only one global writer schema, while these parsers are valid only for
// an isolated table family or a future, explicitly routed table range.
package typedschemaexperiment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	NameUint64 = "bond/pk-u64/v1"
	NameUint32 = "bond/pk-u32/v1"
	NameBytes  = "bond/pk-bytes/v1"

	BundleSize = 16
)

type Family uint8

const (
	FamilyUint64 Family = iota + 1
	FamilyUint32
	FamilyBytes
)

const (
	columnPrefix int = iota
	columnTail
	columnPrimary
	columnCount
)

var (
	uintColumnTypes = []colblk.DataType{
		columnPrefix:  colblk.DataTypePrefixBytes,
		columnTail:    colblk.DataTypeBytes,
		columnPrimary: colblk.DataTypeUint,
	}
	bytesColumnTypes = []colblk.DataType{
		columnPrefix:  colblk.DataTypePrefixBytes,
		columnTail:    colblk.DataTypeBytes,
		columnPrimary: colblk.DataTypeBytes,
	}
	fallbackTailMarker = []byte{0}
	typedTailMarker    = []byte{1}
)

// New constructs an immutable typed family schema. All names use bundle size
// 16 permanently; changing the parser or bundle size requires a new durable
// name. The writer preserves unsupported point-key forms through an opaque
// suffix column. Empty point keys are outside a routed Bond table range and
// remain a reason these schemas cannot be selected globally in production.
func New(comparer *pebble.Comparer, family Family) (pebble.KeySchema, error) {
	if comparer == nil {
		return pebble.KeySchema{}, fmt.Errorf("bond: typed key schema requires a comparer")
	}
	if comparer.Compare == nil || comparer.Split == nil {
		return pebble.KeySchema{}, fmt.Errorf("bond: typed key schema requires comparer Compare and Split functions")
	}
	name, err := family.Name()
	if err != nil {
		return pebble.KeySchema{}, err
	}
	columnTypes := uintColumnTypes
	if family == FamilyBytes {
		columnTypes = bytesColumnTypes
	}
	return pebble.KeySchema{
		Name:        name,
		ColumnTypes: append([]colblk.DataType(nil), columnTypes...),
		NewKeyWriter: func() colblk.KeyWriter {
			return newKeyWriter(comparer, family)
		},
		InitKeySeekerMetadata: func(
			meta *colblk.KeySeekerMetadata, _ *colblk.DataBlockDecoder, block colblk.BlockDecoder,
		) {
			seeker := (*keySeeker)(unsafe.Pointer(&meta[0]))
			seeker.comparer = comparer
			seeker.family = family
			seeker.prefixes = block.PrefixBytes(columnPrefix)
			seeker.tails = block.RawBytes(columnTail)
			if family == FamilyBytes {
				seeker.primaryBytes = block.RawBytes(columnPrimary)
				seeker.primaryUints = colblk.UnsafeUints{}
			} else {
				seeker.primaryUints = block.Uints(columnPrimary)
				seeker.primaryBytes = colblk.RawBytes{}
			}
		},
		KeySeeker: func(meta *colblk.KeySeekerMetadata) colblk.KeySeeker {
			return (*keySeeker)(unsafe.Pointer(&meta[0]))
		},
	}, nil
}

// Configure registers every typed family plus Pebble's legacy schema and
// selects writerName. It is intentionally available only to repository tests
// and isolated benchmark databases.
func Configure(opts *pebble.Options, writerName string) error {
	if opts == nil || opts.Comparer == nil {
		return fmt.Errorf("bond: typed key experiment requires options with a comparer")
	}
	legacy := colblk.DefaultKeySchema(opts.Comparer, BundleSize)
	all := []*pebble.KeySchema{&legacy}
	for _, family := range []Family{FamilyUint64, FamilyUint32, FamilyBytes} {
		schema, err := New(opts.Comparer, family)
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
		return fmt.Errorf("bond: unknown experimental typed Pebble writer schema %q", writerName)
	}
	opts.KeySchemas = keySchemas
	opts.KeySchema = writerName
	return nil
}

func (f Family) Name() (string, error) {
	switch f {
	case FamilyUint64:
		return NameUint64, nil
	case FamilyUint32:
		return NameUint32, nil
	case FamilyBytes:
		return NameBytes, nil
	default:
		return "", fmt.Errorf("bond: unknown typed key family %d", f)
	}
}

func IsName(name string) bool {
	switch name {
	case NameUint64, NameUint32, NameBytes:
		return true
	default:
		return false
	}
}

// NewKeySeekerMetadata allocates metadata with the concrete seeker's pointer
// layout so direct schema tests keep all decoded column references visible to
// the garbage collector.
func NewKeySeekerMetadata() *colblk.KeySeekerMetadata {
	seeker := &keySeeker{}
	return (*colblk.KeySeekerMetadata)(unsafe.Pointer(seeker))
}

type keyWriter struct {
	comparer     *pebble.Comparer
	family       Family
	prefixes     colblk.PrefixBytesBuilder
	tails        colblk.RawBytesBuilder
	primaryUints colblk.UintBuilder
	primaryBytes colblk.RawBytesBuilder
	previousKey  []byte
}

var _ colblk.KeyWriter = (*keyWriter)(nil)

func newKeyWriter(comparer *pebble.Comparer, family Family) *keyWriter {
	w := &keyWriter{comparer: comparer, family: family}
	w.prefixes.Init(BundleSize)
	w.tails.Init()
	if family == FamilyBytes {
		w.primaryBytes.Init()
	} else {
		w.primaryUints.Init()
	}
	return w
}

func (w *keyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	prefixLen := w.comparer.Split(key)
	comparison := colblk.KeyComparison{PrefixLen: int32(prefixLen)}
	if w.prefixes.Rows() == 0 {
		comparison.UserKeyComparison = 1
		return comparison
	}
	previousPrefix := w.prefixes.UnsafeGet(w.prefixes.Rows() - 1)
	comparison.CommonPrefixLen = int32(commonPrefixLength(key[:prefixLen], previousPrefix))
	comparison.UserKeyComparison = int32(w.comparer.Compare(key, w.previousKey))
	return comparison
}

func (w *keyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	if keyPrefixLen == 0 {
		panic("bond: typed key schema cannot encode a point key outside a routed Bond table range")
	}
	prefixLen := int(keyPrefixLen)
	w.prefixes.Put(key[:prefixLen], int(keyPrefixLenSharedWithPrev))

	middle, primaryUint, primaryBytes, typed := parseTypedTail(w.family, key[prefixLen:])
	if typed {
		w.tails.PutConcat(typedTailMarker, middle)
	} else {
		w.tails.PutConcat(fallbackTailMarker, key[prefixLen:])
	}
	if w.family == FamilyBytes {
		w.primaryBytes.Put(primaryBytes)
	} else {
		w.primaryUints.Set(row, primaryUint)
	}
	w.previousKey = append(w.previousKey[:0], key...)
}

func (w *keyWriter) MaterializeKey(dst []byte, row int) []byte {
	dst = append(dst, w.prefixes.UnsafeGet(row)...)
	return w.appendTail(dst, row)
}

func (w *keyWriter) appendTail(dst []byte, row int) []byte {
	tail := w.tails.UnsafeGet(row)
	if len(tail) == 0 {
		panic("bond: corrupt typed key tail")
	}
	dst = append(dst, tail[1:]...)
	if tail[0] == fallbackTailMarker[0] {
		return dst
	}
	dst = append(dst, 1)
	if w.family == FamilyBytes {
		return append(dst, w.primaryBytes.UnsafeGet(row)...)
	}
	return appendUint(dst, w.family, w.primaryUints.Get(row))
}

func (w *keyWriter) NumColumns() int { return columnCount }

func (w *keyWriter) DataType(column int) colblk.DataType {
	if column < 0 || column >= columnCount {
		panic(fmt.Sprintf("bond: unknown typed key schema column %d", column))
	}
	if column == columnPrimary && w.family == FamilyBytes {
		return colblk.DataTypeBytes
	}
	return uintColumnTypes[column]
}

func (w *keyWriter) Reset() {
	w.prefixes.Reset()
	w.tails.Reset()
	if w.family == FamilyBytes {
		w.primaryBytes.Reset()
	} else {
		w.primaryUints.Reset()
	}
	w.previousKey = w.previousKey[:0]
}

func (w *keyWriter) Size(rows int, offset uint32) uint32 {
	offset = w.prefixes.Size(rows, offset)
	offset = w.tails.Size(rows, offset)
	if w.family == FamilyBytes {
		return w.primaryBytes.Size(rows, offset)
	}
	return w.primaryUints.Size(rows, offset)
}

func (w *keyWriter) Finish(column, rows int, offset uint32, buffer []byte) uint32 {
	switch column {
	case columnPrefix:
		return w.prefixes.Finish(0, rows, offset, buffer)
	case columnTail:
		return w.tails.Finish(0, rows, offset, buffer)
	case columnPrimary:
		if w.family == FamilyBytes {
			return w.primaryBytes.Finish(0, rows, offset, buffer)
		}
		return w.primaryUints.Finish(0, rows, offset, buffer)
	default:
		panic(fmt.Sprintf("bond: unknown typed key schema column %d", column))
	}
}

func (w *keyWriter) FinishHeader([]byte) {}

func (w *keyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "0: logical prefixes: ")
	w.prefixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "1: framed tails: ")
	w.tails.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "2: typed primary keys: ")
	if w.family == FamilyBytes {
		w.primaryBytes.WriteDebug(dst, rows)
	} else {
		w.primaryUints.WriteDebug(dst, rows)
	}
	fmt.Fprintln(dst)
}

type keySeeker struct {
	comparer     *pebble.Comparer
	family       Family
	prefixes     colblk.PrefixBytes
	tails        colblk.RawBytes
	primaryUints colblk.UnsafeUints
	primaryBytes colblk.RawBytes
}

var _ colblk.KeySeeker = (*keySeeker)(nil)
var _ uint = colblk.KeySeekerMetadataSize - uint(unsafe.Sizeof(keySeeker{}))

func (s *keySeeker) IsLowerBound(key []byte, syntheticSuffix []byte) bool {
	if s.prefixes.Rows() == 0 {
		return true
	}
	shared, bundle, row := s.prefixParts(0)
	if len(syntheticSuffix) > 0 {
		return comparePiecesToKey(key, shared, bundle, row, syntheticSuffix) >= 0
	}
	return s.compareRowToKey(0, key) >= 0
}

func (s *keySeeker) SeekGE(key []byte, _ int, _ int8) (row int, equalPrefix bool) {
	rows := s.prefixes.Rows()
	row = sort.Search(rows, func(row int) bool {
		return s.compareRowToKey(row, key) >= 0
	})
	if row == rows {
		return row, false
	}
	targetPrefix := key[:s.comparer.Split(key)]
	shared, bundle, rowSuffix := s.prefixParts(row)
	return row, equalPiecesToBytes(targetPrefix, shared, bundle, rowSuffix)
}

func (s *keySeeker) MaterializeUserKey(keyIter *colblk.PrefixBytesIter, _, row int) []byte {
	s.prefixes.SetAt(keyIter, row)
	keyIter.Buf = s.appendTail(keyIter.Buf, row)
	return keyIter.Buf
}

func (s *keySeeker) MaterializeUserKeyWithSyntheticSuffix(
	keyIter *colblk.PrefixBytesIter, syntheticSuffix []byte, _, row int,
) []byte {
	s.prefixes.SetAt(keyIter, row)
	keyIter.Buf = append(keyIter.Buf, syntheticSuffix...)
	return keyIter.Buf
}

func (s *keySeeker) compareRowToKey(row int, key []byte) int {
	tail := s.tails.At(row)
	if len(tail) == 0 {
		panic("bond: corrupt typed key tail")
	}
	shared, bundle, rowSuffix := s.prefixParts(row)
	body := tail[1:]
	if tail[0] == fallbackTailMarker[0] {
		return comparePiecesToKey(key, shared, bundle, rowSuffix, body)
	}
	if s.family == FamilyBytes {
		return comparePiecesToKey(key, shared, bundle, rowSuffix, body, typedTailMarker, s.primaryBytes.At(row))
	}
	var encoded [8]byte
	primary := encoded[:0]
	primary = appendUint(primary, s.family, s.primaryUints.At(row))
	return comparePiecesToKey(key, shared, bundle, rowSuffix, body, typedTailMarker, primary)
}

func (s *keySeeker) appendTail(dst []byte, row int) []byte {
	tail := s.tails.At(row)
	if len(tail) == 0 {
		panic("bond: corrupt typed key tail")
	}
	dst = append(dst, tail[1:]...)
	if tail[0] == fallbackTailMarker[0] {
		return dst
	}
	dst = append(dst, 1)
	if s.family == FamilyBytes {
		return append(dst, s.primaryBytes.At(row)...)
	}
	return appendUint(dst, s.family, s.primaryUints.At(row))
}

func (s *keySeeker) prefixParts(row int) (shared, bundle, suffix []byte) {
	return s.prefixes.SharedPrefix(), s.prefixes.RowBundlePrefix(row), s.prefixes.RowSuffix(row)
}

// parseTypedTail is total over arbitrary suffix bytes. A typed result is
// returned only for the unambiguous Bond suffix framing
// `OrderLen|Order|PrimaryField`; every other shape is preserved opaquely.
func parseTypedTail(family Family, suffix []byte) (
	middle []byte, primaryUint uint64, primaryBytes []byte, typed bool,
) {
	if len(suffix) < 4 {
		return nil, 0, nil, false
	}
	orderLength := binary.BigEndian.Uint32(suffix[:4])
	if uint64(orderLength) > uint64(len(suffix)-4) {
		return nil, 0, nil, false
	}
	primaryOffset := 4 + int(orderLength)
	primary := suffix[primaryOffset:]
	if len(primary) == 0 || primary[0] != 1 {
		return nil, 0, nil, false
	}
	switch family {
	case FamilyUint64:
		if len(primary) != 9 {
			return nil, 0, nil, false
		}
		primaryUint = binary.BigEndian.Uint64(primary[1:])
	case FamilyUint32:
		if len(primary) != 5 {
			return nil, 0, nil, false
		}
		primaryUint = uint64(binary.BigEndian.Uint32(primary[1:]))
	case FamilyBytes:
		primaryBytes = primary[1:]
	default:
		return nil, 0, nil, false
	}
	return suffix[:primaryOffset], primaryUint, primaryBytes, true
}

func appendUint(dst []byte, family Family, value uint64) []byte {
	switch family {
	case FamilyUint64:
		return binary.BigEndian.AppendUint64(dst, value)
	case FamilyUint32:
		return binary.BigEndian.AppendUint32(dst, uint32(value))
	default:
		panic(fmt.Sprintf("bond: family %d has no uint primary key", family))
	}
}

func comparePiecesToKey(key []byte, shared, bundle, row []byte, suffixPieces ...[]byte) int {
	keyOffset := 0
	compare := func(pieces ...[]byte) int {
		for _, piece := range pieces {
			if keyOffset == len(key) {
				if len(piece) > 0 {
					return 1
				}
				continue
			}
			compared := min(len(piece), len(key)-keyOffset)
			if comparison := bytes.Compare(piece[:compared], key[keyOffset:keyOffset+compared]); comparison != 0 {
				return comparison
			}
			keyOffset += compared
			if compared < len(piece) {
				return 1
			}
		}
		return 0
	}
	if comparison := compare(shared, bundle, row); comparison != 0 {
		return comparison
	}
	if comparison := compare(suffixPieces...); comparison != 0 {
		return comparison
	}
	return keyOffset - len(key)
}

func equalPiecesToBytes(value []byte, pieces ...[]byte) bool {
	offset := 0
	for _, piece := range pieces {
		if len(value)-offset < len(piece) || !bytes.Equal(value[offset:offset+len(piece)], piece) {
			return false
		}
		offset += len(piece)
	}
	return offset == len(value)
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
