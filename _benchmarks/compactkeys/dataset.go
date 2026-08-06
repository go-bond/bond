package compactkeys

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"sort"

	"github.com/go-bond/bond"
)

type KeyShape string

const (
	KeyShapeSequentialUint64 KeyShape = "sequential-u64"
	KeyShapeRandomUint64     KeyShape = "random-u64"
	KeyShapeSequentialUint32 KeyShape = "sequential-u32"
	KeyShapeRandomUint32     KeyShape = "random-u32"
	KeyShapeBytes20          KeyShape = "bytes-20"
	KeyShapeBytes32          KeyShape = "bytes-32"
	KeyShapeBytes64          KeyShape = "bytes-64"
	KeyShapeUUID             KeyShape = "uuid"
	KeyShapeAddress          KeyShape = "address"
	KeyShapeComposite2       KeyShape = "composite-2"
	KeyShapeComposite3       KeyShape = "composite-3"
	KeyShapeMixed            KeyShape = "mixed"
)

type OrderShape string

const (
	OrderShapeNone     OrderShape = "none"
	OrderShapeFixed    OrderShape = "fixed"
	OrderShapeVariable OrderShape = "variable"
	OrderShapeMixed    OrderShape = "mixed"
)

type IndexCardinality string

const (
	IndexCardinalityLow   IndexCardinality = "low"
	IndexCardinalityHigh  IndexCardinality = "high"
	IndexCardinalityMixed IndexCardinality = "mixed"
)

type PrefixShape string

const (
	PrefixShapeCommon PrefixShape = "common"
	PrefixShapeRandom PrefixShape = "random"
	PrefixShapeMixed  PrefixShape = "mixed"
)

type DatasetSpec struct {
	Seed                int64            `json:"seed"`
	Rows                int              `json:"rows"`
	SecondaryIndexes    int              `json:"secondary_indexes"`
	KeyShape            KeyShape         `json:"key_shape"`
	OrderShape          OrderShape       `json:"order_shape"`
	IndexCardinality    IndexCardinality `json:"index_cardinality"`
	PrefixShape         PrefixShape      `json:"prefix_shape"`
	PartialIndexPercent int              `json:"partial_index_percent"`
	ValueBytes          int              `json:"value_bytes"`
}

func RepresentativeDatasetSpec(seed int64, rows int) DatasetSpec {
	return DatasetSpec{
		Seed:                seed,
		Rows:                rows,
		SecondaryIndexes:    3,
		KeyShape:            KeyShapeMixed,
		OrderShape:          OrderShapeMixed,
		IndexCardinality:    IndexCardinalityMixed,
		PrefixShape:         PrefixShapeMixed,
		PartialIndexPercent: 70,
		ValueBytes:          160,
	}
}

func CoverageDatasetSpecs() []DatasetSpec {
	keyShapes := []KeyShape{
		KeyShapeSequentialUint64,
		KeyShapeRandomUint64,
		KeyShapeSequentialUint32,
		KeyShapeRandomUint32,
		KeyShapeBytes20,
		KeyShapeBytes32,
		KeyShapeBytes64,
		KeyShapeUUID,
		KeyShapeAddress,
		KeyShapeComposite2,
		KeyShapeComposite3,
	}
	orders := []OrderShape{OrderShapeNone, OrderShapeFixed, OrderShapeVariable}
	cardinalities := []IndexCardinality{IndexCardinalityLow, IndexCardinalityHigh}
	prefixes := []PrefixShape{PrefixShapeCommon, PrefixShapeRandom}
	indexCounts := []int{1, 3, 8}

	specs := make([]DatasetSpec, 0, len(keyShapes))
	for i, shape := range keyShapes {
		specs = append(specs, DatasetSpec{
			Seed:                int64(1000 + i),
			Rows:                12,
			SecondaryIndexes:    indexCounts[i%len(indexCounts)],
			KeyShape:            shape,
			OrderShape:          orders[i%len(orders)],
			IndexCardinality:    cardinalities[i%len(cardinalities)],
			PrefixShape:         prefixes[i%len(prefixes)],
			PartialIndexPercent: 50,
			ValueBytes:          32,
		})
	}
	return specs
}

type Entry struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Row       int    `json:"row"`
	IndexID   byte   `json:"index_id"`
	IsPrimary bool   `json:"is_primary"`
}

type Mutation struct {
	Delete bool   `json:"delete"`
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
}

type Dataset struct {
	Spec                DatasetSpec `json:"spec"`
	InitialEntries      []Entry     `json:"initial_entries"`
	FinalEntries        []Entry     `json:"final_entries"`
	Mutations           []Mutation  `json:"mutations"`
	MissKeys            [][]byte    `json:"miss_keys"`
	PrimaryRows         int         `json:"primary_rows"`
	InitialIndexEntries int         `json:"initial_index_entries"`
	FinalIndexEntries   int         `json:"final_index_entries"`
	Digest              string      `json:"digest"`
}

func Generate(spec DatasetSpec) (Dataset, error) {
	if err := spec.validate(); err != nil {
		return Dataset{}, err
	}

	seed := uint64(spec.Seed)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	initial := make([]Entry, 0, spec.Rows*(spec.SecondaryIndexes+1))
	finalByKey := make(map[string]Entry, cap(initial))
	mutations := make([]Mutation, 0, spec.Rows/8)
	indexEntries := 0

	for row := range spec.Rows {
		primaryKey := makePrimaryKey(spec.KeyShape, spec.Seed, row, rng)
		primaryEntry := Entry{
			Key:       bond.KeyEncode(bond.Key{TableID: 1, PrimaryKey: primaryKey}),
			Value:     makeValue(spec.Seed, row, spec.ValueBytes, false),
			Row:       row,
			IsPrimary: true,
		}
		rowEntries := []Entry{primaryEntry}

		for index := 1; index <= spec.SecondaryIndexes; index++ {
			if !includePartialIndex(row, index, spec.PartialIndexPercent) {
				continue
			}
			entry := Entry{
				Key: bond.KeyEncode(bond.Key{
					TableID:    1,
					IndexID:    bond.IndexID(index),
					Index:      makeIndexKey(spec, row, index, rng),
					IndexOrder: makeIndexOrder(spec.OrderShape, row, index),
					PrimaryKey: primaryKey,
				}),
				Row:     row,
				IndexID: byte(index),
			}
			rowEntries = append(rowEntries, entry)
			indexEntries++
		}

		initial = append(initial, rowEntries...)
		for _, entry := range rowEntries {
			finalByKey[string(entry.Key)] = cloneEntry(entry)
		}

		switch {
		case row%29 == 0:
			for _, entry := range rowEntries {
				delete(finalByKey, string(entry.Key))
				mutations = append(mutations, Mutation{Delete: true, Key: bytes.Clone(entry.Key)})
			}
		case row%17 == 0:
			updated := cloneEntry(primaryEntry)
			updated.Value = makeValue(spec.Seed, row, spec.ValueBytes, true)
			finalByKey[string(updated.Key)] = updated
			mutations = append(mutations, Mutation{Key: bytes.Clone(updated.Key), Value: bytes.Clone(updated.Value)})
		}
	}

	final := make([]Entry, 0, len(finalByKey))
	finalIndexEntries := 0
	for _, entry := range finalByKey {
		final = append(final, entry)
		if !entry.IsPrimary {
			finalIndexEntries++
		}
	}
	sortEntries(initial)
	sortEntries(final)
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })

	dataset := Dataset{
		Spec:                spec,
		InitialEntries:      initial,
		FinalEntries:        final,
		Mutations:           mutations,
		MissKeys:            makeMissKeys(final, min(spec.Rows, 2048)),
		PrimaryRows:         spec.Rows,
		InitialIndexEntries: indexEntries,
		FinalIndexEntries:   finalIndexEntries,
	}
	dataset.Digest = datasetDigest(dataset)
	return dataset, nil
}

func (s DatasetSpec) validate() error {
	if s.Rows <= 0 {
		return fmt.Errorf("rows must be positive")
	}
	if s.SecondaryIndexes != 1 && s.SecondaryIndexes != 3 && s.SecondaryIndexes != 8 {
		return fmt.Errorf("secondary indexes must be one of 1, 3, or 8")
	}
	if s.PartialIndexPercent < 1 || s.PartialIndexPercent > 100 {
		return fmt.Errorf("partial index percent must be within [1,100]")
	}
	if s.ValueBytes < 0 {
		return fmt.Errorf("value bytes must not be negative")
	}
	if !contains([]KeyShape{KeyShapeSequentialUint64, KeyShapeRandomUint64, KeyShapeSequentialUint32, KeyShapeRandomUint32, KeyShapeBytes20, KeyShapeBytes32, KeyShapeBytes64, KeyShapeUUID, KeyShapeAddress, KeyShapeComposite2, KeyShapeComposite3, KeyShapeMixed}, s.KeyShape) {
		return fmt.Errorf("unknown key shape %q", s.KeyShape)
	}
	if !contains([]OrderShape{OrderShapeNone, OrderShapeFixed, OrderShapeVariable, OrderShapeMixed}, s.OrderShape) {
		return fmt.Errorf("unknown order shape %q", s.OrderShape)
	}
	if !contains([]IndexCardinality{IndexCardinalityLow, IndexCardinalityHigh, IndexCardinalityMixed}, s.IndexCardinality) {
		return fmt.Errorf("unknown index cardinality %q", s.IndexCardinality)
	}
	if !contains([]PrefixShape{PrefixShapeCommon, PrefixShapeRandom, PrefixShapeMixed}, s.PrefixShape) {
		return fmt.Errorf("unknown prefix shape %q", s.PrefixShape)
	}
	return nil
}

func makePrimaryKey(shape KeyShape, seed int64, row int, rng *rand.Rand) []byte {
	if shape == KeyShapeMixed {
		shapes := [...]KeyShape{KeyShapeSequentialUint64, KeyShapeRandomUint64, KeyShapeBytes20, KeyShapeBytes32, KeyShapeBytes64, KeyShapeUUID, KeyShapeAddress, KeyShapeComposite2, KeyShapeComposite3}
		shape = shapes[row%len(shapes)]
	}
	builder := bond.NewKeyBuilder(nil)
	switch shape {
	case KeyShapeSequentialUint64:
		return builder.AddUint64Field(uint64(row)).Bytes()
	case KeyShapeRandomUint64:
		return builder.AddUint64Field(bits.Reverse64(uint64(row) ^ uint64(seed))).Bytes()
	case KeyShapeSequentialUint32:
		return builder.AddUint32Field(uint32(row)).Bytes()
	case KeyShapeRandomUint32:
		return builder.AddUint32Field(bits.Reverse32(uint32(row) ^ uint32(seed))).Bytes()
	case KeyShapeBytes20:
		return builder.AddBytesField(deterministicBytes(seed, row, 20)).Bytes()
	case KeyShapeBytes32:
		return builder.AddBytesField(deterministicBytes(seed, row, 32)).Bytes()
	case KeyShapeBytes64:
		return builder.AddBytesField(deterministicBytes(seed, row, 64)).Bytes()
	case KeyShapeUUID:
		value := deterministicBytes(seed, row, 16)
		value[6] = value[6]&0x0f | 0x40
		value[8] = value[8]&0x3f | 0x80
		return builder.AddBytesField(value).Bytes()
	case KeyShapeAddress:
		return builder.AddBytesField(deterministicBytes(seed^0xa44e55, row, 20)).Bytes()
	case KeyShapeComposite2:
		return builder.AddStringField(fmt.Sprintf("tenant-%04d", row%257)).AddUint64Field(uint64(row)).Bytes()
	case KeyShapeComposite3:
		return builder.AddUint32Field(uint32(row % 31)).AddStringField(fmt.Sprintf("bucket-%03d", row%113)).AddUint64Field(uint64(row)).Bytes()
	default:
		panic("validated key shape became invalid")
	}
}

func makeIndexKey(spec DatasetSpec, row, index int, rng *rand.Rand) []byte {
	cardinality := spec.IndexCardinality
	if cardinality == IndexCardinalityMixed {
		if index%2 == 0 {
			cardinality = IndexCardinalityHigh
		} else {
			cardinality = IndexCardinalityLow
		}
	}
	prefix := spec.PrefixShape
	if prefix == PrefixShapeMixed {
		if index%2 == 0 {
			prefix = PrefixShapeRandom
		} else {
			prefix = PrefixShapeCommon
		}
	}

	builder := bond.NewKeyBuilder(nil)
	if prefix == PrefixShapeCommon {
		builder.AddStringField(fmt.Sprintf("index-%02d/common/", index))
	} else {
		randomPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(randomPrefix, rng.Uint64())
		builder.AddBytesField(randomPrefix)
	}
	if cardinality == IndexCardinalityLow {
		return builder.AddUint16Field(uint16((row + index) % 16)).Bytes()
	}
	return builder.AddUint64Field(bits.Reverse64(uint64(row*17 + index))).Bytes()
}

func makeIndexOrder(shape OrderShape, row, index int) []byte {
	if shape == OrderShapeMixed {
		shape = [...]OrderShape{OrderShapeNone, OrderShapeFixed, OrderShapeVariable}[index%3]
	}
	builder := bond.NewKeyBuilder(nil)
	switch shape {
	case OrderShapeNone:
		return nil
	case OrderShapeFixed:
		return builder.AddUint64Field(uint64(1_700_000_000_000 + row*100 + index)).Bytes()
	case OrderShapeVariable:
		payloadBytes := 1 + (row+index)%12
		return builder.AddStringField(fmt.Sprintf("order/%02d/%s", index, hex.EncodeToString(deterministicBytes(int64(index), row, payloadBytes)))).Bytes()
	default:
		panic("validated order shape became invalid")
	}
}

func makeValue(seed int64, row, size int, updated bool) []byte {
	if size == 0 {
		return nil
	}
	marker := "created"
	if updated {
		marker = "updated"
	}
	prefix := []byte(fmt.Sprintf("row=%08d state=%s seed=%d ", row, marker, seed))
	value := make([]byte, size)
	for i := range value {
		value[i] = prefix[i%len(prefix)]
	}
	return value
}

func includePartialIndex(row, index, percent int) bool {
	return (row*37+index*19)%100 < percent
}

func deterministicBytes(seed int64, row, size int) []byte {
	result := make([]byte, size)
	var input [16]byte
	binary.BigEndian.PutUint64(input[:8], uint64(seed))
	binary.BigEndian.PutUint64(input[8:], uint64(row))
	for offset, counter := 0, byte(0); offset < size; counter++ {
		hashInput := append(input[:], counter)
		sum := sha256.Sum256(hashInput)
		offset += copy(result[offset:], sum[:])
	}
	if size >= 8 {
		binary.BigEndian.PutUint64(result[:8], uint64(row))
	}
	return result
}

func makeMissKeys(entries []Entry, count int) [][]byte {
	existing := make(map[string]struct{}, len(entries))
	primaryKeys := make([][]byte, 0, count)
	for _, entry := range entries {
		existing[string(entry.Key)] = struct{}{}
		if entry.IsPrimary {
			primaryKeys = append(primaryKeys, entry.Key)
		}
	}
	misses := make([][]byte, 0, count)
	for i := 0; len(misses) < count && len(primaryKeys) > 0; i++ {
		key := append(bytes.Clone(primaryKeys[i%len(primaryKeys)]), 0xff, byte(i), byte(i>>8))
		if _, ok := existing[string(key)]; !ok {
			misses = append(misses, key)
		}
	}
	return misses
}

func datasetDigest(dataset Dataset) string {
	hash := sha256.New()
	encodedSpec, _ := json.Marshal(dataset.Spec)
	hash.Write(encodedSpec)
	for _, entries := range [][]Entry{dataset.InitialEntries, dataset.FinalEntries} {
		for _, entry := range entries {
			hash.Write(entry.Key)
			hash.Write([]byte{0})
			hash.Write(entry.Value)
			hash.Write([]byte{0xff})
		}
	}
	for _, mutation := range dataset.Mutations {
		if mutation.Delete {
			hash.Write([]byte{1})
		} else {
			hash.Write([]byte{0})
		}
		hash.Write(mutation.Key)
		hash.Write(mutation.Value)
	}
	for _, miss := range dataset.MissKeys {
		hash.Write(miss)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
}

func cloneEntry(entry Entry) Entry {
	entry.Key = bytes.Clone(entry.Key)
	entry.Value = bytes.Clone(entry.Value)
	return entry
}

func contains[T comparable](values []T, target T) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
