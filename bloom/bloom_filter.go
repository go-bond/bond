package bloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-bond/bond"
	"github.com/go-bond/bond/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/lithammer/go-jump-consistent-hash"
)

var _buffPool = utils.NewPreAllocatedSyncPool[[]byte](func() any {
	return make([]byte, 128<<10) // 128 KB
}, 10) // Pre-allocate 10 buffers

type _bucket struct {
	num            int
	hasChanges     bool // true if bucket has unsaved changes since last write
	hasBeenWritten bool // true if bucket has been written to storage at least once

	filter *bloom.BloomFilter

	mu sync.RWMutex
}

type BloomFilter struct {
	hasher utils.SyncPool[jump.KeyHasher]

	keyPrefix    []byte
	numOfBuckets int
	buckets      []*_bucket

	stats bond.FilterStats

	mu sync.RWMutex
}

func NewBloomFilter(n uint, fp float64, numOfBuckets int, keyPrefixes ...[]byte) *BloomFilter {
	hasher := utils.NewPreAllocatedSyncPool[jump.KeyHasher](func() any {
		return jump.NewCRC32()
	}, 4) // Pre-allocate 4 hashers

	buckets := make([]*_bucket, 0, numOfBuckets)
	for i := 0; i < numOfBuckets; i++ {
		buckets = append(buckets, &_bucket{
			num:            i,
			hasChanges:     false,
			hasBeenWritten: false,
			filter:         bloom.NewWithEstimates(n, fp),
		})
	}

	keyPrefix := bond.KeyEncode(bond.Key{
		TableID:    0,
		IndexID:    0,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("bf_"),
	})
	if len(keyPrefixes) > 0 {
		keyPrefix = keyPrefixes[0]
	}

	return &BloomFilter{
		hasher:       hasher,
		keyPrefix:    keyPrefix,
		numOfBuckets: numOfBuckets,
		buckets:      buckets,
		mu:           sync.RWMutex{},
	}
}

func (b *BloomFilter) Add(_ context.Context, key []byte) {
	bucketIndex := b.hash(key)
	bucket := b.buckets[bucketIndex]

	// TestOrAdd is thread-safe, but we lock to protect hasChanges update
	// Lock the specific bucket
	bucket.mu.Lock()

	// Add the key to the bloom filter.
	// NOTE: TestOrAdd returns the value of filter.Test(key) before adding.
	maybeWasPresent := bucket.filter.TestOrAdd(key)
	if !maybeWasPresent {
		bucket.hasChanges = true
	}

	bucket.mu.Unlock()
}

func (b *BloomFilter) MayContain(_ context.Context, key []byte) bool {
	bucketIndex := b.hash(key)
	bucket := b.buckets[bucketIndex]

	// Read-lock the specific bucket
	bucket.mu.RLock()
	contains := bucket.filter.Test(key)
	bucket.mu.RUnlock()

	if !contains {
		atomic.AddUint64(&b.stats.MissCount, 1)
	} else {
		atomic.AddUint64(&b.stats.HitCount, 1)
	}

	return contains
}

func (b *BloomFilter) Stats() bond.FilterStats {
	return bond.FilterStats{
		HitCount:       atomic.LoadUint64(&b.stats.HitCount),
		MissCount:      atomic.LoadUint64(&b.stats.MissCount),
		FalsePositives: atomic.LoadUint64(&b.stats.FalsePositives),
	}
}

func (b *BloomFilter) RecordFalsePositive() {
	atomic.AddUint64(&b.stats.FalsePositives, 1)
}

func (b *BloomFilter) Load(ctx context.Context, store bond.FilterStorer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var keyBuff [1024]byte

	bucketNum, bucketNumCloser, err := store.Get(buildBucketNumKey(keyBuff[:0], b.keyPrefix))
	if err != nil {
		if err == bond.ErrNotFound {
			return fmt.Errorf("bloom filter not found in store")
		}
		return fmt.Errorf("failed to get bucket count: %w", err)
	}
	defer bucketNumCloser.Close()

	if binary.BigEndian.Uint64(bucketNum) != uint64(b.numOfBuckets) {
		return fmt.Errorf("configuration changed: expected %d buckets, found %d",
			b.numOfBuckets, binary.BigEndian.Uint64(bucketNum))
	}

	for i := range b.buckets {
		if err := b.loadBucket(ctx, store, keyBuff, b.buckets[i]); err != nil {
			return fmt.Errorf("failed to load bucket %d: %w", i, err)
		}
	}

	return nil
}

func (b *BloomFilter) Save(ctx context.Context, store bond.FilterStorer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var keyBuff [1024]byte
	dataBuff := _buffPool.Get()
	defer _buffPool.Put(dataBuff[:0])

	binary.BigEndian.PutUint64(dataBuff[:8], uint64(b.numOfBuckets))
	err := store.Set(buildBucketNumKey(keyBuff[:0], b.keyPrefix), dataBuff[:8], bond.Sync)
	if err != nil {
		return fmt.Errorf("failed to save bucket count: %w", err)
	}

	for i := range b.buckets {
		if err := b.saveBucket(ctx, store, keyBuff, b.buckets[i]); err != nil {
			return fmt.Errorf("failed to save bucket %d: %w", i, err)
		}
	}

	return nil
}

func (b *BloomFilter) Clear(_ context.Context, store bond.FilterStorer) error {
	if b.keyPrefix == nil || len(b.keyPrefix) == 0 {
		return fmt.Errorf("cannot clear bloom filter with empty key prefix")
	}

	end := make([]byte, len(b.keyPrefix))
	copy(end, b.keyPrefix)
	end[len(end)-1]++
	return store.DeleteRange(b.keyPrefix, end, bond.Sync)
}

func (b *BloomFilter) hash(key []byte) int {
	hasher := b.hasher.Get()
	v := int(HashBytes(key, int32(b.numOfBuckets), hasher))
	b.hasher.Put(hasher)
	return v
}

func buildKey(buff []byte, keyPrefix []byte, bucketNo int) []byte {
	buff = append(buff, keyPrefix...)
	buff = strconv.AppendInt(buff, int64(bucketNo), 10)
	return buff
}

func buildBucketNumKey(buff []byte, keyPrefix []byte) []byte {
	buff = append(buff, keyPrefix...)
	buff = append(buff, "bn"...)
	return buff
}

func HashBytes(key []byte, buckets int32, h jump.KeyHasher) int32 {
	h.Reset()
	_, err := h.Write(key)
	if err != nil {
		panic(err)
	}
	return jump.Hash(h.Sum64(), buckets)
}

// CountBucketsWithPendingChanges returns the number of buckets that have pending changes to be saved.
func CountBucketsWithPendingChanges(bloomFilter *BloomFilter) int {
	count := 0
	for _, bucket := range bloomFilter.buckets {
		bucket.mu.RLock()
		if bucket.hasChanges {
			count++
		}
		bucket.mu.RUnlock()
	}
	return count
}

func (b *BloomFilter) saveBucket(ctx context.Context, store bond.FilterStorer, keyBuff [1024]byte, bucket *_bucket) error {
	// Lock the bucket early to check flags and potentially write
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// Skip only if clean and already persisted at least once
	if !bucket.hasChanges && bucket.hasBeenWritten {
		return nil
	}

	// Use a temporary buffer from the pool for writing
	writeBuff := _buffPool.Get()
	defer _buffPool.Put(writeBuff[:0])

	buff := bytes.NewBuffer(writeBuff[:0]) // Use the pooled buffer

	zw, err := zstd.NewWriter(buff)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}

	_, err = bucket.filter.WriteTo(zw)
	if err != nil {
		_ = zw.Close()
		return fmt.Errorf("failed to write bloom filter data: %w", err)
	}

	if err := zw.Close(); err != nil {
		return fmt.Errorf("failed to close zstd writer: %w", err)
	}

	err = store.Set(
		buildKey(keyBuff[:0], b.keyPrefix, bucket.num),
		buff.Bytes(),
		bond.Sync,
	)
	if err != nil {
		return fmt.Errorf("failed to save bucket data: %w", err)
	}

	bucket.hasChanges = false
	bucket.hasBeenWritten = true

	return nil
}

func (b *BloomFilter) loadBucket(ctx context.Context, store bond.FilterStorer, keyBuff [1024]byte, bucket *_bucket) error {
	data, closer, err := store.Get(buildKey(keyBuff[:0], b.keyPrefix, bucket.num))
	if err != nil {
		return fmt.Errorf("failed to get bucket data: %w", err)
	}
	defer closer.Close()

	filter := bloom.New(0, 0)
	zr, err := zstd.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	_, err = filter.ReadFrom(zr)
	if err != nil {
		return fmt.Errorf("failed to read bloom filter data: %w", err)
	}

	bucket.mu.Lock()

	err = bucket.filter.Merge(filter)
	if err != nil {
		bucket.mu.Unlock()

		return fmt.Errorf("configuration changed (incompatible filter parameters): %w", err)
	}
	bucket.hasChanges = false
	bucket.hasBeenWritten = true

	bucket.mu.Unlock()

	return nil
}
