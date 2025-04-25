package bloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

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
	hasChanges     bool
	hasBeenWritten bool

	filter *bloom.BloomFilter

	mutex sync.RWMutex
}

type BloomFilter struct {
	hasher utils.SyncPool[jump.KeyHasher]

	keyPrefix    []byte
	numOfBuckets int
	buckets      []*_bucket

	mutex sync.RWMutex
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
		mutex:        sync.RWMutex{},
	}
}

func (b *BloomFilter) Add(_ context.Context, key []byte) {
	b.mutex.RLock()
	bucketIndex := b.hash(key)
	b.mutex.RUnlock()

	bucket := b.buckets[bucketIndex]

	// TestOrAdd is thread-safe, but we lock to protect hasChanges update
	added := bucket.filter.TestOrAdd(key)
	bucket.mutex.Lock()
	if added {
		bucket.hasChanges = true
	}
	bucket.mutex.Unlock()
}

func (b *BloomFilter) MayContain(_ context.Context, key []byte) bool {
	b.mutex.RLock()
	bucketIndex := b.hash(key)
	b.mutex.RUnlock()

	bucket := b.buckets[bucketIndex]

	bucket.mutex.RLock()
	contains := bucket.filter.Test(key)
	bucket.mutex.RUnlock()
	return contains
}

func (b *BloomFilter) Load(_ context.Context, store bond.FilterStorer) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var keyBuff [1024]byte

	bucketNum, bucketNumCloser, err := store.Get(buildBucketNumKey(keyBuff[:0], b.keyPrefix))
	if err != nil || (err == nil && binary.BigEndian.Uint64(bucketNum) != uint64(b.numOfBuckets)) {
		if err == nil {
			_ = bucketNumCloser.Close()
		}
		return fmt.Errorf("configuration changed")
	}

	for _, bucket := range b.buckets {
		data, closer, err := store.Get(buildKey(keyBuff[:0], b.keyPrefix, bucket.num))
		if err != nil {
			return err
		}

		filter := bloom.New(0, 0)
		zr, err := zstd.NewReader(bytes.NewBuffer(data))
		if err != nil {
			return err
		}

		_, err = filter.ReadFrom(zr)
		if err != nil {
			return err
		}

		zr.Close()
		_ = closer.Close()

		// Lock the specific bucket before modifying it
		bucket.mutex.Lock()
		err = bucket.filter.Merge(filter)
		if err != nil {
			bucket.mutex.Unlock() // Ensure unlock on error
			return fmt.Errorf("configuration changed (incompatible filter parameters): %w", err)
		}
		bucket.hasBeenWritten = true
		bucket.mutex.Unlock()
	}

	return nil
}

func (b *BloomFilter) Save(_ context.Context, store bond.FilterStorer) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var keyBuff [1024]byte
	dataBuff := _buffPool.Get()
	defer _buffPool.Put(dataBuff[:0])

	binary.BigEndian.PutUint64(dataBuff[:8], uint64(b.numOfBuckets))
	err := store.Set(buildBucketNumKey(keyBuff[:0], b.keyPrefix), dataBuff[:8], bond.Sync)
	if err != nil {
		return err
	}

	for _, bucket := range b.buckets {
		// Lock the bucket early to check flags and potentially write
		bucket.mutex.Lock()
		if !bucket.hasChanges && bucket.hasBeenWritten {
			bucket.mutex.Unlock() // Unlock if we skip
			continue
		}

		// Use a temporary buffer from the pool for writing
		writeBuff := _buffPool.Get()
		buff := bytes.NewBuffer(writeBuff[:0]) // Use the pooled buffer

		zw, err := zstd.NewWriter(buff)
		if err != nil {
			return err
		}

		_, err = bucket.filter.WriteTo(zw)
		if err != nil {
			return err
		}

		err = zw.Close()
		if err != nil {
			return err
		}

		// Put the buffer back BEFORE potential error return from store.Set
		_buffPool.Put(writeBuff[:0])

		err = store.Set(
			buildKey(keyBuff[:0], b.keyPrefix, bucket.num),
			buff.Bytes(), // Use the compressed data from the buffer
			bond.Sync,
		)
		if err != nil {
			bucket.mutex.Unlock() // Ensure unlock on error
			return err
		}

		bucket.hasChanges = false
		bucket.hasBeenWritten = true
		bucket.mutex.Unlock() // Unlock after successful write and flag update
	}

	return nil
}

func (b *BloomFilter) Clear(_ context.Context, store bond.FilterStorer) error {
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
