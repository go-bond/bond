package filters

import (
	"context"

	"github.com/bits-and-blooms/bloom"
	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
)

type SingleBloomFilter struct {
	filter *bloom.BloomFilter
}

// NewSingleBloomFilter the bloom filter with capacity n and false-positive ration fp. The filter
// is initialized at creation time with the data from the database. The filter is in-memory only.
// It's not saved anywhere.
func NewSingleBloomFilter(db bond.DB, tableID bond.TableID, n uint, fp float64) *SingleBloomFilter {
	filter := bloom.NewWithEstimates(n, fp)

	iter := db.Iter(&bond.IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: []byte{byte(tableID)},
		},
	})
	for iter.First(); iter.Valid(); iter.Next() {
		key := bond.KeyBytes(iter.Key())
		keyTableID := key.TableID()
		keyIndexID := key.IndexID()

		if tableID != 0 && keyTableID != tableID {
			break
		}

		if keyTableID != 0 && keyIndexID != 0 {
			continue
		}

		filter.Add(key)
	}

	_ = iter.Close()

	return &SingleBloomFilter{
		filter: filter,
	}
}

func (s *SingleBloomFilter) Add(_ context.Context, key []byte) {
	s.filter.Add(key)
}

func (s *SingleBloomFilter) MayContain(_ context.Context, key []byte) bool {
	return s.filter.Test(key)
}
