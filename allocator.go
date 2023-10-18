package bond

import (
	"math/big"
	"sync"

	"github.com/cockroachdb/pebble"
)

var AllocatorMarker = []byte{'a', 'l', 'l', 'o', 'c', 'a', 't', 'o', 'r'}

type Allocator struct {
	TableID TableID
	max     *big.Int
	db      DB
	sync.Mutex
}

func getAllocatorKey(id TableID) []byte {
	key := make([]byte, 0, len(AllocatorMarker)+1)
	key = append(key, uint8(id))
	key = append(key, AllocatorMarker...)
	return key
}

func NewAllocator(id TableID, db DB) (*Allocator, error) {
	val, closer, err := db.Get(getAllocatorKey(id))
	if err != nil && err != pebble.ErrNotFound {
		return nil, err
	}
	var max *big.Int
	if err == nil {
		max = new(big.Int).SetBytes(val)
		closer.Close()
	}
	if err == pebble.ErrNotFound {
		max = big.NewInt(1)
		if err := db.Set(getAllocatorKey(id), max.Bytes(), Sync); err != nil {
			return nil, err
		}
	}
	return &Allocator{
		TableID: id,
		max:     max,
		db:      db,
	}, nil
}

func (a *Allocator) Alloc(n int64) {
	a.Lock()
	defer a.Unlock()
	start := new(big.Int).SetBytes(a.max.Bytes())
	a.max = new(big.Int).Add(a.max, big.NewInt(n))
	if err := a.db.Set(getAllocatorKey(a.TableID), a.max.Bytes(), Sync); err != nil {

	}
}
