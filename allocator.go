package bond

import (
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/pebble"
)

var AllocatorMarker = []byte{'a', 'l', 'l', 'o', 'c', 'a', 't', 'o', 'r'}

type Allocator struct {
	TableID TableID
	max     uint64
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
	var max uint64
	if err == nil {
		max = DecodeUint64(val)
		closer.Close()
	}
	if err == pebble.ErrNotFound {
		max = 1
		if err := db.Set(getAllocatorKey(id), EncodeUint64(max), Sync); err != nil {
			return nil, err
		}
	}
	return &Allocator{
		TableID: id,
		max:     max,
		db:      db,
	}, nil
}

func EncodeUint64(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func DecodeUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func (a *Allocator) Alloc(n uint64) (uint64, uint64, error) {
	a.Lock()
	defer a.Unlock()
	a.max += n
	if err := a.db.Set(getAllocatorKey(a.TableID), EncodeUint64(a.max), Sync); err != nil {
		return 0, 0, err
	}
	return a.max - n, a.max, nil
}
