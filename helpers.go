package bond

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
)

type UniqueKeyGenerator[T any] interface {
	Next() (T, error)
}

const NumberSequenceTimestampMask = 0xFFFFFFFFFF000000
const NumberSequenceBitShift = 24
const NumberSequenceSequenceNumberMask = 0x0000000000FFFFFF

type NumberSequence struct {
	lastId uint64
	mutex  sync.Mutex
}

func (n *NumberSequence) Next() (uint64, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	nextId := uint64(time.Now().Unix() << NumberSequenceBitShift)
	if n.lastId&NumberSequenceTimestampMask == nextId&NumberSequenceTimestampMask {
		if n.lastId&NumberSequenceSequenceNumberMask == NumberSequenceSequenceNumberMask {
			return math.MaxUint64, fmt.Errorf("sequence number overflow")
		}

		nextId = n.lastId + 1
		n.lastId = nextId
	} else {
		n.lastId = nextId
	}

	return nextId, nil
}

type UUIDGenerator struct {
}

func (n *UUIDGenerator) Next() (uuid.UUID, error) {
	return uuid.New(), nil
}

type Lazy[T any] struct {
	getFunc func() (T, error)
}

func (l Lazy[T]) Get() (T, error) {
	return l.getFunc()
}
