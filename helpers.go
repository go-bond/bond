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
	mu     sync.Mutex
}

func (n *NumberSequence) Next() (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

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

func (n *NumberSequence) Timestamp(ns uint64) uint64 {
	return ns >> NumberSequenceBitShift
}

func (n *NumberSequence) SequenceNumber(ns uint64) uint64 {
	return ns & NumberSequenceSequenceNumberMask
}

type UUIDGenerator struct {
}

func (n *UUIDGenerator) Next() (uuid.UUID, error) {
	return uuid.New(), nil
}
