package bond

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

type mergerWraper struct {
	Valid func(key []byte) bool
	Merge pebble.Merge
}

type bondMerger struct {
	sync.RWMutex
	mergers []*mergerWraper
	*pebble.Merger
}

func newBondMerger() *bondMerger {
	m := &bondMerger{
		mergers: make([]*mergerWraper, 0),
	}
	m.Merger = &pebble.Merger{
		Name:  "bond.merger",
		Merge: m.valueMerger,
	}
	return m
}

func (b *bondMerger) registerMerger(merger *mergerWraper) {
	b.Lock()
	defer b.Unlock()
	b.mergers = append(b.mergers, merger)
}

func (m *bondMerger) valueMerger(key, value []byte) (pebble.ValueMerger, error) {
	m.RLock()
	defer m.RLock()
	for _, merger := range m.mergers {
		if merger.Valid(key) {
			return merger.Merge(key, value)
		}
	}
	return nil, fmt.Errorf("no merger found for the key %x", key)
}

func indexMerger() *mergerWraper {
	return &mergerWraper{
		Valid: func(key []byte) bool {
			return key[1] != byte(PrimaryIndexID)
		},
		Merge: pebble.DefaultMerger.Merge,
	}
}

var defaultMerger = newBondMerger()

func init() {
	defaultMerger.registerMerger(indexMerger())
}
