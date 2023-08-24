package bond

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

type mergerWrapper struct {
	merge      func(old, new map[string]interface{}) (map[string]interface{}, map[string]struct{}, error)
	serializer Serializer[any]
}

type bondMerger struct {
	*pebble.Merger

	tables map[TableID]mergerWrapper
}

func newBondMerger(name string) *bondMerger {
	m := &bondMerger{
		tables: make(map[TableID]mergerWrapper),
	}
	m.Merger = &pebble.Merger{
		Name:  name,
		Merge: m.valueMerger,
	}
	return m
}

func (m *bondMerger) registerTable(tableID TableID, serializer Serializer[any], merge func(old, new map[string]interface{}) (map[string]interface{}, map[string]struct{}, error)) {
	m.tables[tableID] = mergerWrapper{
		merge:      merge,
		serializer: serializer,
	}
}

func (m *bondMerger) valueMerger(key, value []byte) (pebble.ValueMerger, error) {
	if mergeWrapped, ok := m.tables[KeyBytes(key).TableID()]; ok {
		var currentValue = map[string]interface{}{}
		err := mergeWrapped.serializer.Deserialize(value, &currentValue)
		if err != nil {
			return nil, err
		}

		return &bondValueMerger{
			merge:        mergeWrapped.merge,
			serializer:   mergeWrapped.serializer,
			currentValue: currentValue,
		}, nil
	}
	return nil, fmt.Errorf("unknown table id: %d", KeyBytes(key).TableID())
}

type bondValueMerger struct {
	merge      func(old, new map[string]interface{}) (map[string]interface{}, map[string]struct{}, error)
	serializer Serializer[any]

	fieldsUpdated map[string]struct{}

	currentValue map[string]interface{}
}

func (b *bondValueMerger) MergeNewer(value []byte) error {
	if b.currentValue == nil {
		err := b.serializer.Deserialize(value, &b.currentValue)
		if err != nil {
			return err
		}
		return nil
	} else {
		var newValue map[string]interface{}
		err := b.serializer.Deserialize(value, &newValue)
		if err != nil {
			return err
		}

		var fieldsUpdated map[string]struct{}
		b.currentValue, fieldsUpdated, err = b.merge(b.currentValue, newValue)
		if err != nil {
			return err
		}

		if b.fieldsUpdated == nil {
			b.fieldsUpdated = fieldsUpdated
		} else {
			for field := range fieldsUpdated {
				b.fieldsUpdated[field] = struct{}{}
			}
		}
		return nil
	}
}

func (b *bondValueMerger) MergeOlder(value []byte) error {
	if b.currentValue == nil {
		err := b.serializer.Deserialize(value, &b.currentValue)
		if err != nil {
			return err
		}
		return nil
	} else {
		var oldValue map[string]interface{}
		err := b.serializer.Deserialize(value, &oldValue)
		if err != nil {
			return err
		}

		var fieldsUpdated map[string]struct{}
		b.currentValue, fieldsUpdated, err = b.merge(oldValue, b.currentValue)
		if err != nil {
			return err
		}

		if b.fieldsUpdated == nil {
			b.fieldsUpdated = fieldsUpdated
		} else {
			for field := range fieldsUpdated {
				b.fieldsUpdated[field] = struct{}{}
			}
		}
		return nil
	}
}

func (b *bondValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if b.currentValue == nil {
		return nil, nil, fmt.Errorf("no value to finish")
	}

	if !includesBase {
		if b.fieldsUpdated != nil {
			for field := range b.currentValue {
				_, ok := b.fieldsUpdated[field]
				if !ok {
					delete(b.currentValue, field)
				}
			}
		}
	}

	data, err := b.serializer.Serialize(b.currentValue)
	if err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}

var _ pebble.ValueMerger = (*bondValueMerger)(nil)

var defaultMerger = newBondMerger("bond.default")
