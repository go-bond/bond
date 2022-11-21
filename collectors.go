package bond

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

const _tableIDCollectorName = "table_id"

type _tableIDCollector struct {
	tableIDs bitset.BitSet
}

func (t *_tableIDCollector) Add(key sstable.InternalKey, value []byte) error {
	t.tableIDs.Set(uint(KeyBytes(key.UserKey).TableID()))
	return nil
}

func (t *_tableIDCollector) Finish(userProps map[string]string) error {
	bs, err := t.tableIDs.MarshalBinary()
	if err != nil {
		return err
	}

	userProps[_tableIDCollectorName] = string(bs)
	return nil
}

func (t *_tableIDCollector) Name() string {
	return _tableIDCollectorName
}

func _filterTableID(tableID TableID) func(userProps map[string]string) bool {
	return func(userProps map[string]string) bool {
		var bs bitset.BitSet
		if bsData, ok := userProps[_tableIDCollectorName]; ok {
			err := bs.UnmarshalBinary([]byte(bsData))
			if err != nil {
				return true
			}

			return bs.Test(uint(tableID))
		} else {
			return true
		}
	}
}

var _ pebble.TablePropertyCollector = &_tableIDCollector{}

const _indexIDCollectorName = "index_id"

type _indexIDCollector struct {
	indexIDs bitset.BitSet
}

func (t *_indexIDCollector) Add(key sstable.InternalKey, value []byte) error {
	t.indexIDs.Set(uint(KeyBytes(key.UserKey).IndexID()))
	return nil
}

func (t *_indexIDCollector) Finish(userProps map[string]string) error {
	bs, err := t.indexIDs.MarshalBinary()
	if err != nil {
		return err
	}

	userProps[_indexIDCollectorName] = string(bs)
	return nil
}

func (t *_indexIDCollector) Name() string {
	return _indexIDCollectorName
}

func _filterIndexID(indexID IndexID) func(userProps map[string]string) bool {
	return func(userProps map[string]string) bool {
		var bs bitset.BitSet
		if bsData, ok := userProps[_indexIDCollectorName]; ok {
			err := bs.UnmarshalBinary([]byte(bsData))
			if err != nil {
				return true
			}

			return bs.Test(uint(indexID))
		} else {
			return true
		}
	}
}

var _ pebble.TablePropertyCollector = &_indexIDCollector{}

type _indexKeyCollector struct {
}

func (ik *_indexKeyCollector) Add(key sstable.InternalKey, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (ik *_indexKeyCollector) Finish(userProps map[string]string) error {
	//TODO implement me
	panic("implement me")
}

func (ik *_indexKeyCollector) Name() string {
	//TODO implement me
	panic("implement me")
}

var _ pebble.TablePropertyCollector = &_indexKeyCollector{}
