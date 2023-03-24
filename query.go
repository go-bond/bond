package bond

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/go-bond/bond/utils"
)

// FilterFunc is the function template to be used for record filtering.
type FilterFunc[R any] func(r R) bool

// OrderLessFunc is the function template to be used for record sorting.
type OrderLessFunc[R any] func(r, r2 R) bool

// Query is the structure that is used to build record query.
//
// Example:
//
//	t.Query().
//		With(ContractTypeIndex, &Contract{ContractType: ContractTypeERC20}).
//		Filter(func(c *Contract) bool {
//			return c.Balance > 25
//		}).
//		Limit(50)
type Query[T any] struct {
	table         *_table[T]
	index         *Index[T]
	indexSelector T

	filterFunc    FilterFunc[T]
	orderLessFunc OrderLessFunc[T]
	offset        uint64
	limit         uint64
	afterSelector T
	isAfter       bool

	intersects []Query[T]
}

func newQuery[T any](t *_table[T], i *Index[T]) Query[T] {
	return Query[T]{
		table:         t,
		index:         i,
		indexSelector: utils.MakeNew[T](),
		afterSelector: utils.MakeNew[T](),
		filterFunc:    nil,
		orderLessFunc: nil,
		offset:        0,
		limit:         0,
		isAfter:       false,
	}
}

// Table returns table that is used by this query.
func (q Query[T]) Table() Table[T] {
	return q.table
}

// With selects index for query execution. If not stated the default index will
// be used. The index need to be supplemented with a record selector that has
// indexed and order fields set. This is very important as selector also defines
// the row at which we start the query.
//
// WARNING: if we have DESC order on ID field, and we try to query with a selector
// that has ID set to 0 it will start from the last row.
func (q Query[T]) With(idx *Index[T], selector T) Query[T] {
	q.index = idx
	q.indexSelector = selector
	return q
}

// Filter adds additional filtering to the query. The conditions can be built with
// structures that implement Evaluable interface.
func (q Query[T]) Filter(filter FilterFunc[T]) Query[T] {
	q.filterFunc = filter
	return q
}

// Order sets order of the records.
func (q Query[T]) Order(less OrderLessFunc[T]) Query[T] {
	q.orderLessFunc = less
	return q
}

// Offset sets offset of the records.
//
// WARNING: Using Offset requires traversing through all the rows
// that are skipped. This may take a long time. Bond allows to use
// more efficient way to do that by passing last received row to
// With method as a selector. This will jump to that row instantly
// and start iterating from that point.
func (q Query[T]) Offset(offset uint64) Query[T] {
	q.offset = offset
	return q
}

// Limit sets the maximal number of records returned.
//
// WARNING: If not defined it will return all rows. Please be
// mindful of your memory constrains.
func (q Query[T]) Limit(limit uint64) Query[T] {
	q.limit = limit
	return q
}

// After sets the query to start after the row provided in argument.
func (q Query[T]) After(sel T) Query[T] {
	q.afterSelector = sel
	q.isAfter = true
	return q
}

// Execute the built query.
func (q Query[T]) Execute(ctx context.Context, r *[]T, optBatch ...Batch) error {
	var err error
	var records []T
	if len(q.intersects) == 0 {
		records, err = q.executeQuery(ctx, optBatch...)
		if err != nil {
			return err
		}
	} else {
		records, err = q.executeIntersect(ctx, optBatch...)
		if err != nil {
			return err
		}
	}

	*r = records
	return nil
}

func (q Query[T]) Intersects(queries ...Query[T]) Query[T] {
	q.intersects = append(q.intersects, queries...)
	return q
}

func (q Query[T]) executeQuery(ctx context.Context, optBatch ...Batch) ([]T, error) {
	// todo: after works with ordered query
	if q.isAfter && q.orderLessFunc != nil {
		return nil, fmt.Errorf("after can not be used with order")
	}

	var (
		hasFilter = q.filterFunc != nil
		hasSort   = q.orderLessFunc != nil
		hasAfter  = q.isAfter
		hasOffset = q.offset > 0
		hasLimit  = q.limit > 0

		//filterApplied = false
		sortApplied   = false
		afterApplied  = false
		offsetApplied = false
		limitApplied  = false
	)

	// after
	selector := q.indexSelector
	if hasAfter && !hasSort {
		selector = q.afterSelector
	}

	var records []T
	count := uint64(0)
	skippedFirstRow := false
	err := q.table.ScanIndexForEach(ctx, q.index, selector, func(key KeyBytes, lazy Lazy[T]) (bool, error) {
		if q.isAfter && !hasSort && !skippedFirstRow {
			skippedFirstRow = true
			afterApplied = true

			keyBuffer := q.table.db.getKeyBufferPool().Get()
			defer q.table.db.getKeyBufferPool().Put(keyBuffer)

			rowIdxKey := key.ToKey()
			selIdxKey := KeyBytes(encodeIndexKey[T](q.table, selector, q.index, keyBuffer[:0])).ToKey()
			if bytes.Compare(selIdxKey.Index, rowIdxKey.Index) == 0 &&
				bytes.Compare(selIdxKey.IndexOrder, rowIdxKey.IndexOrder) == 0 &&
				bytes.Compare(selIdxKey.PrimaryKey, rowIdxKey.PrimaryKey) == 0 {
				return true, nil
			}
		}

		// check if can apply offset in here
		if !hasFilter && !hasSort && q.offset > count {
			offsetApplied = true
			count++
			return true, nil
		}

		// get and deserialize
		record, err := lazy.Get()
		if err != nil {
			return false, err
		}

		// filter if filter available
		if hasFilter {
			//filterApplied = true
			if q.filterFunc(record) {
				records = append(records, record)
				count++
			}
		} else {
			records = append(records, record)
			count++
		}

		next := true
		// check if we need to iterate further
		if !hasSort && hasLimit {
			limitApplied = true
			next = count < q.offset+q.limit
		}

		return next, nil
	}, optBatch...)
	if err != nil {
		return nil, err
	}

	// sorting
	if hasSort && !sortApplied {
		sort.Slice(records, func(i, j int) bool {
			return q.orderLessFunc(records[i], records[j])
		})
	}

	if hasAfter && !afterApplied {
		afterKey := q.table.PrimaryKey(NewKeyBuilder([]byte{}), q.indexSelector)
		recordKey := []byte{}
		for index, record := range records {
			recordKey = q.table.PrimaryKey(NewKeyBuilder(recordKey), record)
			if bytes.Compare(afterKey, recordKey) == 0 {
				records = records[minInt(index+1, len(records)):]
				break
			}
		}
	}

	// offset
	if hasOffset && !offsetApplied {
		if int(q.offset) >= len(records) {
			records = make([]T, 0)
		} else {
			records = records[q.offset:]
		}
	}

	// limit
	if hasLimit && !limitApplied {
		lastIndex := q.limit
		if int(lastIndex) >= len(records) {
			lastIndex = uint64(len(records))
		}
		records = records[:lastIndex]
	}

	return records, nil
}

func (q Query[T]) executeIntersect(ctx context.Context, optBatch ...Batch) ([]T, error) {
	var (
		hasFilter = q.filterFunc != nil
		hasSort   = q.orderLessFunc != nil
		hasAfter  = q.isAfter
		hasOffset = q.offset > 0
		hasLimit  = q.limit > 0

		//filterApplied = false
		sortApplied   = false
		offsetApplied = false
		limitApplied  = false
	)

	keys, err := q.index.Intersect(ctx, q.table, q.indexSelector, q.indexes(), q.selectors(), optBatch...)
	if err != nil {
		return nil, err
	}

	// apply offset & limit early
	if !hasFilter && !hasSort && !hasAfter {
		if hasOffset {
			offsetApplied = true
			if int(q.offset) >= len(keys) {
				return make([]T, 0), nil
			} else {
				keys = keys[q.offset:]
			}
		}

		if hasLimit {
			limitApplied = true
			lastIndex := q.limit
			if int(lastIndex) >= len(keys) {
				lastIndex = uint64(len(keys))
			}
			keys = keys[:lastIndex]
		}
	}

	// get data
	values, err := q.table.get(keys, nil, make([][]byte, len(keys)))
	if err != nil {
		return nil, err
	}

	var records = make([]T, 0)
	for _, value := range values {
		var record T
		err = q.table.Serializer().Deserialize(value, &record)
		if err != nil {
			return nil, err
		}

		if !hasFilter || (hasFilter && q.filterFunc(record)) {
			//filterApplied = true
			records = append(records, record)
		}
	}

	// sort
	if hasSort && !sortApplied {
		sort.Slice(records, func(i, j int) bool {
			return q.orderLessFunc(records[i], records[j])
		})
	}

	// after
	if hasAfter {
		afterKey := q.table.PrimaryKey(NewKeyBuilder([]byte{}), q.afterSelector)
		recordKey := []byte{}
		for index, record := range records {
			recordKey = q.table.PrimaryKey(NewKeyBuilder(recordKey), record)
			if bytes.Compare(afterKey, recordKey) == 0 {
				records = records[minInt(index+1, len(records)):]
				break
			}
		}
	}

	if hasFilter || hasSort {
		// offset
		if hasOffset && !offsetApplied {
			if int(q.offset) >= len(records) {
				records = make([]T, 0)
			} else {
				records = records[q.offset:]
			}
		}

		// limit
		if hasLimit && !limitApplied {
			lastIndex := q.limit
			if int(lastIndex) >= len(records) {
				lastIndex = uint64(len(records))
			}
			records = records[:lastIndex]
		}
	}

	return records, nil
}

func (q Query[T]) indexes() []*Index[T] {
	indexes := make([]*Index[T], 0, len(q.intersects))
	for _, inter := range q.intersects {
		indexes = append(indexes, inter.index)
	}
	return indexes
}

func (q Query[T]) selectors() []T {
	sels := make([]T, 0, len(q.intersects))
	for _, inter := range q.intersects {
		sels = append(sels, inter.indexSelector)
	}
	return sels
}
