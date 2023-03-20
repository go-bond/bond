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
	isAfter       bool

	intersects []Query[T]
}

func newQuery[T any](t *_table[T], i *Index[T]) Query[T] {
	return Query[T]{
		table:         t,
		index:         i,
		indexSelector: utils.MakeNew[T](),
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
	q.indexSelector = sel
	q.isAfter = true
	return q
}

// Execute the built query.
func (q Query[T]) Execute(ctx context.Context, r *[]T, optBatch ...Batch) error {
	// todo: after works with ordered query
	if q.isAfter && q.orderLessFunc != nil {
		return fmt.Errorf("after can not be used with order")
	}

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
	var (
		hasFilter = q.filterFunc != nil
		hasSort   = q.orderLessFunc != nil
		hasOffset = q.offset > 0
		hasLimit  = q.limit > 0

		//filterApplied = false
		sortApplied   = false
		offsetApplied = false
		limitApplied  = false
	)

	var records []T
	count := uint64(0)
	skippedFirstRow := false
	err := q.table.ScanIndexForEach(ctx, q.index, q.indexSelector, func(key KeyBytes, lazy Lazy[T]) (bool, error) {
		if q.isAfter && !skippedFirstRow {
			skippedFirstRow = true

			keyBuffer := q.table.db.getKeyBufferPool().Get()
			defer q.table.db.getKeyBufferPool().Put(keyBuffer)

			rowIdxKey := key.ToKey()
			selIdxKey := KeyBytes(q.table.indexKey(q.indexSelector, q.index, keyBuffer[:0])).ToKey()
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
		hasOffset = q.offset > 0
		hasLimit  = q.limit > 0

		//filterApplied = false
		sortApplied   = false
		offsetApplied = false
		limitApplied  = false
	)

	keys, err := q.intersectKeys(ctx, optBatch...)
	if err != nil {
		return nil, err
	}

	// apply offset & limit early
	if !hasFilter && !hasSort {
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

func (q Query[T]) intersectKeys(ctx context.Context, optBatch ...Batch) ([][]byte, error) {
	var tempKeys [][]byte
	intersectKeys, err := q.scanKeys(ctx, optBatch...)
	if err != nil {
		return nil, err
	}

	for _, q2 := range q.intersects {
		err = q2.scanKeysForEach(ctx, func(key KeyBytes) (bool, error) {
			for _, iKey := range intersectKeys {
				if bytes.Compare(iKey, key) == 0 {
					tempKeys = append(tempKeys, key)
				}
			}
			return true, nil
		}, optBatch...)
		if err != nil {
			return nil, err
		}

		temp := intersectKeys[:0]
		intersectKeys = tempKeys
		tempKeys = temp
	}

	return intersectKeys, nil
}

func (q Query[T]) scanKeys(ctx context.Context, optBatch ...Batch) ([][]byte, error) {
	var keys [][]byte
	err := q.scanKeysForEach(ctx, func(key KeyBytes) (bool, error) {
		keys = append(keys, key.ToDataKeyBytes([]byte{}))
		return true, nil
	}, optBatch...)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (q Query[T]) scanKeysForEach(ctx context.Context, f func(key KeyBytes) (bool, error), optBatch ...Batch) error {
	err := q.table.ScanIndexForEach(ctx, q.index, q.indexSelector, func(key KeyBytes, lazy Lazy[T]) (bool, error) {
		return f(key.ToDataKeyBytes([]byte{}))
	}, optBatch...)
	return err
}
