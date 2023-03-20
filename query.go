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
		if q.shouldApplyOffsetEarly() && q.offset > count {
			count++
			return true, nil
		}

		// get and deserialize
		record, err := lazy.Get()
		if err != nil {
			return false, err
		}

		// filter if filter available
		if q.shouldFilter() {
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
		if !q.shouldSort() && q.shouldLimit() {
			next = count < q.offset+q.limit
		}

		return next, nil
	}, optBatch...)
	if err != nil {
		return err
	}

	// sorting
	if q.shouldSort() {
		sort.Slice(records, func(i, j int) bool {
			return q.orderLessFunc(records[i], records[j])
		})
	}

	// offset
	if !q.isOffsetApplied() {
		if int(q.offset) >= len(records) {
			records = make([]T, 0)
		} else {
			records = records[q.offset:]
		}
	}

	// limit
	if !q.isLimitApplied() && q.shouldLimit() {
		lastIndex := q.limit
		if int(lastIndex) >= len(records) {
			lastIndex = uint64(len(records))
		}
		records = records[:lastIndex]
	}

	*r = records

	return nil
}

func (q Query[T]) Intersects(queries ...Query[T]) Query[T] {
	q.intersects = append(q.intersects, queries...)
	return q
}

func (q Query[T]) shouldFilter() bool {
	return q.filterFunc != nil
}

func (q Query[T]) shouldSort() bool {
	return q.orderLessFunc != nil
}

func (q Query[T]) shouldApplyOffsetEarly() bool {
	return q.orderLessFunc == nil && q.filterFunc == nil
}

func (q Query[T]) shouldLimit() bool {
	return q.limit != 0
}

func (q Query[T]) isLimitApplied() bool {
	return q.orderLessFunc == nil
}

func (q Query[T]) isOffsetApplied() bool {
	return q.orderLessFunc == nil && q.filterFunc == nil
}
