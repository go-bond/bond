package bond

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/go-bond/bond/utils"
)

type _evaluableFunc[R any] func(r R) bool

// Eval implements Evaluable interface.
func (e _evaluableFunc[R]) Eval(r R) bool {
	return e(r)
}

// EvaluableFunc is the function template to be used for record filtering.
func EvaluableFunc[R any](f func(r R) bool) Evaluable[R] {
	return _evaluableFunc[R](f)
}

// OrderLessFunc is the function template to be used for record sorting.
type OrderLessFunc[R any] func(r, r2 R) bool

// Query is the structure that is used to build record query.
//
// Example:
//
//	t.Query().
//		With(ContractTypeIndex, bond.NewSelectorPoint(&Contract{ContractType: ContractTypeERC20})).
//		Filter(func(c *Contract) bool {
//			return c.Balance > 25
//		}).
//		Limit(50)
type Query[T any] struct {
	table         *_table[T]
	index         *Index[T]
	indexSelector Selector[T]
	reverse       bool

	filterFunc    _evaluableFunc[T]
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
		indexSelector: NewSelectorPoint[T](utils.MakeNew[T]()),
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

// EvaluableFuncType returns the type of the filter function.
func (q Query[T]) EvaluableFuncType() reflect.Type {
	return reflect.TypeOf(q.filterFunc)
}

// With selects index for query execution. If not stated the default index will
// be used. The index need to be supplemented with a record selector that has
// indexed and order fields set. This is very important as selector also defines
// the row at which we start the query.
//
// WARNING: if we have DESC order on ID field, and we try to query with a selector
// that has ID set to 0 it will start from the last row.
func (q Query[T]) With(idx *Index[T], selector Selector[T]) Query[T] {
	q.index = idx
	q.indexSelector = selector
	return q
}

// Filter adds additional filtering to the query. The conditions can be built with
// structures that implement Evaluable interface.
func (q Query[T]) Filter(filter Evaluable[T]) Query[T] {
	q.filterFunc = filter.Eval
	return q
}

// Order sets order of the records.
func (q Query[T]) Order(less OrderLessFunc[T]) Query[T] {
	q.orderLessFunc = less
	return q
}

// Reverse reverses the order of the records.
// If not defined the order will be ASC.
// If defined it will be DESC.
// If defined twice it will be ASC again.
func (q Query[T]) Reverse() Query[T] {
	q.reverse = !q.reverse
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
		var err error
		selector, err = q.selectorWithAfter()
		if err != nil {
			return nil, err
		}
	}

	var records = make([]T, 0, DefaultScanPrefetchSize)
	count := uint64(0)
	skippedFirstRow := false
	err := q.table.ScanIndexForEach(ctx, q.index, selector, func(key KeyBytes, lazy Lazy[T]) (bool, error) {
		if q.isAfter && !hasSort && !skippedFirstRow {
			skippedFirstRow = true
			afterApplied = true

			keyBuffer := q.table.db.getKeyBufferPool().Get()
			defer q.table.db.getKeyBufferPool().Put(keyBuffer)

			rowIdxKey := key.ToKey()
			selIdxKey := KeyBytes(encodeIndexKey[T](q.table, q.afterSelector, q.index, keyBuffer[:0])).ToKey()
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
	}, q.reverse, optBatch...)
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
		afterKey := q.table.PrimaryKey(NewKeyBuilder([]byte{}), q.afterSelector)
		recordKey := []byte{}
		for index, record := range records {
			recordKey = q.table.PrimaryKey(NewKeyBuilder(recordKey[:0]), record)
			if bytes.Equal(afterKey, recordKey) {
				if index+1 == len(records) {
					records = make([]T, 0)
				} else {
					records = records[index+1:]
				}
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

	if err := q.checkIntersectQueries(); err != nil {
		return nil, err
	}

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
	values, err := q.table.get(keys, nil, make([][]byte, len(keys)), true)
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
			recordKey = q.table.PrimaryKey(NewKeyBuilder(recordKey[:0]), record)
			if bytes.Equal(afterKey, recordKey) {
				if index+1 == len(records) {
					records = make([]T, 0)
				} else {
					records = records[index+1:]
				}
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

func (q Query[T]) checkIntersectQueries() error {
	for _, q1 := range q.intersects {
		if q1.filterFunc != nil {
			return fmt.Errorf("queries passed to Intersect do not support filter")
		}
		if q1.orderLessFunc != nil {
			return fmt.Errorf("queries passed to Intersect do not support order")
		}
		if q1.isAfter {
			return fmt.Errorf("queries passed to Intersect do not support after")
		}
		if q1.offset != 0 {
			return fmt.Errorf("queries passed to Intersect do not support offset")
		}
		if q1.limit != 0 {
			return fmt.Errorf("queries passed to Intersect do not support limit")
		}
	}
	return nil
}

func (q Query[T]) indexes() []*Index[T] {
	indexes := make([]*Index[T], 0, len(q.intersects))
	for _, inter := range q.intersects {
		indexes = append(indexes, inter.index)
	}
	return indexes
}

func (q Query[T]) selectors() []Selector[T] {
	sels := make([]Selector[T], 0, len(q.intersects))
	for _, inter := range q.intersects {
		sels = append(sels, inter.indexSelector)
	}
	return sels
}

func (q Query[T]) selectorWithAfter() (Selector[T], error) {
	switch q.indexSelector.Type() {
	case SelectorTypePoint:
		return NewSelectorPoint(q.afterSelector), nil
	case SelectorTypePoints:
		afterKey := encodeIndexKey[T](q.table, q.afterSelector, q.index, []byte{})

		var newPoints []T
		pntsSelector := q.indexSelector.(SelectorPoints[T])
		for _, pnt := range pntsSelector.Points() {
			pntKey := encodeIndexKey[T](q.table, pnt, q.index, []byte{})
			if bytes.Compare(afterKey, pntKey) < 0 {
				newPoints = append(newPoints, pnt)
			} else if bytes.Compare(afterKey, pntKey) >= 0 &&
				bytes.Compare(afterKey[:_KeyPrefixSplitIndex(afterKey)], pntKey[:_KeyPrefixSplitIndex(pntKey)]) == 0 {
				newPoints = append(newPoints, q.afterSelector)
			}
		}
		return NewSelectorPoints(newPoints...), nil
	case SelectorTypeRange:
		rngSelector := q.indexSelector.(SelectorRange[T])

		_, upper := rngSelector.Range()
		return NewSelectorRange(q.afterSelector, upper), nil
	case SelectorTypeRanges:
		afterKey := encodeIndexKey[T](q.table, q.afterSelector, q.index, []byte{})

		var newRanges [][]T
		rngsSelector := q.indexSelector.(SelectorRanges[T])
		for _, rng := range rngsSelector.Ranges() {
			lowerKey := encodeIndexKey[T](q.table, rng[0], q.index, []byte{})
			upperKey := encodeIndexKey[T](q.table, rng[1], q.index, []byte{})

			if bytes.Compare(afterKey, lowerKey) < 0 {
				newRanges = append(newRanges, rng)
			} else if bytes.Compare(afterKey, lowerKey) >= 0 && bytes.Compare(afterKey, upperKey) <= 0 {
				newRanges = append(newRanges, []T{q.afterSelector, rng[1]})
			}
		}
		return NewSelectorRanges(newRanges...), nil
	default:
		return nil, fmt.Errorf("unsupported selector type")
	}
}
