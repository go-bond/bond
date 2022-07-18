package bond

import (
	"sort"
)

// FilterFunc is the function template to be used for record filtering.
type FilterFunc[R any] func(r R) bool

// FilterAndIndex the pair of evaluable and index on which query is executed.
type FilterAndIndex[R any] struct {
	FilterFunc    FilterFunc[R]
	Index         *Index[R]
	IndexSelector R
}

// OrderLessFunc is the function template to be used for record sorting.
type OrderLessFunc[R any] func(r, r2 R) bool

// Query is the structure that is used to build record query.
//
// Example:
//	bond.Query[*Contract]{}.
//		With(ContractTypeIndex, &Contract{ContractType: ContractTypeERC20}).
//		Where(&bond.Gt[*Contract, uint64]{
//			Record: func(c *Contract) uint64 {
//				return c.Balance
//			},
//			Greater: 10,
//		}).
//		Limit(50)
//
type Query[R any] struct {
	table         *Table[R]
	index         *Index[R]
	indexSelector R

	queries       []FilterAndIndex[R]
	orderLessFunc OrderLessFunc[R]
	offset        uint64
	limit         uint64
}

func newQuery[R any](t *Table[R], i *Index[R]) Query[R] {
	return Query[R]{
		table:         t,
		index:         i,
		queries:       []FilterAndIndex[R]{},
		orderLessFunc: nil,
		offset:        0,
		limit:         0,
	}
}

// With selects index for query execution. If not stated the default index will
// be used. The index need to be supplemented with a record selector that has
// indexed fields set.
func (q Query[R]) With(idx *Index[R], selector R) Query[R] {
	q.index = idx
	q.indexSelector = selector
	return q
}

// Filter adds additional filtering to the query. The conditions can be built with
// structures that implement Evaluable interface.
func (q Query[R]) Filter(filter FilterFunc[R]) Query[R] {
	newWhere := make([]FilterAndIndex[R], 0, len(q.queries)+1)
	q.queries = append(append(newWhere, q.queries...), FilterAndIndex[R]{
		FilterFunc:    filter,
		Index:         q.index,
		IndexSelector: q.indexSelector,
	})
	return q
}

// Order sets order of the records.
func (q Query[R]) Order(less OrderLessFunc[R]) Query[R] {
	q.orderLessFunc = less
	return q
}

// Offset sets offset of the records.
func (q Query[R]) Offset(offset uint64) Query[R] {
	q.offset = offset
	return q
}

// Limit sets the maximal number of records returned.
func (q Query[R]) Limit(limit uint64) Query[R] {
	q.limit = limit
	return q
}

// Execute the built query.
func (q Query[R]) Execute(r *[]R) error {
	if len(q.queries) == 0 {
		q.queries = append([]FilterAndIndex[R]{
			{
				FilterFunc:    func(r R) bool { return true },
				Index:         q.index,
				IndexSelector: q.indexSelector,
			},
		})
	}

	var records []R
	for _, query := range q.queries {
		err := q.table.ScanIndexForEach(query.Index, query.IndexSelector, func(record R) {
			if query.FilterFunc(record) {
				records = append(records, record)
			}
		})
		if err != nil {
			return err
		}
	}

	// sorting
	if q.orderLessFunc != nil {
		sort.Slice(records, func(i, j int) bool {
			return q.orderLessFunc(records[i], records[j])
		})
	}

	// offset
	if int(q.offset) >= len(records) {
		*r = make([]R, 0)
		return nil
	}

	// limit
	lastIndex := q.offset + q.limit
	if int(lastIndex) >= len(records) {
		lastIndex = uint64(len(records))
	}

	*r = records[q.offset:lastIndex]

	return nil
}
