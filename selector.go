package bond

// Selector is a structure that represents row template or a range of
// row templates that is used to select rows from a table. It can point
// to a single row or a range of rows defined by a first and last row.
//
// The row templates need to contain fields that are part of the primary
// key or index key depending on the context.
//
// Example:
//
//		var t Table[Contract]
//
//		t.Get(NewPointSelector(Contract{ID: 1})) // points to a single row
//		t.Get(NewRangeSelector(Contract{ID: 1}, Contract{ID: 10})) // points to a range of rows
//
//	 	t.Query(idx, NewPointSelector(Contract{ID: 1})) // points to a single index bucket
//	 	t.Query(idx, NewRangeSelector(Contract{ID: 1}, Contract{ID: 10})) // points to a range of index buckets
type Selector[T any] struct {
	first, last T
	points      []T

	isPoint      bool
	isMultiPoint bool
	isRange      bool
	isSet        bool
}

// First returns the first row template of the selector.
func (s *Selector[T]) First() T {
	return s.first
}

// Last returns the last row template of the selector.
func (s *Selector[T]) Last() T {
	return s.last
}

// IsPoint returns true if the selector points to a single row.
func (s *Selector[T]) IsPoint() bool {
	return s.isPoint
}

// IsMultiPoint returns true if the selector points to multiple rows.
func (s *Selector[T]) IsMultiPoint() bool {
	return s.isMultiPoint
}

// IsRange returns true if the selector points to a range of rows.
func (s *Selector[T]) IsRange() bool {
	return s.isRange
}

// IsSet returns true if the selector is set.
func (s *Selector[T]) IsSet() bool {
	return s.isSet
}

func SelectorNotSet[T any]() Selector[T] {
	return Selector[T]{}
}

// NewPointSelector creates a new selector that points to a single row.
func NewPointSelector[T any](first T) Selector[T] {
	return Selector[T]{first: first, last: first, isPoint: true, isSet: true}
}

// NewMultiPointsSelector creates a new selector that points to multiple rows.
func NewMultiPointsSelector[T any](points ...T) Selector[T] {
	return Selector[T]{points: points, isMultiPoint: true, isSet: true}
}

// NewRangeSelector creates a new selector that points to a range of rows.
func NewRangeSelector[T any](first, last T) Selector[T] {
	return Selector[T]{first: first, last: last, isRange: true, isSet: true}
}

// Selectors is a list of selectors.
type Selectors[T any] []Selector[T]
