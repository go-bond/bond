package cond

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

// Cond is the interface used to simplify Query.Filter usage
type Cond[R any] interface {
	Eval(r R) bool
}

// RecordValueGetter is the function type used to getting a value from a record
type RecordValueGetter[R any, V any] func(r R) V

// CondFunc is the function type used to implement Cond interface
type CondFunc[R any] func(r R) bool

// Eval implements Cond interface.
func (e CondFunc[R]) Eval(r R) bool {
	return e(r)
}

// Func is the function template to be used for condition function
func Func[R any](f func(r R) bool) Cond[R] {
	return CondFunc[R](f)
}

type _eq[R any, V any] struct {
	RecordValue RecordValueGetter[R, V]
	Equal       V
}

// Eval returns true if the record value is equal to the given value
func (e *_eq[R, V]) Eval(r R) bool {
	return assert.ObjectsAreEqual(e.RecordValue(r), e.Equal)
}

// Eq returns an Cond that returns true if the record value is equal to the given value
func Eq[R any, V any](valueGetter RecordValueGetter[R, V], otherValue V) Cond[R] {
	return &_eq[R, V]{valueGetter, otherValue}
}

type _gt[R any, V constraints.Ordered] struct {
	RecordValue RecordValueGetter[R, V]
	OtherValue  V
}

// Eval returns true if the record value is greater than the given value
func (g *_gt[R, V]) Eval(r R) bool {
	return g.RecordValue(r) > g.OtherValue
}

// Gt returns an Cond that returns true if the record value is greater than the given value
func Gt[R any, V constraints.Ordered](valueGetter RecordValueGetter[R, V], otherValue V) Cond[R] {
	return &_gt[R, V]{valueGetter, otherValue}
}

type _gte[R any, V constraints.Ordered] struct {
	RecordValue RecordValueGetter[R, V]
	OtherValue  V
}

// Eval returns true if the record value is greater than or equal to the given value
func (g *_gte[R, V]) Eval(r R) bool {
	return g.RecordValue(r) >= g.OtherValue
}

// Gte returns an Cond that returns true if the record value is greater than or equal to the given value
func Gte[R any, V constraints.Ordered](valueGetter RecordValueGetter[R, V], otherValue V) Cond[R] {
	return &_gte[R, V]{valueGetter, otherValue}
}

type _lt[R any, V constraints.Ordered] struct {
	RecordValue RecordValueGetter[R, V]
	OtherValue  V
}

// Eval returns true if the record value is less than the given value
func (l *_lt[R, V]) Eval(r R) bool {
	return l.RecordValue(r) < l.OtherValue
}

// Lt returns an Cond that returns true if the record value is less than the given value
func Lt[R any, V constraints.Ordered](valueGetter RecordValueGetter[R, V], otherValue V) Cond[R] {
	return &_lt[R, V]{valueGetter, otherValue}
}

type _lte[R any, V constraints.Ordered] struct {
	RecordValue RecordValueGetter[R, V]
	OtherValue  V
}

// Eval returns true if the record value is less than or equal to the given value
func (l *_lte[R, V]) Eval(r R) bool {
	return l.RecordValue(r) <= l.OtherValue
}

// Lte returns an Cond that returns true if the record value is less than or equal to the given value
func Lte[R any, V constraints.Ordered](valueGetter RecordValueGetter[R, V], otherValue V) Cond[R] {
	return &_lte[R, V]{valueGetter, otherValue}
}

type _and[R any] []any

// Eval returns true if all the Cond return true
func (a *_and[R]) Eval(r R) bool {
	evalReturn := true
	for _, evaluable := range *a {
		evalReturn = evalReturn && evaluable.(Cond[R]).Eval(r)
	}
	return evalReturn
}

// And returns an Cond that returns true if all the Cond return true
func And[R any](evalList ...Cond[R]) Cond[R] {
	var e _and[R]
	for _, eval := range evalList {
		e = append(e, eval)
	}
	return &e
}

type _or[R any] []any

// Eval returns true if any of the Cond return true
func (o *_or[R]) Eval(r R) bool {
	evalReturn := false
	for _, evaluable := range *o {
		evalReturn = evalReturn || evaluable.(Cond[R]).Eval(r)
	}
	return evalReturn
}

// Or returns an Cond that returns true if any of the Cond return true
func Or[R any](evalList ...Cond[R]) Cond[R] {
	var e _or[R]
	for _, eval := range evalList {
		e = append(e, eval)
	}
	return &e
}

type _not[R any] struct {
	Evaluable Cond[R]
}

// Eval returns true if the Cond returns false
func (n *_not[R]) Eval(r R) bool {
	return !n.Evaluable.Eval(r)
}

// Not returns an Cond that returns true if the Cond returns false
func Not[R any](eval Cond[R]) Cond[R] {
	return &_not[R]{eval}
}

var _ Cond[any] = (*_eq[any, uint])(nil)
var _ Cond[any] = (*_gt[any, uint])(nil)
var _ Cond[any] = (*_gte[any, uint])(nil)
var _ Cond[any] = (*_lt[any, uint])(nil)
var _ Cond[any] = (*_lte[any, uint])(nil)
var _ Cond[any] = (*_and[any])(nil)
var _ Cond[any] = (*_or[any])(nil)
var _ Cond[any] = (*_not[any])(nil)
