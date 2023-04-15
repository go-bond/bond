package bond

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

// Evaluable is the interface used to simplify Query.Filter usage
type Evaluable[R any] interface {
	Eval(r R) bool
}

// Value is the interface used to simplify getting a value from a record
type Value[R any, V any] func(r R) V

type _eq[R any, V any] struct {
	Record Value[R, V]
	Equal  V
}

// Eval returns true if the record value is equal to the given value
func (e *_eq[R, V]) Eval(r R) bool {
	return assert.ObjectsAreEqual(e.Record(r), e.Equal)
}

// Eq returns an Evaluable that returns true if the record value is equal to the given value
func Eq[R any, V any](record Value[R, V], equal V) Evaluable[R] {
	return &_eq[R, V]{record, equal}
}

type _gt[R any, V constraints.Ordered] struct {
	Record  Value[R, V]
	Greater V
}

// Eval returns true if the record value is greater than the given value
func (g *_gt[R, V]) Eval(r R) bool {
	return g.Record(r) > g.Greater
}

// Gt returns an Evaluable that returns true if the record value is greater than the given value
func Gt[R any, V constraints.Ordered](record Value[R, V], greater V) Evaluable[R] {
	return &_gt[R, V]{record, greater}
}

type _gte[R any, V constraints.Ordered] struct {
	Record       Value[R, V]
	GreaterEqual V
}

// Eval returns true if the record value is greater than or equal to the given value
func (g *_gte[R, V]) Eval(r R) bool {
	return g.Record(r) >= g.GreaterEqual
}

// Gte returns an Evaluable that returns true if the record value is greater than or equal to the given value
func Gte[R any, V constraints.Ordered](record Value[R, V], greaterEqual V) Evaluable[R] {
	return &_gte[R, V]{record, greaterEqual}
}

type _lt[R any, V constraints.Ordered] struct {
	Record Value[R, V]
	Less   V
}

// Eval returns true if the record value is less than the given value
func (l *_lt[R, V]) Eval(r R) bool {
	return l.Record(r) < l.Less
}

// Lt returns an Evaluable that returns true if the record value is less than the given value
func Lt[R any, V constraints.Ordered](record Value[R, V], less V) Evaluable[R] {
	return &_lt[R, V]{record, less}
}

type _lte[R any, V constraints.Ordered] struct {
	Record    Value[R, V]
	LessEqual V
}

// Eval returns true if the record value is less than or equal to the given value
func (l *_lte[R, V]) Eval(r R) bool {
	return l.Record(r) <= l.LessEqual
}

// Lte returns an Evaluable that returns true if the record value is less than or equal to the given value
func Lte[R any, V constraints.Ordered](record Value[R, V], lessEqual V) Evaluable[R] {
	return &_lte[R, V]{record, lessEqual}
}

type _and[R any] []any

// Eval returns true if all the Evaluable return true
func (a *_and[R]) Eval(r R) bool {
	evalReturn := true
	for _, evaluable := range *a {
		evalReturn = evalReturn && evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}

// And returns an Evaluable that returns true if all the Evaluable return true
func And[R any](evalList ...Evaluable[R]) Evaluable[R] {
	var e _and[R]
	for _, eval := range evalList {
		e = append(e, eval)
	}
	return &e
}

type _or[R any] []any

// Eval returns true if any of the Evaluable return true
func (o *_or[R]) Eval(r R) bool {
	evalReturn := false
	for _, evaluable := range *o {
		evalReturn = evalReturn || evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}

// Or returns an Evaluable that returns true if any of the Evaluable return true
func Or[R any](evalList ...Evaluable[R]) Evaluable[R] {
	var e _or[R]
	for _, eval := range evalList {
		e = append(e, eval)
	}
	return &e
}

type _not[R any] struct {
	Evaluable Evaluable[R]
}

// Eval returns true if the Evaluable returns false
func (n *_not[R]) Eval(r R) bool {
	return !n.Evaluable.Eval(r)
}

// Not returns an Evaluable that returns true if the Evaluable returns false
func Not[R any](eval Evaluable[R]) Evaluable[R] {
	return &_not[R]{eval}
}

var _ Evaluable[any] = (*_eq[any, uint])(nil)
var _ Evaluable[any] = (*_gt[any, uint])(nil)
var _ Evaluable[any] = (*_gte[any, uint])(nil)
var _ Evaluable[any] = (*_lt[any, uint])(nil)
var _ Evaluable[any] = (*_lte[any, uint])(nil)
var _ Evaluable[any] = (*_and[any])(nil)
var _ Evaluable[any] = (*_or[any])(nil)
var _ Evaluable[any] = (*_not[any])(nil)
