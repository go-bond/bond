package bond

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

type Evaluable[R any] interface {
	Eval(r R) bool
}

type Value[R any, V any] func(r R) V

type Eq[R any, V any] struct {
	Record Value[R, V]
	Equal  V
}

func (e *Eq[R, V]) Eval(r R) bool {
	return assert.ObjectsAreEqual(e.Record(r), e.Equal)
}

type Gt[R any, V constraints.Ordered] struct {
	Record  Value[R, V]
	Greater V
}

func (g *Gt[R, V]) Eval(r R) bool {
	return g.Record(r) > g.Greater
}

type Gte[R any, V constraints.Ordered] struct {
	Record       Value[R, V]
	GreaterEqual V
}

func (g *Gte[R, V]) Eval(r R) bool {
	return g.Record(r) >= g.GreaterEqual
}

type Lt[R any, V constraints.Ordered] struct {
	Record Value[R, V]
	Less   V
}

func (l *Lt[R, V]) Eval(r R) bool {
	return l.Record(r) < l.Less
}

type Lte[R any, V constraints.Ordered] struct {
	Record    Value[R, V]
	LessEqual V
}

func (l *Lte[R, V]) Eval(r R) bool {
	return l.Record(r) <= l.LessEqual
}

type And[R any] []any

func (a *And[R]) Eval(r R) bool {
	evalReturn := true
	for _, evaluable := range *a {
		evalReturn = evalReturn && evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}

type Or[R any] []any

func (o *Or[R]) Eval(r R) bool {
	evalReturn := false
	for _, evaluable := range *o {
		evalReturn = evalReturn || evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}

// OrderLessFunc is the function template to be used for sorting.
type OrderLessFunc[R any] func(r, r2 R) bool

// Query is the structure that is used to build record query.
//
// Example:
//		bond.Query[*Contract]{}.
//			With(ContractTypeIndex, &Contract{ContractType: ContractTypeERC20}).
//			Where(&bond.Gt[*Contract, uint64]{
//				Record: func(c *Contract) uint64 {
//					return c.Balance
//				},
//				Greater: 10,
//			}).
//			Limit(50)
//
type Query[R any] struct {
}

// With selects index for query execution. If not stated the default index will
// be used. The index need to be supplemented with a record selector that has
// indexed fields set.
func (q Query[R]) With(idx *Index[R], selector R) Query[R] {
	panic("implement me!")
}

// Where adds additional filtering to the query. The conditions can be built with
// structures that implement Evaluable interface.
func (q Query[R]) Where(evaluable Evaluable[R]) Query[R] {
	panic("implement me!")
}

// Order sets order of the records.
func (q Query[R]) Order(less OrderLessFunc[R]) Query[R] {
	panic("implement me!")
}

// Offset sets offset of the records.
func (q Query[R]) Offset(offset uint64) Query[R] {
	panic("implement me!")
}

// Limit sets the maximal number of records returned.
func (q Query[R]) Limit(limit uint64) Query[R] {
	panic("implement me!")
}

// Execute the built query.
func (q Query[R]) Execute(a interface{}) error {
	panic("implement me!")
}
