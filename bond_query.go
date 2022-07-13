package bond

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

type Evaluable[T any] interface {
	Eval(t T) bool
}

type Value[T any, V any] func(t T) V

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

type And[T any] []any

func (a *And[R]) Eval(r R) bool {
	evalReturn := true
	for _, evaluable := range *a {
		evalReturn = evalReturn && evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}

type Or[T any] []any

func (o *Or[R]) Eval(r R) bool {
	evalReturn := false
	for _, evaluable := range *o {
		evalReturn = evalReturn || evaluable.(Evaluable[R]).Eval(r)
	}
	return evalReturn
}
