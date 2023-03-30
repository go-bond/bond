package bond

type SelectorType uint8

const (
	SelectorTypePoint SelectorType = iota
	SelectorTypeMultiPoint
	SelectorTypeRange
	SelectorTypeRanges
)

type Selector[T any] interface {
	Type() SelectorType
}
type SelectorPoint[T any] interface {
	Selector[T]
	Point() T
}

type SelectorPoints[T any] interface {
	Selector[T]
	Points() []T
}

type SelectorRange[T any] interface {
	Selector[T]
	Range() (T, T)
}

type SelectorRanges[T any] interface {
	Selector[T]
	Ranges() [][]T
}

type selectorPoint[T any] struct {
	point T
}

func NewSelectorPoint[T any](point T) SelectorPoint[T] {
	return &selectorPoint[T]{point: point}
}

func (s *selectorPoint[T]) Type() SelectorType {
	return SelectorTypePoint
}

func (s *selectorPoint[T]) Point() T {
	return s.point
}

type selectorPoints[T any] struct {
	points []T
}

func NewSelectorPoints[T any](points ...T) SelectorPoints[T] {
	return &selectorPoints[T]{points: points}
}

func (s *selectorPoints[T]) Type() SelectorType {
	return SelectorTypeMultiPoint
}

func (s *selectorPoints[T]) Points() []T {
	return s.points
}

type selectorRange[T any] struct {
	start T
	end   T
}

func NewSelectorRange[T any](start, end T) SelectorRange[T] {
	return &selectorRange[T]{start: start, end: end}
}

func (s *selectorRange[T]) Type() SelectorType {
	return SelectorTypeRange
}

func (s *selectorRange[T]) Range() (T, T) {
	return s.start, s.end
}

type selectorRanges[T any] struct {
	ranges [][]T
}

func NewSelectorRanges[T any](ranges ...[]T) SelectorRanges[T] {
	return &selectorRanges[T]{ranges: ranges}
}

func (s *selectorRanges[T]) Type() SelectorType {
	return SelectorTypeRanges
}

func (s *selectorRanges[T]) Ranges() [][]T {
	return s.ranges
}
