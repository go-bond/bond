package bond

// SelectorType is the type of selector.
type SelectorType uint8

const (
	SelectorTypePoint SelectorType = iota
	SelectorTypeMultiPoint
	SelectorTypeRange
	SelectorTypeRanges
)

// Selector is the interface for all selectors.
type Selector[T any] interface {
	Type() SelectorType
}

// SelectorPoint is the interface for point selector.
type SelectorPoint[T any] interface {
	Selector[T]
	Point() T
}

// SelectorPoints is the interface for multi-point selector.
type SelectorPoints[T any] interface {
	Selector[T]
	Points() []T
}

// SelectorRange is the interface for range selector.
// The range is represented as a two-element slice.
// The first element is the start of the range, and the second element is the end of the range.
type SelectorRange[T any] interface {
	Selector[T]
	Range() (T, T)
}

// SelectorRanges is the interface for multi-range selector.
// The ranges are represented as a slice of two-element slices.
// The first element of each slice is the start of the range, and the second element is the end of the range.
type SelectorRanges[T any] interface {
	Selector[T]
	Ranges() [][]T
}

type selectorPoint[T any] struct {
	point T
}

// NewSelectorPoint creates a new point selector.
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

// NewSelectorPoints creates a new multi-point selector.
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

// NewSelectorRange creates a new range selector.
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

// NewSelectorRanges creates a new multi-range selector.
func NewSelectorRanges[T any](ranges ...[]T) SelectorRanges[T] {
	return &selectorRanges[T]{ranges: ranges}
}

func (s *selectorRanges[T]) Type() SelectorType {
	return SelectorTypeRanges
}

func (s *selectorRanges[T]) Ranges() [][]T {
	return s.ranges
}
