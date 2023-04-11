package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelector(t *testing.T) {
	selector := NewSelectorPoint(1)

	assert.Equal(t, SelectorTypePoint, selector.Type())
	assert.Equal(t, 1, selector.Point())

	selectorPoints := NewSelectorPoints([]int{1, 2, 3}...)

	assert.Equal(t, SelectorTypePoints, selectorPoints.Type())
	assert.Equal(t, []int{1, 2, 3}, selectorPoints.Points())

	selectorRange := NewSelectorRange(1, 2)

	start, end := selectorRange.Range()
	assert.Equal(t, SelectorTypeRange, selectorRange.Type())
	assert.Equal(t, 1, start)
	assert.Equal(t, 2, end)

	selectorRanges := NewSelectorRanges([][]int{{1, 2}, {3, 4}}...)

	assert.Equal(t, SelectorTypeRanges, selectorRanges.Type())
	assert.Equal(t, [][]int{{1, 2}, {3, 4}}, selectorRanges.Ranges())
}
