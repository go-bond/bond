package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelector(t *testing.T) {
	// contract struct for testing
	type Contract struct {
		ID int
	}

	// selector test cases
	selectorTestCases := []struct {
		name string
		in   Contract
		want Selector[Contract]
	}{
		{
			name: "NewPointSelector",
			in:   Contract{ID: 1},
			want: Selector[Contract]{first: Contract{ID: 1}, last: Contract{ID: 1}, isPoint: true, isSet: true},
		},
		{
			name: "NewMultiPointsSelector",
			in:   Contract{ID: 1},
			want: Selector[Contract]{points: []Contract{{ID: 1}, {ID: 2}, {ID: 3}}, isMultiPoint: true, isSet: true},
		},
		{
			name: "NewRangeSelector",
			in:   Contract{ID: 1},
			want: Selector[Contract]{first: Contract{ID: 1}, last: Contract{ID: 10}, isRange: true, isSet: true},
		},
		{
			name: "SelectorNotSet",
			in:   Contract{ID: 1},
			want: Selector[Contract]{},
		},
	}

	for _, tc := range selectorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.name {
			case "NewPointSelector":
				got := NewPointSelector(tc.in)
				assert.Equal(t, got.first, tc.want.first)
				assert.Equal(t, got.last, tc.want.last)
				assert.Equal(t, got.isPoint, tc.want.isPoint)
				assert.Equal(t, got.isSet, tc.want.isSet)
			case "NewRangeSelector":
				got := NewRangeSelector(tc.in, Contract{ID: 10})
				assert.Equal(t, got.first, tc.want.first)
				assert.Equal(t, got.last, tc.want.last)
				assert.Equal(t, got.isRange, tc.want.isRange)
				assert.Equal(t, got.isSet, tc.want.isSet)
			case "SelectorNotSet":
				got := SelectorNotSet[Contract]()
				assert.Equal(t, got.isSet, tc.want.isSet)
			case "NewMultiPointsSelector":
				got := NewMultiPointsSelector(Contract{ID: 1}, Contract{ID: 2}, Contract{ID: 3})
				assert.Equal(t, got.points, tc.want.points)
				assert.Equal(t, got.isMultiPoint, tc.want.isMultiPoint)
				assert.Equal(t, got.isSet, tc.want.isSet)
			}
		})
	}
}
