package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStructure struct {
	ID          int
	Name        string
	Description string
}

func TestIndex_indexKey(t *testing.T) {
	NameIndexID := IndexID(1)
	NameIndex := NewIndex[*TestStructure](
		NameIndexID,
		func(builder KeyBuilder, ts *TestStructure) []byte {
			return builder.AddStringField(ts.Name).Bytes()
		},
	)

	testStructure := &TestStructure{1, "test", "test desc"}

	assert.Equal(t, NameIndexID, NameIndex.IndexID)
	assert.Equal(t, []byte{0x01, 't', 'e', 's', 't'}, NameIndex.indexKey(NewKeyBuilder(nil), testStructure))
}
