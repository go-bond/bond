package bond

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumberSequence_Next(t *testing.T) {
	IDSequence := &NumberSequence{}

	id, err := IDSequence.Next()
	require.NoError(t, err)

	id2, err := IDSequence.Next()
	require.NoError(t, err)

	require.NotEqual(t, id, id2)
}

func TestNumberSequence_Timestamp(t *testing.T) {
	IDSequence := &NumberSequence{}

	expectedGreater := time.Now().Unix()

	id, err := IDSequence.Next()
	require.NoError(t, err)

	ts := IDSequence.Timestamp(id)

	expectedLess := time.Now().Unix()

	assert.GreaterOrEqual(t, ts, uint64(expectedGreater))
	assert.LessOrEqual(t, ts, uint64(expectedLess))
}

func TestNumberSequence_SequenceNumber(t *testing.T) {
	IDSequence := &NumberSequence{}

	id, err := IDSequence.Next()
	require.NoError(t, err)

	id2, err := IDSequence.Next()
	require.NoError(t, err)

	ts := IDSequence.SequenceNumber(id)

	ts2 := IDSequence.SequenceNumber(id2)

	assert.Greater(t, ts2, ts)
	assert.Equal(t, ts+1, ts2)
}

func TestNumberSequence_Interface(t *testing.T) {
	IDSequence := UniqueKeyGenerator[uint64](&NumberSequence{})
	require.NotNil(t, IDSequence)
}

func TestUUIDGenerator_Next(t *testing.T) {
	uuidGenerator := &UUIDGenerator{}

	id, err := uuidGenerator.Next()
	require.NoError(t, err)

	id2, err := uuidGenerator.Next()
	require.NoError(t, err)

	require.NotEqual(t, id, id2)

}

func TestUUIDGenerator_Interface(t *testing.T) {
	uuidGenerator := UniqueKeyGenerator[uuid.UUID](&UUIDGenerator{})
	require.NotNil(t, uuidGenerator)
}
