package bond

import (
	"testing"

	"github.com/google/uuid"
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
