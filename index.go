package bond

type IndexID uint8
type IndexKeyFunction[T any] func(builder KeyBuilder, t T) []byte
type IndexFilterFunction[T any] func(t T) bool

const MainIndexID = IndexID(0)

type Index[T any] struct {
	IndexID             IndexID
	IndexKeyFunction    IndexKeyFunction[T]
	IndexFilterFunction IndexFilterFunction[T]
}

func NewIndex[T any](idxID IndexID, idxFn IndexKeyFunction[T], idxFFn ...IndexFilterFunction[T]) *Index[T] {
	idx := &Index[T]{
		IndexID:          idxID,
		IndexKeyFunction: idxFn,
		IndexFilterFunction: func(t T) bool {
			return true
		},
	}

	if len(idxFFn) > 0 {
		idx.IndexFilterFunction = idxFFn[0]
	}

	return idx
}

func (i *Index[T]) IndexKey(builder KeyBuilder, t T) []byte {
	return i.IndexKeyFunction(builder, t)
}
