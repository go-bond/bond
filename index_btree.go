package bond

import "context"

type IndexTypeBtree[T any] struct {
}

func (ie *IndexTypeBtree[T]) OnInsert(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	panic("not implemented!")
}

func (ie *IndexTypeBtree[T]) OnUpdate(table Table[T], idx *Index[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error {
	panic("not implemented!")

}

func (ie *IndexTypeBtree[T]) OnDelete(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	panic("not implemented!")
}

func (ie *IndexTypeBtree[T]) Iter(table Table[T], idx *Index[T], selector Selector[T], optBatch ...Batch) Iterator {
	panic("not implemented!")
}

func (ie *IndexTypeBtree[T]) Intersect(ctx context.Context, table Table[T], idx *Index[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
	panic("not implemented!")
}

var _ IndexType[any] = (*IndexTypeBtree[any])(nil)
