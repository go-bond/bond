package bond

import "context"

type IndexTypeBtree[T any] struct {
}

// Idea:
// Primary key's are part of index key in index_bond.go. So, each record produced n index key for n index.
// This increased the size significantly.
// To tackle the problem, btree index are produced.
// Here, primary key will be part of value instead of key.
// eg:
// index_key -> [primary1, primary2]
// we'll be using merge operator to append primary key to index key. so that we can
// avoid get during inserting or updating. This leads to following problem.
// 1) primary key per index will grow as the number of record being inserted.
// 2) how do we delete primary key from the list.
// solutions:
// 1) we'll have a background rotuine which actively split the list across multiple index
//   key so all the primary key don't get accumlated in the same index key.
// 2) we'll a reserved key for each index key called delete index key, which track all the
//   deleted primary key. That will be used to skip the primary key while retriving the
//  records using primary key. later background routine will remove the deleted primary key
// from the original list while spliting the list.

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
