package bond

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-bond/bond/cond"
)

// BoundTable is a typed handle over the existing logical Bond table encoding.
// It does not own or open a database.
type BoundTable[T any] struct {
	runtime    *_table[T]
	definition *TableDefinition[T]
	indexes    map[*IndexDefinition[T]]*BoundIndex[T]
}

func (t *BoundTable[T]) Definition() *TableDefinition[T] {
	if t == nil {
		return nil
	}
	return t.definition
}

func (t *BoundTable[T]) ID() TableID  { return t.runtime.ID() }
func (t *BoundTable[T]) Name() string { return t.runtime.Name() }

// Database returns the one already-opened database shared by all handles. It
// does not expose the private runtime Table whose index set is catalog-owned.
func (t *BoundTable[T]) Database() DB { return t.runtime.DB() }

func (t *BoundTable[T]) Get(ctx context.Context, selector Selector[T], batch ...Batch) ([]T, error) {
	return t.runtime.Get(ctx, selector, batch...)
}

func (t *BoundTable[T]) GetPoint(ctx context.Context, selector T, batch ...Batch) (T, error) {
	return t.runtime.GetPoint(ctx, selector, batch...)
}

func (t *BoundTable[T]) Exist(record T, batch ...Batch) bool {
	return t.runtime.Exist(record, batch...)
}

func (t *BoundTable[T]) Insert(ctx context.Context, records []T, batch ...Batch) error {
	return t.runtime.Insert(ctx, records, batch...)
}

func (t *BoundTable[T]) Update(ctx context.Context, records []T, batch ...Batch) error {
	return t.runtime.Update(ctx, records, batch...)
}

func (t *BoundTable[T]) Upsert(
	ctx context.Context,
	records []T,
	onConflict func(old, new T) T,
	batch ...Batch,
) error {
	return t.runtime.Upsert(ctx, records, onConflict, batch...)
}

func (t *BoundTable[T]) Delete(ctx context.Context, records []T, batch ...Batch) error {
	return t.runtime.Delete(ctx, records, batch...)
}

func (t *BoundTable[T]) Iter(selector Selector[T], batch ...Batch) Iterator {
	return t.runtime.Iter(selector, batch...)
}

func (t *BoundTable[T]) Scan(ctx context.Context, records *[]T, reverse bool, batch ...Batch) error {
	return t.runtime.Scan(ctx, records, reverse, batch...)
}

func (t *BoundTable[T]) ScanForEach(
	ctx context.Context,
	callback func(keyBytes KeyBytes, value Lazy[T]) (bool, error),
	reverse bool,
	batch ...Batch,
) error {
	return t.runtime.ScanForEach(ctx, callback, reverse, batch...)
}

func (t *BoundTable[T]) ScanIndex(
	ctx context.Context,
	index *BoundIndex[T],
	selector Selector[T],
	records *[]T,
	reverse bool,
	batch ...Batch,
) error {
	if index == nil || index.table != t {
		return errors.New("bond: bound index does not belong to table handle")
	}
	return t.runtime.ScanIndex(ctx, index.runtime, selector, records, reverse, batch...)
}

func (t *BoundTable[T]) ScanIndexForEach(
	ctx context.Context,
	index *BoundIndex[T],
	selector Selector[T],
	callback func(keyBytes KeyBytes, value Lazy[T]) (bool, error),
	reverse bool,
	batch ...Batch,
) error {
	if index == nil || index.table != t {
		return errors.New("bond: bound index does not belong to table handle")
	}
	return t.runtime.ScanIndexForEach(ctx, index.runtime, selector, callback, reverse, batch...)
}

func (t *BoundTable[T]) Query() BoundQuery[T] {
	return BoundQuery[T]{query: t.runtime.Query(), owner: t}
}

// InsertOne is a convenience wrapper that retains the existing caller-owned
// Batch contract. The same batch may be supplied to handles for other tables.
func (t *BoundTable[T]) InsertOne(ctx context.Context, record T, batch ...Batch) error {
	return t.Insert(ctx, []T{record}, batch...)
}

// BoundIndex couples a catalog index definition to the one bound table whose
// logical keyspace it belongs to.
type BoundIndex[T any] struct {
	definition *IndexDefinition[T]
	table      *BoundTable[T]
	runtime    *Index[T]
}

func (i *BoundIndex[T]) ID() IndexID {
	if i == nil || i.definition == nil {
		return PrimaryIndexID
	}
	return i.definition.ID()
}

func (i *BoundIndex[T]) Name() string {
	if i == nil || i.definition == nil {
		return ""
	}
	return i.definition.Name()
}

func (i *BoundIndex[T]) Definition() *IndexDefinition[T] {
	if i == nil {
		return nil
	}
	return i.definition
}

func (i *BoundIndex[T]) Table() *BoundTable[T] {
	if i == nil {
		return nil
	}
	return i.table
}

// Query starts a logical query through this index. Physical schema families
// remain absent from this API and from every mutation API.
func (i *BoundIndex[T]) Query(selector Selector[T]) BoundQuery[T] {
	return BoundQuery[T]{query: i.table.runtime.Query().With(i.runtime, selector), owner: i.table}
}

// BoundQuery forwards the safe logical query builder without exposing Query's
// underlying runtime Table through Query.Table().
type BoundQuery[T any] struct {
	query Query[T]
	owner *BoundTable[T]
	err   error
}

func (q BoundQuery[T]) Filter(condition cond.Cond[T]) BoundQuery[T] {
	q.query = q.query.Filter(condition)
	return q
}

func (q BoundQuery[T]) Order(less OrderLessFunc[T]) BoundQuery[T] {
	q.query = q.query.Order(less)
	return q
}

func (q BoundQuery[T]) Reverse() BoundQuery[T] {
	q.query = q.query.Reverse()
	return q
}

func (q BoundQuery[T]) Offset(offset uint64) BoundQuery[T] {
	q.query = q.query.Offset(offset)
	return q
}

func (q BoundQuery[T]) Limit(limit uint64) BoundQuery[T] {
	q.query = q.query.Limit(limit)
	return q
}

func (q BoundQuery[T]) After(selector T) BoundQuery[T] {
	q.query = q.query.After(selector)
	return q
}

func (q BoundQuery[T]) Intersects(queries ...BoundQuery[T]) BoundQuery[T] {
	if q.err != nil {
		return q
	}
	runtimeQueries := make([]Query[T], len(queries))
	for index, query := range queries {
		if query.err != nil {
			q.err = query.err
			return q
		}
		if q.owner == nil || query.owner != q.owner {
			q.err = fmt.Errorf(
				"bond: cannot intersect queries from different bound table handles (base table %q ID %d)",
				q.ownerName(),
				q.ownerID(),
			)
			return q
		}
		runtimeQueries[index] = query.query
	}
	q.query = q.query.Intersects(runtimeQueries...)
	return q
}

func (q BoundQuery[T]) Execute(ctx context.Context, result *[]T, batch ...Batch) error {
	if q.err != nil {
		return q.err
	}
	return q.query.Execute(ctx, result, batch...)
}

func (q BoundQuery[T]) ownerName() string {
	if q.owner == nil {
		return ""
	}
	return q.owner.Name()
}

func (q BoundQuery[T]) ownerID() TableID {
	if q.owner == nil {
		return BOND_DB_DATA_TABLE_ID
	}
	return q.owner.ID()
}

// BindTable compiles a validated definition into a typed handle over the
// already-opened Bond database. It performs no I/O and never calls pebble.Open.
func BindTable[T any](db DB, definition *TableDefinition[T]) (*BoundTable[T], error) {
	if db == nil {
		return nil, errors.New("bond: bind table to nil database")
	}
	if definition == nil || definition.catalog == nil {
		return nil, errors.New("bond: bind nil table definition")
	}
	authorization := db.catalogAuthorization()
	openedCatalog := authorization.catalog
	if openedCatalog == nil {
		return nil, errors.New("bond: database was opened without a catalog; use NewTable for the legacy dynamic API")
	}
	if openedCatalog != definition.catalog {
		return nil, fmt.Errorf(
			"bond: table definition %q belongs to catalog %q/%q, not the catalog instance supplied to Open",
			definition.schema.Name,
			definition.catalog.Name(),
			definition.catalog.Version(),
		)
	}
	if err := definition.catalog.Validate(); err != nil {
		return nil, err
	}

	runtimeTable := newCatalogTable(TableOptions[T]{
		DB:                  db,
		TableID:             definition.schema.TableID,
		TableName:           definition.schema.Name,
		TablePrimaryKeyFunc: definition.schema.PrimaryKeyFunc,
		Serializer:          definition.schema.Serializer,
		Filter:              definition.schema.Filter,
		ScanPrefetchSize:    definition.schema.ScanPrefetchSize,
	}, definition)
	bound := &BoundTable[T]{
		runtime:    runtimeTable,
		definition: definition,
		indexes:    make(map[*IndexDefinition[T]]*BoundIndex[T], len(definition.indexes)),
	}
	runtimeIndexes := make([]*Index[T], 0, len(definition.indexes))
	for _, indexDefinition := range definition.indexes {
		runtimeIndex := NewIndex(IndexOptions[T]{
			IndexID:           indexDefinition.schema.IndexID,
			IndexName:         indexDefinition.schema.Name,
			IndexKeyFunc:      indexDefinition.schema.IndexKeyFunc,
			IndexMultiKeyFunc: indexDefinition.schema.IndexMultiKeyFunc,
			IndexOrderFunc:    indexDefinition.schema.IndexOrderFunc,
			IndexFilterFunc:   indexDefinition.schema.IndexFilterFunc,
		})
		runtimeIndexes = append(runtimeIndexes, runtimeIndex)
		bound.indexes[indexDefinition] = &BoundIndex[T]{
			definition: indexDefinition,
			table:      bound,
			runtime:    runtimeIndex,
		}
	}
	if err := runtimeTable.AddIndex(runtimeIndexes); err != nil {
		return nil, fmt.Errorf("bond: bind table %q indexes: %w", definition.schema.Name, err)
	}
	return bound, nil
}

// BindIndex returns the index handle compiled when its table was bound.
func BindIndex[T any](table *BoundTable[T], definition *IndexDefinition[T]) (*BoundIndex[T], error) {
	if table == nil || table.definition == nil {
		return nil, errors.New("bond: bind index to nil table handle")
	}
	if definition == nil {
		return nil, errors.New("bond: bind nil index definition")
	}
	if definition.table != table.definition {
		return nil, fmt.Errorf(
			"bond: index definition %q does not belong to bound table %q",
			definition.schema.Name,
			table.definition.schema.Name,
		)
	}
	index, ok := table.indexes[definition]
	if !ok {
		return nil, fmt.Errorf("bond: index definition %q was not compiled with table %q", definition.schema.Name, table.definition.schema.Name)
	}
	return index, nil
}
