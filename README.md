bond
====

Bond is database built on [Pebble](https://github.com/cockroachdb/pebble) the key-value store that's based
on LevelDB/RocksDB and is used in CockroachDB as a storage engine. It leverages Pebbles SSTables in order
to provide efficient query execution times in comparison to other solutions on the market.

Bond features:
- Tables
- Indexes
- Ordered Indexes
- Partial Indexes
- Queries
- Custom Serialization / Deserialization

### Basics:

The example data structure:
```go
type ExampleStruct struct {
    Id          uint64 `json:"id"`
    Type        string `json:"type"`
    IsActive    bool   `json:"isActive"`
    Description string `json:"description"`
    Amount      uint64 `json:"amount"`
}
```

The Bond database open / close:
```go
db, err := bond.Open("example", &bond.Options{})
if err != nil {
    panic(err)
}

defer func() { _ = db.Close() }()
```

Table create:
```go
const (
    ExampleStructTableId bond.TableID = 1
)

ExampleStructTable := bond.NewTable[*ExampleStruct](bond.TableOptions[*ExampleStruct]{
    // The database instance
    DB:        db,
    // The unique table identifier
    TableID:   ExampleStructTableID,
    // The table name for inspect purposes
    TableName: "example_stuct_table",
    TablePrimaryKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddUint64Field(es.Id).Bytes()
    },
})
```

The index creation:
```go
ExampleStructTypeIndex := bond.NewIndex[*ExampleStruct](bond.IndexOptions[*ExampleStruct]{
    // The unique index identifier
    IndexID:   bond.PrimaryIndexID + 1,
    // The index name for inspect purposes
    IndexName: "type_idx",
    // The function that determines index key
    IndexKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddBytesField([]byte(es.Type)).Bytes()
    },
    IndexOrderFunc: bond.IndexOrderDefault[*ExampleStruct],
})
```

Insert:
```go
exapleStructs := []*ExampleStruct{
    {
        Id:          1,
        Type:        "test",
        IsActive:    true,
        Description: "test description",
        Amount:      1,
    },
}

err := ExampleStructTable.Insert(context.Background(), exapleStructs)
if err != nil {
    panic(err)
}
```

Update:
```go
exapleStructs := []*ExampleStruct{
    {
        Id:          1,
        Type:        "test",
        IsActive:    true,
        Description: "test description",
        Amount:      1,
    },
}

err := ExampleStructTable.Update(context.Background(), exapleStructs)
if err != nil {
    panic(err)
}
```

Delete:
```go
err := ExampleStructTable.Delete(context.Background(), &ExampleStruct{Id: 1})
if err != nil {
    panic(err)
}
```

Query:
```go
var exampleStructsFromQuery []*ExampleStruct
err := ExampleStructTable.Query().Execute(context.Background(), bond.NewSelectorPoint(&exampleStructsFromQuery))
if err != nil {
    panic(err)
}
```

Query using index:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Filter(cond.Func(func(es *ExampleStruct) bool {
        return es.Amount > 5
    })).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter and order:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Filter(cond.Func(func(es *ExampleStruct) bool {
        return es.Amount > 5
    })).
    Order(func(es *ExampleStruct, es2 *ExampleStruct) bool {
        return es.Amount < es2.Amount
    }).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with offset and limit:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Offset(1).
    Limit(2).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with cursor:
```go
var exampleStructsFromQuery []*ExampleStruct

// page 1, page size 10
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Limit(10).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}

// page 2, page size 10
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(exampleStructsFromQuery[9])).
    Limit(10).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Please see working example: [here](https://github.com/go-bond/bond/blob/master/_examples/simple/main.go) 

### Architecture:

Layers, top to bottom: user API (`Table[T]` / `Index[T]` / `Query[T]`) -> the typed table/index/query layer -> `Batch` (atomic unit of work) -> Pebble (`*pebble.DB`).

Key layout — `KeyEncode` (keys.go:215-240) writes `[TableID 1B][IndexID 1B][len(Index) uint32 BE][Index][len(IndexOrder) uint32 BE][IndexOrder][PrimaryKey]`. Both length words are explicit big-endian `uint32`s; `PrimaryKey` has no length prefix, since it always trails the rest of the key. A key *prefix* (used to range-scan one index) omits the `IndexOrder` length word and `PrimaryKey` entirely (keys.go:222-224, 233-237).

Core types:
- `DB` (bond.go:50) — root handle; Getter/Setter/Deleter/Batcher/Applier over `*pebble.DB`.
- `Table[T]` (table.go:109) — typed record store, `TableReader[T]` + `TableWriter[T]`; options at table.go:114.
- `Index[T]` (index.go:151, `NewIndex` at :160) — secondary index; key/filter/order functions at index.go:14,16,17.
- `Query[T]` (query.go:27) — fluent builder: index selector, filter, order, offset/limit, after-cursor.
- `Batch` (batch.go:28) — atomic unit of work over Pebble; `Type()`, `Count()`, Get/Set/Delete.
- `KeyBuilder` (keys.go:15) — fluent order-preserving field encoder (`AddUint64Field`, ...).
- `Key` / `KeyEncode` (keys.go:165-171 / 215-240) — key struct and the wire layout above.
- `Filter` (filter.go:27) — `Add`/`MayContain` membership test with `Load`/`Save`/`Clear` persistence.
- `BloomFilter` (bloom/bloom_filter.go:33) — bloom implementation of `Filter`.
- `Serializer[T]` (serializer.go:5) — `Serialize`/`Deserialize`; implementations in `serializers/` (JSON, CBOR, Protobuf).
- `cond` (cond/cond.go:9,25-156) — record predicates: `Func`, `Eq`, `Gt`, `Gte`, `Lt`, `Lte`, `And`, `Or`, `Not`.
- `inspect` (inspect/inspect.go:13,25) — runtime introspection: `Inspect`, `NewInspect`, HTTP handler + CLI.

#### Backup

- [Backup System Brief](https://github.com/go-bond/bond/blob/master/docs/01-backup-brief.md) — overview of the backup problem and design goals.
- [Backup to Object Storage via Pebble Checkpoint](https://github.com/go-bond/bond/blob/master/docs/02-backup-plan.md) — checkpoint-based backup design.
- [Enhancement Proposal: Optimized Backup Restore](https://github.com/go-bond/bond/blob/master/docs/03-backup-restore-optimization.md) — incremental restore that skips redundant downloads.
- [Backup Chain Integrity: Incremental Backup Baseline Mismatch](https://github.com/go-bond/bond/blob/master/docs/04-backup-chain-integrity.md) — per-backup UUID chaining to detect a broken backup chain.

Public entry points, package `backup`:
- `backup.Backup(ctx, db bond.DB, bucket objstore.Bucket, opts BackupOptions) (*BackupMeta, error)`
- `backup.Restore(ctx, bucket objstore.Bucket, opts RestoreOptions) error`
- `backup.ListBackups(ctx, bucket objstore.Bucket, prefix string) ([]BackupInfo, error)`
- `backup.FindRestoreSet(ctx, bucket objstore.Bucket, prefix string, before time.Time) ([]BackupInfo, error)`

`bond.DB` also embeds a root-level `Backup` interface (bond.go:67); that is a lower-level, DB-scoped hook, distinct from and not implemented in terms of the package-`backup` API above.

### Benchmarks:

Single historical run, not re-run for this document (see Provenance below). Machine: Intel(R) Core(TM)
i7-1068NG7 CPU @ 2.30GHz, `goos: darwin`, `goarch: amd64`, pkg `github.com/go-bond/bond/_benchmarks`.

**Writes** — batch size is the number of records committed together; `ns/op`/`op/s` are per batch, not per
record.

| Operation | Batch size | ns/op | op/s | B/op |
|---|---|---|---|---|
| Insert | 1 | 22040371 | 45.37 | 46106 |
| Insert | 1000 | 33895317 | 29.50 | 1328954 |
| Insert | 1000000 | 4105578035 | 0.2436 | 1316474888 |
| Update | 1 | 24977718 | 40.04 | 89446 |
| Update | 1000 | 32783521 | 30.50 | 1446429 |
| Update | 1000000 | 5807789010 | 0.1722 | 1374315704 |
| Upsert | 1 | 21369676 | 46.80 | 97086 |
| Upsert | 1000 | 34393250 | 29.08 | 23415072 |
| Upsert | 1000000 | 19010319722 | 0.05260 | 23349540512 |
| Delete | 1 | 21435621 | 46.65 | 46761 |
| Delete | 1000 | 25474395 | 39.26 | 437938 |
| Delete | 1000000 | 1779859111 | 0.5618 | 443109856 |

**Point reads**

| Operation | ns/op | op/s |
|---|---|---|
| Get row exists | 4608 | 217061 |
| Get row missing | 5513 | 181389 |
| Exist true | 3507 | 285225 |
| Exist false | 3341 | 299312 |

**Query & scan** — `Query_Index_Default` cases from `BenchmarkTableQuerySuite` (not the `WithTableSerializer`
variant, which additionally round-trips results through the table's serializer).

| Case | ns/op | op/s | B/op |
|---|---|---|---|
| Query, index default, Limit 500 | 700731 | 1427 | 132644 |
| Query, index default, Limit 0 (unbounded) | 30996623071 | 0.03226 | 4271256224 |
| Scan, skip 0, read 0 | 13555 | 73779 | 43849 |
| Scan, skip 1,000,000, read 0 | 91755776 | 10.90 | 344361 |
| Scan, skip 1,000,000, read 1000 | 261022972 | 3.831 | 235722 |

What drives these numbers:
- Every write batch, regardless of size, has a ~21-25 ms floor (Delete_1 21435621 ns, Insert_1 22040371 ns,
  Upsert_1 21369676 ns), because Pebble performs a synchronous commit (fsync) per batch. That cost is
  per-commit, not per-record, so a 1,000,000-row insert (4105578035 ns) costs only ~186x a 1-row insert despite
  carrying 1,000,000x the rows — batch your writes.
- Point reads and existence checks land at 3.3-5.5 us / 181k-299k op/s: a single Pebble point lookup plus one
  deserialize for `Get` (`Exist` skips the deserialize, hence the slightly lower latency).
- `Limit(0)` disables pagination and materializes the entire result set before returning: 30996623071 ns
  (~31 s), 4271256224 B (~4.3 GB), 100038541 allocs, versus 700731 ns for `Limit(500)` on the same query —
  always bound your queries.
- Scan cost is linear in rows skipped, not constant: 13555 -> 109424 -> 896962 -> 9501140 -> 91755776 ns for
  skip 0 / 1k / 10k / 100k / 1M, because a skip walks and discards each preceding key rather than seeking past
  it in one step — page with `After`/cursors instead of large offsets.
- Upsert is read-modify-write plus index maintenance, so it allocates far more than a blind `Insert` at the
  same size: 23349540512 B / 19010319722 ns at 1M rows versus Insert's 1316474888 B / 4105578035 ns.

**Provenance.** This run is labeled `MsgpackSerializer`, but the benchmark suites (`_benchmarks/suites/*.go`)
today register only `CBORSerializer` (the `JSONSerializer` entry is commented out), and the shipped
`serializers/` package implements CBOR, JSON, and Protobuf — there is no Msgpack serializer in this repo.
Treat these numbers as historical and indicative only, not a current measurement or a guarantee for your
workload or hardware.

**How to re-run** (see `Makefile:46-50`): `make bench` runs `cd _benchmarks && go test -timeout=25m -bench=.`;
`make bench-csv` runs `cd _benchmarks && go run ./benchmark.go --report=csv` and writes a CSV report.
