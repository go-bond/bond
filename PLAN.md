# Bond compact-key and schema-aware SST plan

Status: **future design and implementation plan; no production change is authorized by this document**

Planned on: **2026-08-05**

Planning baselines:

- Bond: `fea660a96e4685be235b88c694f2c630d09235d0` (`master` at planning time)
- Pebble: `/home/peter/Dev/other/pebble` at `8fb150d9135d6f94e183a874475e0bd1afb18f63` (`master` at planning time)
- Pebble issue: [cockroachdb/pebble#4380](https://github.com/cockroachdb/pebble/issues/4380), verified open on 2026-08-05
- Current Bond dependency: `github.com/cockroachdb/pebble v0.0.0-20251103183323-1df7d4aae653`

This plan is intentionally elaborate. It records the architectural conclusions, the immediately useful Pebble-master capabilities, the proposed Bond API, migration rules, benchmark design, rejected alternatives, and a phased path that can stop safely after any experiment.

## 1. Executive decision

The recommended direction is:

1. Keep **one Bond database backed by exactly one Pebble database**.
2. Upgrade Bond to the reviewed Pebble `master` baseline without immediately changing Bond's logical key format or Pebble format-major version.
3. Correct Bond's global `PreferFastCompression` policy and benchmark Pebble master's compression and table-filter choices before writing a custom key schema.
4. Implement a conservative global physical schema, tentatively named `bond/full-key/v1`, which preserves every logical Bond key byte but encodes the complete user key through a `PrefixBytes` column. This is the first KeySchema experiment because it needs no catalog, no per-write schema hint, no logical migration, and no Pebble fork.
5. Add a declarative Bond catalog as the durable source of table/index identities and key-layout metadata. The catalog is supplied before `bond.Open`, but callers still write through bound table/index handles—not through `Catalog.Set` and not through a schema-aware `Set`.
6. If the global schema proves useful, add one small Pebble extension: allow `SpanPolicy` to choose the KeySchema used for each output key range. Pebble master already asks `SpanPolicyFunc` for key-range policy and splits output SSTs at policy boundaries, so schema choice now has a natural home that did not exist when issue #4380 was opened.
7. Initially route custom schemas **per table range**, not per index. Reusable schema families should target the primary-key tail shared by the primary row and secondary index entries. Only add per-index policies if measurements justify the additional SST boundaries and operational complexity.
8. Treat more invasive logical encodings—tuple key v2, surrogate row IDs, unique-index value layouts, and posting-list indexes—as separate opt-in projects. They can produce larger savings, but they change WAL/memtable keys or query semantics and must not be conflated with the first physical KeySchema rollout.

The answer to the original `.Set()` question is therefore:

> There should be no schema argument on `Set`, and the catalog should not be a second database or a write destination. A bound table uses catalog metadata to build an ordinary logical Bond key, then calls ordinary `pebble.Batch.Set`. Later, when Pebble flushes or compacts a key range, the active range policy selects the physical KeySchema for the resulting SST.

## 2. Why this is now practical

Issue #4380 asked whether user-level KeySchemas could help Bond and whether each write could provide a schema hint. The useful part of that idea is now practical, but the right implementation point is different from the issue's proposed per-`Set` hint.

Pebble master now has the following relevant behavior:

- `Options.KeySchema` names the schema used to write new columnar SSTs.
- `Options.KeySchemas` is a registry of every schema that may be needed to read existing SSTs.
- Each columnar SST stores its KeySchema name in table properties.
- Opening an SST resolves that stored name through `Options.KeySchemas`.
- Multiple SSTs in one Pebble database may therefore use different KeySchemas over the lifetime of the database.
- `SpanPolicyFunc` now receives key bounds, returns policy for a range, and causes flush/compaction output to end at `SpanPolicy.KeyRange.End`.

The last capability is the important evolution. It was added after the issue was filed and gives Pebble a range-aware, SST-output-time routing point. The remaining per-range schema feature is small: carry a schema name in `SpanPolicy` and apply it to the writer options for that output SST.

What is **not** available on stock Pebble master is direct per-range schema selection. Stock master still has one active `Options.KeySchema` for all new outputs. The plan distinguishes carefully between:

- what Bond can test immediately using unmodified Pebble master; and
- what requires a focused Pebble patch or an upstream feature.

## 3. Scope

### 3.1 In scope

- Bond on the reviewed local Pebble `master` baseline.
- Storage size and read/write behavior of Bond primary and secondary keys.
- Columnar SST KeySchemas for Bond's user keys.
- A future Bond catalog describing tables, indexes, stable IDs, logical key layouts, codecs, and physical schema assignments.
- A small range-to-KeySchema extension in Pebble if global schema results warrant it.
- Upgrade, mixed-schema compatibility, rollback, backup/restore, inspection tooling, and benchmarks.
- Optional later logical index designs, clearly isolated from the physical-schema work.

Likely Bond implementation areas include:

- `go.mod` and `go.sum`
- `options.go`
- `bond.go`
- `keys.go`
- `table.go`
- `index.go`
- `version.go`
- backup/restore metadata and tests
- a new catalog/schema package or files
- `_benchmarks`
- focused test helpers and fixtures

Likely Pebble implementation areas, only if the range-schema phase is approved, include:

- `/home/peter/Dev/other/pebble/options.go`
- `/home/peter/Dev/other/pebble/internal/base/span_policy.go`
- `/home/peter/Dev/other/pebble/compaction.go`
- Pebble option, compaction, flush, and metamorphic tests

### 3.2 Out of scope for the initial work

- One Pebble database per Bond table.
- A schema argument on each `Set`, `Delete`, or batch mutation.
- A Pebble format-major-version ratchet to `FormatNewest` merely because dependencies are upgraded.
- An immediate rewrite of Bond's logical keys.
- Hash-only primary or secondary keys.
- Moving primary keys out of generic non-unique secondary keys without another uniqueness/disambiguation mechanism.
- Removing any KeySchema that has ever been used by an SST that may still exist.
- Activating a new writer before all readers, backup tools, migrations, and inspectors can load its schema.
- Unrelated changes to backup architecture or Bond's query language.

### 3.3 Nature of this review

This is a focused storage-layout and Pebble-capability review, not a complete security or correctness audit of Bond. One existing logical-key collision hazard was discovered because it directly affects schema decomposition; it is recorded below and should receive its own fix plan.

## 4. Baseline and drift checks

Before executing any phase, confirm that the assumptions in this plan still match both repositories.

```bash
cd /home/peter/Dev/0xsequence/bond
git status --short
git rev-parse HEAD
git diff --stat fea660a96e4685be235b88c694f2c630d09235d0..HEAD -- \
  go.mod go.sum options.go bond.go keys.go table.go index.go version.go backup _benchmarks

cd /home/peter/Dev/other/pebble
git status --short
git rev-parse HEAD
git diff --stat 8fb150d9135d6f94e183a874475e0bd1afb18f63..HEAD -- \
  options.go compaction.go internal/base/span_policy.go sstable
```

STOP and re-audit the affected section if any of the following has changed materially:

- Pebble added an upstream per-range KeySchema selector.
- `SpanPolicyFunc` no longer determines output boundaries.
- SSTs no longer persist and resolve KeySchema names in the same way.
- Bond's comparer, `Split`, or key layout changed.
- Bond already introduced a catalog or a logical key-format version.
- `PebbleDBFormat` changed from `pebble.FormatV2BlobFiles`.
- Backup metadata now records required schema readers.

The line numbers in this document are evidence anchors from the planning revisions, not permanent API references.

## 5. Current Bond storage model

### 5.1 One logical database and one physical LSM

Bond should continue to look like a lower-level relational/table layer built on a single Pebble KV store:

```text
Bond DB
├── catalog: logical table and index definitions
├── table 1: accounts
│   ├── primary records
│   ├── by-email index
│   └── by-status/balance index
├── table 2: sessions
│   ├── primary records
│   └── by-account/expiry index
└── table 3: events
    ├── primary records
    └── by-kind/time index

                 one pebble.Open(...)
                         │
             one WAL / memtable system
                         │
                 one shared version
                         │
             one LSM and block cache
                         │
       many SSTs, each naming one KeySchema
```

This preserves:

- atomic batches across tables and indexes;
- one WAL and recovery stream;
- shared cache and file lifecycle;
- one manifest/version set;
- one compaction scheduler;
- simple backup/checkpoint semantics; and
- the current Bond operational model.

Creating a separate Pebble DB per table would multiply WALs, manifests, caches, compaction work, open files, and failure/recovery surfaces. It would also remove straightforward cross-table atomicity. It is not recommended.

### 5.2 Current logical key layout

At the planning revision, Bond constructs keys with this broad layout:

```text
TableID       1 byte
IndexID       1 byte
IndexLen      4 bytes
Index         variable bytes
OrderLen      4 bytes
Order         variable bytes
PrimaryKey    Bond key bytes, including its outer envelope
```

The main construction is in `keys.go:215-239`. A complete non-prefix key has ten bytes of fixed top-level framing—two one-byte IDs and two four-byte lengths—before counting index, order, primary-key content, and the per-field bytes emitted by `KeyBuilder`.

Secondary index values are intentionally empty. The complete primary key is appended to each secondary index key (`index.go:484-500`). This makes every non-unique index entry independently ordered and disambiguated, but it also means a long primary key is repeated in every index.

### 5.3 Current comparer and columnar split

Bond retains bytewise ordering and overrides only `Comparer.Split` (`keys.go:361-379`):

- secondary keys split after the logical index prefix;
- primary keys split at the full key;
- comparisons remain bytewise.

Pebble's default columnar KeySchema encodes the comparer-defined prefix in a `PrefixBytes` column and the remaining suffix in a `RawBytes` column. For a typical Bond secondary key, that leaves this important portion as raw suffix data:

```text
OrderLen | Order | PrimaryKey
```

That is exactly the region where Bond may have repeated/local structure that the default physical schema cannot exploit.

### 5.4 Existing research establishes an addressable corpus, not promised savings

The repository's collector/filter research records an example database with approximately:

- 5.9 GB total database size;
- 2.4 GB of index data;
- 542 MB attributed to the index-key portion; and
- 1.9 GB attributed to the repeated primary-key tail.

See `docs/_research/1_Pebble_Collectors_And_Filters/README.md:193-202`.

The 1.9 GB figure identifies a large target, but it is **not** the expected saving. Block compression already captures repetition; actual gains depend on key distribution, prefix locality, block size, schema encoding, and CPU cost. Every proposed saving in this plan must be measured on representative data.

## 6. The schema is not selected at `Set`

### 6.1 Correct write path

The intended flow is:

```text
application record
      │
      ▼
bound Bond table/index definition
      │  builds the stable logical key bytes
      ▼
pebble.Batch.Set(key, value, ...)
      │
      ├── WAL
      └── memtable
             │
             ▼
      flush or compaction
             │
             ▼
SpanPolicyFunc(key-range bounds)
             │
             ├── compression/value policy
             ├── output range end
             └── future KeySchema name
                         │
                         ▼
              one output SST writer
                         │
                         ▼
             SST records KeySchema name
```

The table definition influences the logical key at write time. The physical schema is selected later for the SST containing the key range.

### 6.2 Why a per-write schema hint is the wrong abstraction

A per-`Set` schema hint would need to survive:

- batch representation;
- WAL encoding and replay;
- memtable insertion;
- snapshots;
- flush picking;
- range deletions;
- ingests;
- compaction input merging; and
- repeated compactions over the database lifetime.

More importantly, an SST writer uses one KeySchema. Interleaved writes with different hints would eventually need to be re-partitioned by logical key range anyway. Range policy is therefore the stable and composable abstraction.

### 6.3 What the catalog does

The catalog should:

- assign and validate stable table and index IDs;
- define the logical key layout and codecs;
- define optional typed primary/order fields;
- define physical schema families and versions;
- compile table/index key ranges into range routes;
- assemble the complete KeySchema registry before opening Pebble;
- bind application types to table and index handles; and
- provide metadata for migrations, backups, and inspection.

The catalog should **not**:

- own a second Pebble database;
- expose `Set` as the normal record-write API;
- require callers to choose a physical schema per mutation; or
- allow schema names/semantics to mutate in place.

## 7. Pebble-master findings relevant to Bond

### 7.1 Multiple schemas are already readable in one store

In Pebble master:

- `Options.KeySchema` and `Options.KeySchemas` are documented in `options.go:934-949`.
- The SST writer records `KeySchema.Name` in `sstable/colblk_writer.go:185`.
- The SST reader resolves the stored name through the registry in `sstable/reader.go:1243-1256`.

Consequences:

1. A single database may contain old default-schema SSTs and new Bond-schema SSTs.
2. Compaction can gradually rewrite old data into the currently selected schema.
3. A rollback can select an older writer while retaining new schema readers.
4. Schema names become durable storage-format identifiers.
5. Every schema ever used by a live or restorable SST must remain registered.

No new Pebble on-disk format is required merely to have multiple schema names across SSTs.

### 7.2 Stock master has one active writer schema

Stock master uses `Options.KeySchema` when constructing new writer options. `Options.KeySchemas` is a registry, not a routing table. Merely registering three schemas does not cause writes for three Bond tables to land in differently encoded SSTs.

This supports a global Bond schema immediately. It does not yet support table-specific schemas for new flush/compaction outputs.

### 7.3 SpanPolicy provides the missing routing boundary

Pebble master defines `SpanPolicyFunc(bounds UserKeyBounds) (SpanPolicy, error)` in `options.go:1371-1388`. During compaction, Pebble:

- calls the policy function for current output bounds;
- applies policy such as fast compression; and
- ends the output at `SpanPolicy.KeyRange.End`.

Relevant code is in `compaction.go:2861-2903` and `internal/base/span_policy.go:18-66`.

This machinery came from the May 5, 2025 range-policy work (including commit `afff98cb...`). It supplies the key-range-to-SST partitioning that issue #4380 lacked.

### 7.4 Bond currently defeats its configured deep-level compression preference

Bond's current `spanPolicyFunc` in `options.go:342-346` sets:

```go
policy.PreferFastCompression = true
```

for the entire keyspace. Pebble master applies that flag in `compaction.go:2882-2888` by replacing any non-`NoCompression` writer choice with `FastestCompression`.

Bond's level options may say Zstd, but the global span policy can override that decision for every compaction output. This may be costing substantially more space than any KeySchema enhancement can recover.

This is the first and cheapest experiment:

- remove the global preference; or
- restrict it to a justified hot/transient key range; then
- compare Bond's balanced and good compression profiles on the same corpus.

This change must still be benchmarked. Better compression may increase compaction CPU or alter write latency.

### 7.5 New table-filter choices are worth isolated measurement

Pebble master includes newer progressive table-filter choices, including:

- progressive Bloom filters through `DBTableFilterPolicyProgressive`; and
- progressive binary-fuse filters through `DBTableFilterPolicyBinaryFuseProgressive`.

Binary-fuse filters may improve lower-level filter size and false-positive tradeoffs for some distributions. They are not automatically better for every Bond workload. Test them independently from key schema and compression changes so their effect remains attributable.

### 7.6 Bond is already on the columnar-capable format line

Bond sets:

```go
const PebbleDBFormat = pebble.FormatV2BlobFiles
```

in `options.go:18`. That format is newer than the columnar-table milestone. There is no reason to couple this work to `FormatNewest`.

Keep `FormatMajorVersion` pinned to `FormatV2BlobFiles` throughout the early upgrade and schema experiments unless a separate reviewed feature requires a ratchet. Format-major-version upgrades are one-way operational decisions; ordinary KeySchema registration is not.

### 7.7 Newer columnar fixes reduce implementation risk, not correctness obligations

The reviewed six-month Pebble history includes ongoing columnar correctness, prefix-iteration, filter, and compaction improvements. Upgrading is therefore useful. It does not make a custom KeyWriter/KeySeeker routine. Bond still needs adversarial tests for short seeks, reverse iteration, synthetic prefixes/suffixes, concurrency, and mixed schemas.

### 7.8 Relevant Pebble commit inventory

The local Pebble history was reviewed from 2026-02-05 through the planning revision. The most directly relevant commits in that window are:

| Commit | Date | Relevance to Bond |
|---|---|---|
| `0ce8e60f` | 2026-03-24 | Widens columnar decode offset/count types to prevent overflow. This is a useful correctness fix for large/unusual blocks. |
| `521683ee` | 2026-04-23 | Fixes `equalPrefix` behavior in columnar and Cockroach custom `KeySeeker.SeekGE`. This is directly relevant evidence that custom seeker prefix semantics are subtle. |
| `cde34a2b` | 2026-04-28 | Adds `SeekPrefixGE`/`NextWithSamePrefix` support to iterator block data. Relevant to Bond's prefix-oriented index scans. |
| `835bd183` | 2026-04-28 | Enforces strict prefix iteration in SST iterators. This strengthens the contract a Bond schema must obey. |
| `5bc27b59`, `8e4258cc`, `d45d7d3f` | 2026-04-28 | Continue prefix/`NextPrefix` work through iterator v2. Relevant to scan implementation and future performance, but not a substitute for Bond correctness tests. |
| `4f6c69c5`, `b011c8ae`, `2d0cef8a`, `4c708f6e` | 2026-05-06 | Extend strict-prefix enforcement through fake, batch, arena-skiplist, and interleaving iterators. These changes make an upgrade preferable before schema experimentation. |
| `7f431a57` | 2026-05-06 | Adds iterator-v2 fast paths for `SeekGE(TrySeekUsingNext)` and `NextPrefix`. Potential scan benefit; benchmark rather than assume. |
| `25db1a57` | 2026-06-28 | Accounts for blob-file compression in value-separation split-size estimates. Relevant because Bond remains on `FormatV2BlobFiles` and upgrade semantics include value storage. |

Several enabling changes slightly predate that strict six-month window but are part of the reviewed master capability and matter to the design:

| Commit | Date | Capability |
|---|---|---|
| `686c3c88` | 2025-12-15 | Adds progressive Bloom table-filter policy. |
| `0e733d48` | 2026-01-14 | Adds top-level binary-fuse filter support. |
| `6375bc02` | 2026-01-23 | Adds `TieringPolicy`, continuing range/span policy development. |
| `afff98cb` | 2025-05-05 | Introduces `SpanPolicy` and `SpanPolicyFunc`, the key range-to-output hook used by this proposal. |
| `c719c4e5` | 2024-10-15 | Adds DB support for multiple registered KeySchemas, allowing SSTs with different durable schema names to coexist. |

The inventory supports three conclusions:

1. Upgrade before implementing a custom schema so Bond receives the prefix-seek and columnar correctness work.
2. Prefix behavior remains an actively hardened area, so the KeySeeker test burden is real.
3. The multiple-schema and range-policy primitives arrived separately; combining them through a small policy field is a natural incremental extension, not a redesign of Pebble's write API.

## 8. Proposed architecture

The target design has four distinct layers. Keeping them distinct prevents physical-storage experiments from leaking into application APIs.

### 8.1 Layer A: immutable logical key format

This is the byte string seen by:

- the Pebble comparer;
- memtables;
- WAL records;
- range bounds;
- snapshots;
- batches;
- logical dumps; and
- all existing Bond query construction.

The first KeySchema phases do **not** change it.

### 8.2 Layer B: declarative Bond catalog

The catalog describes:

- database catalog name/version;
- table name and stable one-byte `TableID`;
- record codec;
- primary key builder/descriptor;
- index name and stable one-byte `IndexID`;
- uniqueness;
- index key and order descriptors;
- partial-index predicate identity/version;
- logical layout version;
- physical schema family/version; and
- any opt-in storage mode, such as row IDs or posting lists.

The catalog compiles immutable runtime handles and Pebble-open options.

### 8.3 Layer C: physical KeySchema registry

The registry maps durable names to implementations, for example:

```text
DefaultKeySchema(bond_comparer,16) -> legacy/default reader
bond/full-key/v1-b16              -> full logical key PrefixBytes, bundle 16
bond/pk-u64/v1                    -> table-range schema with uint PK tail
bond/pk-bytes/v1                  -> table-range schema with bytes PK tail
bond/pk-opaque/v1                 -> safe typed-routing fallback
```

Names and their meanings are immutable. If implementation behavior changes, create a new name.

### 8.4 Layer D: range policy

The range policy maps Bond's ordered table ranges to physical writer settings:

```text
[table 1 start, table 2 start) -> bond/pk-u64/v1
[table 2 start, table 3 start) -> bond/pk-bytes/v1
[table 3 start, table 4 start) -> bond/pk-u64/v1
```

Adjacent ranges with identical policy should be coalesced. Each policy end can force an SST boundary, so gratuitous per-index routes can increase file counts and read amplification.

## 9. Hypothetical Bond API

The following API is deliberately illustrative. It does not exist in current Bond and is not intended to freeze exact Go names. It demonstrates ownership and write flow.

### 9.1 Application types

```go
type Account struct {
    ID      uint64
    Email   string
    Status  string
    Balance uint64
}

type Session struct {
    Token     []byte
    AccountID uint64
    ExpiresAt time.Time
}

type Event struct {
    TenantID uint32
    Sequence uint64
    Kind     string
    Created  time.Time
    Payload  []byte
}
```

### 9.2 Define the catalog before opening the database

```go
catalog := bond.NewCatalog("example/v1")

accountsDef := bond.DefineTable[Account](catalog, bond.TableSchema[Account]{
    Name:    "accounts",
    TableID: 1,
    Codec:   bond.CBOR[Account](),
    PrimaryKey: bond.Uint64Key(
        func(a *Account) uint64 { return a.ID },
    ),
    PhysicalKeySchema: bond.KeySchemaPKUint64V1,
})

accountsByEmail := bond.DefineIndex(accountsDef, bond.IndexSchema[Account]{
    Name:    "by_email",
    IndexID: 1,
    Unique:  true,
    Key: bond.StringKey(
        func(a *Account) string { return a.Email },
    ),
})

accountsByStatusBalance := bond.DefineIndex(accountsDef, bond.IndexSchema[Account]{
    Name:    "by_status_balance",
    IndexID: 2,
    Key: bond.StringKey(
        func(a *Account) string { return a.Status },
    ),
    Order: bond.Uint64Order(
        func(a *Account) uint64 { return a.Balance },
    ),
})

sessionsDef := bond.DefineTable[Session](catalog, bond.TableSchema[Session]{
    Name:    "sessions",
    TableID: 2,
    Codec:   bond.CBOR[Session](),
    PrimaryKey: bond.BytesKey(
        func(s *Session) []byte { return s.Token },
    ),
    PhysicalKeySchema: bond.KeySchemaPKBytesV1,
})

sessionsByAccount := bond.DefineIndex(sessionsDef, bond.IndexSchema[Session]{
    Name:    "by_account_expiry",
    IndexID: 1,
    Key: bond.Uint64Key(
        func(s *Session) uint64 { return s.AccountID },
    ),
    Order: bond.TimeOrder(
        func(s *Session) time.Time { return s.ExpiresAt },
    ),
})

eventsDef := bond.DefineTable[Event](catalog, bond.TableSchema[Event]{
    Name:    "events",
    TableID: 3,
    Codec:   bond.CBOR[Event](),
    PrimaryKey: bond.TupleKey2(
        bond.Uint32Part(func(e *Event) uint32 { return e.TenantID }),
        bond.Uint64Part(func(e *Event) uint64 { return e.Sequence }),
    ),
    // Start with the safe opaque family until a composite typed family wins
    // a benchmark and its encoding is fully specified.
    PhysicalKeySchema: bond.KeySchemaPKOpaqueV1,
})

eventsByKind := bond.DefineIndex(eventsDef, bond.IndexSchema[Event]{
    Name:    "by_kind_created",
    IndexID: 1,
    Key: bond.StringKey(
        func(e *Event) string { return e.Kind },
    ),
    Order: bond.TimeOrder(
        func(e *Event) time.Time { return e.Created },
    ),
})

if err := catalog.Validate(); err != nil {
    return err
}
```

The definitions are declarative enough for Bond to understand key shape, but callbacks may still extract values from records. Stable IDs and codec/schema names are explicit and reviewable.

### 9.3 Open one Bond/Pebble database and bind handles

```go
db, err := bond.Open("example.db", &bond.Options{
    Catalog: catalog,
    Pebble: bond.PebbleOptions{
        Profile: bond.DBCompressionBalanced,
    },
})
if err != nil {
    return err
}
defer db.Close()

accounts := bond.BindTable(db, accountsDef)
sessions := bond.BindTable(db, sessionsDef)
events   := bond.BindTable(db, eventsDef)
```

There is one `bond.Open` and internally one `pebble.Open`. Binding validates that a definition belongs to the opened catalog; it does not open another store.

### 9.4 Write records atomically

```go
batch := db.NewWriteBatch()
defer batch.Close()

account := &Account{
    ID:      42,
    Email:   "peter@example.com",
    Status:  "active",
    Balance: 125_000,
}
session := &Session{
    Token:     []byte("session-7eb9"),
    AccountID: account.ID,
    ExpiresAt: time.Now().Add(24 * time.Hour),
}
event := &Event{
    TenantID: 7,
    Sequence: 991,
    Kind:     "account.created",
    Created:  time.Now(),
}

if err := accounts.Put(batch, account); err != nil {
    return err
}
if err := sessions.Put(batch, session); err != nil {
    return err
}
if err := events.Put(batch, event); err != nil {
    return err
}

if err := batch.Commit(pebble.Sync); err != nil {
    return err
}
```

Conceptually, `accounts.Put` expands to ordinary KV operations:

```go
func (t *Table[Account]) Put(batch *bond.WriteBatch, a *Account) error {
    primaryKey := t.logicalPrimaryKey(a)
    value, err := t.codec.Marshal(a)
    if err != nil {
        return err
    }

    // No KeySchema parameter here.
    if err := batch.Set(primaryKey, value, nil); err != nil {
        return err
    }

    emailKey := t.byEmail.logicalIndexKey(a, primaryKey)
    if err := batch.Set(emailKey, nil, nil); err != nil {
        return err
    }

    statusBalanceKey := t.byStatusBalance.logicalIndexKey(a, primaryKey)
    return batch.Set(statusBalanceKey, nil, nil)
}
```

The schema name is absent because the resulting WAL and memtable contain stable logical keys. If a later flush spans accounts, sessions, and events, the range policy may produce three SSTs, each naming its chosen schema, all inside the same Pebble database.

### 9.5 Query through the same handles

```go
account, err := accounts.Get(ctx, uint64(42))

account, err = accountsByEmail.GetUnique(ctx, "peter@example.com")

active, err := accountsByStatusBalance.Scan(ctx, bond.Scan{
    Key:     "active",
    Reverse: true,
    Limit:   100,
})

sessionsForAccount, err := sessionsByAccount.Scan(ctx, bond.Scan{
    Key:   uint64(42),
    Until: time.Now().Add(2 * time.Hour),
})

recentEvents, err := eventsByKind.Scan(ctx, bond.Scan{
    Key:     "account.created",
    Reverse: true,
    Limit:   50,
})
```

Query APIs continue to express logical table/index operations. The caller neither sees nor chooses SST schemas.

## 10. Catalog contract and invariants

### 10.1 Catalog availability before `Open`

The clean design supplies all definitions before Pebble opens because Pebble options must contain:

- the comparer;
- the complete KeySchema reader registry;
- the default writer schema; and
- the compiled range-policy function.

Current Bond permits table construction after opening the DB. That is convenient but mismatched with a schema-aware storage catalog. Preserve compatibility with legacy dynamic tables by assigning them the global/default schema; require pre-open registration only for typed physical routing.

### 10.2 Stable identity

Catalog validation must reject:

- duplicate table IDs or names;
- duplicate index IDs within a table;
- reserved IDs;
- schema names missing from the registry;
- a changed layout under an existing catalog/table/schema version;
- unsupported schema/key-type combinations; and
- overlapping or unsorted compiled ranges.

Table and index IDs are storage identities. Renaming a Go variable must not alter them.

### 10.3 Definition fingerprints

Consider computing a deterministic catalog fingerprint over storage-relevant fields:

- catalog version;
- table/index IDs;
- logical key-layout versions;
- codec names/versions;
- uniqueness and partial-index metadata;
- physical schema names; and
- storage mode.

Do not hash opaque Go callback code and pretend the result proves equivalence. Extraction callbacks should be paired with explicit durable descriptor/version strings chosen by the developer.

### 10.4 Catalog persistence

Two models are possible:

1. The application definition is authoritative, and Bond stores only a compatibility fingerprint/manifest in reserved metadata keys.
2. Bond stores a complete serializable catalog and validates the supplied application bindings against it.

For a low-level library, begin with model 1. It avoids trying to serialize Go functions while still detecting accidental storage-definition drift. A later schema-management layer may adopt model 2.

### 10.5 Legacy API coexistence

Existing callers should remain supported:

```text
legacy table created after Open
        -> existing logical layout
        -> global active KeySchema
        -> no typed range guarantee

catalog-bound table created before Open
        -> validated logical descriptor
        -> eligible for per-table physical routing
```

Do not silently infer a typed schema from an opaque callback. An explicit fallback is safer than a false type claim.

## 11. First schema experiment: `bond/full-key/v1`

### 11.1 Objective

Encode the complete logical Bond user key as one `DataTypePrefixBytes` column while preserving:

- byte-for-byte logical keys;
- bytewise ordering;
- Bond comparer behavior;
- Bond's current `Split` result;
- batch/WAL/memtable formats;
- existing range bounds and queries; and
- all values.

This extends columnar prefix encoding across the part that the default schema currently treats as raw suffix, including order bytes and repeated primary-key tails.

### 11.2 Why it is the right first custom schema

It requires:

- no table catalog;
- no knowledge of individual key fields;
- no Pebble patch;
- no per-range selection;
- no logical data rewrite;
- no query API change; and
- no format-major-version upgrade.

It can be activated globally through `Options.KeySchema` after its reader is registered.

### 11.3 Physical—not logical—encoding

The distinction is critical:

```text
logical key bytes before schema:  T | I | ILen | Index | OLen | Order | PK
logical key bytes after schema:   T | I | ILen | Index | OLen | Order | PK

default physical columns:         PrefixBytes(logical prefix) | RawBytes(suffix)
BondFullKeyV1 physical columns:    PrefixBytes(complete logical key)
```

Iterators reconstruct exactly the original user key. A caller cannot observe the physical column decomposition except through performance, storage metrics, or table inspection.

### 11.4 Required KeyWriter behavior

The writer must:

- compare complete keys according to Bond's bytewise comparer;
- return `KeyComparison.PrefixLen` equal to `Comparer.Split(key)`;
- return the correct physical common-prefix length for consecutive keys;
- write the complete key into the `PrefixBytes` column;
- reset cleanly between blocks;
- handle the first key and duplicate user keys correctly; and
- preserve all invariants expected by range-key, suffix-rewrite, and validation paths.

The implementation must not report the complete key length as the logical prefix length merely because the whole key is physically prefix-encoded. `PrefixLen` still drives Pebble prefix semantics.

### 11.5 Required KeySeeker behavior

The seeker must:

- decode complete logical keys;
- implement lower-bound seeks equivalent to the comparer;
- support keys shorter than any normal Bond key without panicking;
- handle exact hits, gaps, before-first, and after-last seeks;
- support forward and reverse iteration;
- work with prefix seeks using Bond's `Split` behavior;
- reconstruct keys without aliasing mutable buffers incorrectly;
- support concurrent independent seeker instances; and
- participate correctly in synthetic prefix/suffix and block-property paths used by Pebble.

Use Pebble's custom schema tests as exemplars, especially:

- `/home/peter/Dev/other/pebble/cockroachkvs/key_schema_test.go:31`
- `/home/peter/Dev/other/pebble/cockroachkvs/key_schema_test.go:124`
- `/home/peter/Dev/other/pebble/cockroachkvs/cockroachkvs_test.go:237`
- `/home/peter/Dev/other/pebble/cockroachkvs/cockroachkvs_test.go:334`

### 11.6 Bundle sizes

Benchmark at least bundle sizes 16, 32, and 64. Use different durable names if bundle size is semantically embedded in the reader/writer configuration, for example:

```text
bond/full-key/v1-b16
bond/full-key/v1-b32
bond/full-key/v1-b64
```

Do not select a bundle size from microbenchmarks alone. It can affect encoded size, seek work, cache behavior, and block construction CPU.

### 11.7 Activation gate

Suggested—not mandatory—gate:

- at least 10% reduction in secondary-index SST bytes on a representative corpus; and
- no unacceptable regression in the common primary-get and secondary-scan workloads; and
- no material correctness or operational complication.

If the result is neutral, stop. The experiment is still valuable because it answers whether physical prefix encoding alone can address the measured PK-tail corpus.

## 12. Typed per-table schema families

Typed schemas are a later optimization. They should only begin after the global schema has established a correct benchmark harness and after the range-selection extension exists.

### 12.1 Why typed primary-key tails may help

Pebble's columnar `DataTypeUint` can encode unsigned integer values using 0, 1, 2, 4, or 8 bytes and may delta-encode suitable sequences (`sstable/colblk/column.go:20-29`). Bond's logical integer encoding includes type/framing bytes. Sequential or locally clustered integer primary keys repeated across indexes may therefore compress more efficiently as a typed physical column.

The gain is workload-dependent:

- sequential IDs are promising;
- uniformly random 64-bit IDs may show less benefit;
- long string/byte IDs may benefit more from prefix encoding than typed integers;
- composite IDs require a precise unambiguous parser; and
- block compression may already capture much of the redundancy.

### 12.2 Start per table, not per index

A table range contains its primary records and secondary indexes. The primary-key tail is shared across those entry types. A table-aware schema can encode:

```text
PrefixBytes(table/index/index/order portion) | Uint(primary-key tail)
```

or a safe equivalent.

Per-table policy provides three benefits:

- it targets the repeated tail across all indexes;
- it requires at most one policy boundary per table; and
- it avoids creating a separate output boundary for every index.

Only consider per-index schemas when an index-specific typed order/key column wins enough space or scan performance to justify more files and routing complexity.

### 12.3 Reusable families

Prefer a small stable family registry to dynamically generated schema names:

```text
bond/pk-u64/v1
bond/pk-u32/v1
bond/pk-bytes/v1
bond/pk-opaque/v1
```

Possible later variants:

```text
bond/pk-u64-order-u64/v1
bond/pk-u64-order-time/v1
bond/pk-bytes-order-u64/v1
```

Every family must be registered on every open after it has ever written live/restorable SSTs, even when no current table chooses it.

### 12.4 Conditional parsing within a table schema

A table range includes multiple `IndexID` layouts. A schema may inspect stable header bytes and decompose variants into the same physical column types, but it must:

- parse all valid primary and secondary keys for that table;
- define behavior for range keys and short bounds;
- fail safely during validation, not silently misdecode;
- keep logical ordering exactly equivalent to the comparer; and
- use a new schema name if the parser changes.

If parsing cannot be made total and obviously correct, use `bond/pk-opaque/v1` or `bond/full-key/v1` for that table.

### 12.5 Boundary coalescing

Compile routes in key order and merge adjacent ranges when all policy fields match:

```text
before coalescing:
  table 1 -> bond/pk-u64/v1
  table 2 -> bond/pk-u64/v1
  table 3 -> bond/pk-bytes/v1

after coalescing:
  [table 1 start, table 3 start) -> bond/pk-u64/v1
  [table 3 start, table 4 start) -> bond/pk-bytes/v1
```

This reduces forced SST boundaries. The compiler must preserve any different compression, value-storage, or other span-policy fields when deciding whether routes are truly identical.

## 13. Focused Pebble extension for per-range schemas

This phase should be pursued only after the global experiment demonstrates value and Bond has a stable catalog/routing model.

### 13.1 Proposed public shape

Conceptually add an optional field:

```go
type SpanPolicy struct {
    KeyRange KeyRange

    // Existing policy fields...
    PreferFastCompression bool

    // KeySchemaName selects the registered KeySchema for SSTs produced for
    // this range. Empty means the database-wide Options.KeySchema.
    KeySchemaName string
}
```

Exact naming is subject to Pebble maintainer feedback. An empty field must preserve current behavior.

Pebble's current contract says correctness must not depend on receiving a particular span policy and permits the policy function to change its answer. Schema selection still satisfies that rule: every registered schema reconstructs the same logical user keys, and changing the policy only changes a new SST's physical encoding. Bond must never put table semantics into a schema that are required to interpret the logical key outside that SST reader.

### 13.2 Compaction integration

After Pebble computes ordinary writer options and after it obtains the policy for the current output range:

```go
writerOpts := d.makeWriterOptions(c.eventualOutputLevel)

if spanPolicy.KeySchemaName != "" {
    schema, ok := d.opts.KeySchemas[spanPolicy.KeySchemaName]
    if !ok {
        return errors.Errorf(
            "span policy references unknown key schema %q",
            spanPolicy.KeySchemaName,
        )
    }
    writerOpts.KeySchema = schema
}

objMeta, tw, err := d.newCompactionOutputTable(jobID, c, writerOpts)
```

The concrete patch belongs near the current `makeWriterOptions` / `newCompactionOutputTable` flow around `compaction.go:2882-2899`.

Flush and any other SST-producing paths that honor `SpanPolicyFunc` must receive the same review. Do not assume compaction coverage automatically proves flush coverage.

### 13.3 Validation

Pebble should reject an unknown schema before creating an output file. Validation options include:

- validating all static policies at open time when enumerable; and
- validating every dynamic policy result at use time.

Bond's compiled policy is enumerable, so Bond itself should validate all names before calling `pebble.Open`.

### 13.4 Policy identity methods

Update all helpers that define or expose policy equality/default state:

- `SpanPolicy.IsDefault`
- `SpanPolicy.String`
- static policy construction/sorting
- option serialization if applicable
- test formatting/golden files
- any policy comparison used for boundary coalescing

Omitting the field from equality/default logic can merge ranges incorrectly or hide schema policy in diagnostics.

### 13.5 SST boundary semantics

One SST has one schema. Therefore, when the schema changes at key `K`, the preceding SST must end no later than `K`, and the next SST starts under the new schema. Reuse the existing `KeyRange.End` splitting mechanism rather than adding a parallel partitioner.

Test:

- a flush spanning two schema ranges;
- a compaction spanning two and three ranges;
- adjacent ranges sharing a schema;
- empty tables/ranges;
- range deletion crossing a boundary;
- an exact boundary key;
- L0 files initially spanning ranges, if possible in the chosen flush design;
- manual compaction;
- ingest and rewrite paths;
- mixed old/new schema inputs;
- reopen with every schema registered;
- reopen failure when a required schema is intentionally omitted; and
- fallback to global schema when the policy field is empty.

### 13.6 Upstream strategy

Keep the Pebble change generic:

- no Bond imports or Bond key assumptions;
- schema selected only by registered name;
- no per-write metadata;
- no new on-disk format;
- no schema creation from arbitrary option strings at compaction time; and
- comprehensive Pebble-native tests.

Before maintaining a long-lived fork, propose the narrow field and use case upstream on issue #4380 or a linked design/PR. If upstream declines, isolate the patch and continuously rebase/test it against the exact Pebble commit pinned by Bond.

## 14. Existing logical-key correctness hazard

`keyBuilder.AddStringField` and `AddBytesField` in `keys.go:122-131` append raw variable-width bytes after a field ordinal without an escaping or length delimiter for the content itself. This allows ambiguous concatenation across adjacent variable-width fields.

For example, two logical tuples can produce the same bytes:

```text
("a",    "\x02b")
("a\x02", "b")
```

The exact collision depends on the ordinal bytes and builder sequence, but the structural problem is that a parser cannot know where one raw variable-width value ends and the next begins.

Implications:

- Existing opaque full-key physical encoding does not need to parse these fields and is not blocked by this issue.
- A typed schema that tries to split inner composite fields would be unsafe until the logical encoding is unambiguous.
- Any logical fix changes key bytes and requires versioned migration; silently changing current builders would make existing records/indexes unreachable.
- Tests should establish the current collision behavior before designing key format v2.

This deserves a separate high-priority correctness issue even if schema work is deferred.

## 15. Optional logical-layout projects

These ideas can reduce more than physical KeySchemas because they change bytes in the WAL, memtable, batches, filters, and SSTs. They also carry much larger migration and semantic costs. Each must have an independent design and opt-in version.

### 15.1 Logical compact key v2

Define a self-delimiting, memcomparable tuple codec that:

- unambiguously encodes variable-width fields;
- preserves intended sort ordering;
- reduces repeated four-byte length words and per-field overhead where possible;
- supports ascending/descending fields explicitly;
- defines null and empty values;
- defines type/version tags;
- produces exact prefix bounds; and
- supports forward/reverse iteration.

Potential benefits:

- smaller WAL and memtables;
- smaller batch and iterator keys;
- smaller filters and index separators;
- less cache pressure; and
- easier safe typed decomposition.

Costs:

- a full logical migration or dual-read period;
- every primary and secondary key must be rebuilt;
- query pagination tokens and saved bounds may change;
- backups/dumps need explicit version handling; and
- old and new keyspaces need collision-free separation.

Do not start this until the physical-only experiment establishes how much remains to gain.

### 15.2 Surrogate row-ID table mode

For tables with long logical primary keys and many indexes, assign an internal monotonic `uint64` row ID:

```text
logical PK -> row ID mapping
row ID     -> record
secondary index key -> row ID tail
```

All mappings and index entries must be written atomically in one batch.

Potential benefit:

- a fixed compact tail repeated across every secondary index;
- excellent typed/delta encoding for clustered row IDs;
- smaller index blocks and filters.

Costs and semantic changes:

- primary lookup by logical key adds a mapping lookup;
- row-ID allocation must be crash-safe and concurrent;
- deletes and updates touch mapping state;
- pagination/tie ordering changes unless carefully specified;
- imports/restores need allocation rules; and
- migration rebuilds all table indexes.

This should be an opt-in new table storage mode, never an invisible retrofit.

### 15.3 Unique-index primary key in value

For a declared unique index, the index key can contain only the unique index fields/order while the primary key is stored in the value.

Potential benefit:

- smaller hot index keys, separators, and filter inputs;
- avoids appending a long primary key to the unique search key.

Limitations:

- total bytes may remain similar because the PK moves to the value;
- reads must fetch/decode the value;
- uniqueness enforcement must be atomic and explicit;
- covering behavior and iteration semantics change; and
- it is invalid for generic non-unique indexes because equal keys overwrite each other.

Benchmark it only for indexes declared and enforced as unique.

### 15.4 Posting-list indexes

For high-fanout equality/membership indexes, store chunked compressed row-ID posting lists instead of one KV per record:

```text
index term | chunk -> compressed sorted row IDs
```

Potential benefit:

- large space reduction for repeated terms;
- fast intersections and membership operations.

Costs:

- hot-key write contention;
- chunk split/merge policy;
- read-modify-write complexity;
- deletion/tombstone handling;
- transaction and concurrency rules;
- pagination over chunks; and
- dependence on surrogate row IDs.

Treat this as a separate `IndexKind`, not an optimization hidden behind current indexes.

### 15.5 Key-only index projection

Current secondary keys already contain index, order, and primary-key information. Bond can expose a decoded key-only projection so scans that only need those fields avoid primary record fetches without adding index bytes.

This is mostly an API/decoder feature. A broader covering index that stores selected record fields in the value may improve reads but increases index size and update amplification. Keep those two concepts separate.

### 15.6 Partial indexes

Bond already supports an index filter callback. A partial index reduces entries roughly in proportion to excluded rows and may beat sophisticated encoding when a query only needs a subset.

Enhance it by making predicate identity/version explicit in catalog metadata and providing safe rebuild tooling. An opaque changed callback can otherwise leave an index containing a mixture of old and new predicate results.

### 15.7 Better separators

Bond currently inherits bytewise separator/successor behavior. Custom separator logic or a future columnar index-block schema may reduce index-block size, but this is lower priority than:

- correcting compression;
- encoding full key prefixes;
- reducing repeated primary-key tails; and
- fixing ambiguous logical fields.

Measure index-block bytes separately before investing here.

## 16. Alternatives rejected for the default design

### 16.1 Multiple Pebble DBs

Rejected because it loses simple cross-table atomic batches and multiplies WAL, cache, manifest, compaction, backup, and file-descriptor overhead.

### 16.2 Per-`Set` schema hints

Rejected because schemas are SST-writer properties, not stable mutation properties. Hints would need invasive persistence through WAL/memtable/compaction and would still require range partitioning.

### 16.3 Hash every primary key

Rejected as a default because:

- collision handling remains necessary;
- order/range semantics are lost;
- the original key often still needs storage;
- hashing adds CPU; and
- secondary indexes still require disambiguation.

A fingerprint may be useful as an auxiliary accelerator, never as an unverified identity.

### 16.4 Move the PK into every secondary value

Rejected for non-unique indexes because identical index/order keys would overwrite each other. Adding an ordinal or row ID simply moves the need for uniqueness elsewhere.

### 16.5 Activate `FormatNewest`

Rejected as part of this plan because it couples reversible schema experiments to a one-way format ratchet without a demonstrated dependency.

### 16.6 Generate one schema per table callback

Rejected because opaque callbacks do not provide a durable parseable type contract, and unbounded schema names increase compatibility burden. Use explicit reusable families and an opaque fallback.

## 17. Pebble upgrade plan

The local compatibility experiment showed that Bond can compile and pass its test suite against the reviewed Pebble master after concentrated API adaptations. Those temporary changes were not made to this worktree.

Expected adaptation areas include:

- move Bloom imports from the old `pebble/bloom` location to the current `pebble/sstable/tablefilters/bloom` package;
- flatten options that moved out of `opts.Experimental`;
- replace older `FilterPolicy` / `FilterType` configuration with the functional table-filter policy API;
- update `SpanPolicyFunc` from the old start-key signature to the bounds-based signature returning `(SpanPolicy, error)`;
- update `ValueStoragePolicy` construction for current fields; and
- provide a positive `MinimumMVCCGarbageSize` where required by current validation.

The temporary compatibility run completed `go test -mod=mod ./...`; the backup package dominated runtime at roughly 345 seconds. Reproduce this from a clean branch during implementation and retain the logs.

### 17.1 Semantic decisions during upgrade

Do not treat compilation as the entire upgrade. Review:

- current value-separation defaults;
- table-filter policy defaults;
- compression names and levels;
- format-major-version behavior;
- cache sizing/accounting;
- iterator option changes;
- ingest behavior;
- SpanPolicy invocation on flush and compaction; and
- any changed experimental-to-stable option semantics.

### 17.2 Upgrade verification

At minimum:

```bash
cd /home/peter/Dev/0xsequence/bond
go test ./...
go test -race ./...

go test . -run 'TestBond_(BackupRestore|RestoreDifferentVersion|Table_Index)|Test_BondVersionMigrate|TestBond_VersionCheck|TestIndex_Operations|TestBond_Query'
go test ./tests -run 'TestBackupRestore_'
go test ./backup -run 'Test(RestoreComplete|RestoreWithIncrementals|BackupIncremental)'
```

`make test` may also be used, but inspect `Makefile` first: its cleanup targets remove repository test database directories. It should never be pointed at developer or production data.

For Pebble changes:

```bash
cd /home/peter/Dev/other/pebble
go test -tags invariants .
go test -tags invariants ./...
```

Run Pebble's race, metamorphic, and stress tests appropriate to the changed packages before proposing an upstream PR.

## 18. Compatibility, migration, and rollback

### 18.1 Schema names are storage-format versions

Rules:

1. Never change the implementation behind a used schema name.
2. Never remove a schema reader while a live, backup, checkpoint, or ingested SST may name it.
3. New behavior receives a new immutable name.
4. The registry is assembled after the Bond comparer is configured and before `pebble.Open`.
5. All processes opening the DB—including tools and migrations—must use the same reader registry.

### 18.2 Reader-before-writer rollout

Use at least two releases:

#### Release A: reader-only

- upgrades Pebble;
- registers legacy and new Bond schemas;
- leaves the active writer on the legacy/default schema;
- updates backup metadata and inspectors;
- verifies that every deployed binary can open an SST naming the new schema; and
- adds metrics/logging for encountered schema names.

#### Release B: canary writer

- selects `bond/full-key/v1-...` for new outputs in a controlled environment;
- leaves all legacy readers registered;
- monitors file size, compaction cost, query latency, and reopen/restore behavior; and
- supports selecting the legacy writer again without deleting the new reader.

Typed per-range writers need the same reader-before-writer process for each new family.

### 18.3 Mixed-schema steady state

Mixed schemas are expected:

```text
old L5 SST -> DefaultKeySchema(...)
new L0 SST -> bond/full-key/v1-b32
new L4 SST -> bond/pk-u64/v1
```

Pebble readers resolve each table independently. Natural compaction gradually rewrites data. Avoid a forced full compaction solely to make schema usage uniform unless measurements show a compelling operational reason.

### 18.4 Rollback

Safe rollback means:

- switch new output to a previously supported writer schema;
- keep every newer reader schema registered;
- do not downgrade to binaries released before reader support;
- allow compaction to rewrite gradually; and
- restore only into binaries whose registry satisfies backup metadata.

Deleting the new registry entry is not rollback; it can make the DB impossible to open.

### 18.5 Backup/checkpoint metadata

Pebble checkpoints copy schema-bearing SSTs. Current Bond backup metadata records Pebble/Bond version information but does not appear to declare the schema-reader set required by the files.

Add metadata such as:

```text
StorageReaderEpoch: 2
RequiredKeySchemas:
  - DefaultKeySchema(bond-comparer,16)
  - bond/full-key/v1-b32
  - bond/pk-u64/v1
```

Derive the set from actual table properties when practical; otherwise conservatively record every schema enabled in the source DB. Restore must reject an unsupported required schema before replacing/opening destination data.

The exact metadata format must be versioned and backward compatible.

### 18.6 Logical dump as an escape hatch

Bond's logical dump path writes row data in Pebble v2 form around `bond.go:584-587`. Because it reconstructs logical records rather than copying physical SST encoding, it can serve as a schema-neutral migration/recovery route.

Verify this assumption with tests. If dump includes raw internal keys or SSTs in any mode, document the distinction explicitly.

### 18.7 Format migration and tooling

Every path that calls `pebble.Open` must have the schema registry, including:

- normal Bond open;
- `MigratePebbleFormatVersion`;
- backup validation/restore;
- repair or inspection tools;
- benchmarks opening copied production data; and
- one-off operational utilities.

Centralize options construction so tools cannot accidentally omit schemas.

## 19. Benchmark and experiment design

### 19.1 Principle

No compact-key design should ship from encoded-size theory alone. Measure full lifecycle behavior: loading, flushing, compaction, reopening, querying, backup, and mixed-schema operation.

### 19.2 Engine matrix

Use at least:

| ID | Engine/configuration | Purpose |
|---|---|---|
| A | Current pinned Pebble and current Bond options | Production baseline |
| B | Pebble master, logical format unchanged, compression policy corrected | Isolate upgrade/config benefit |
| C16 | B + global `bond/full-key/v1-b16` | Full-key schema candidate |
| C32 | B + global `bond/full-key/v1-b32` | Full-key schema candidate |
| C64 | B + global `bond/full-key/v1-b64` | Full-key schema candidate |
| D | Pebble master/fork + typed per-table schemas | Measure incremental typed/range benefit |

Within B, independently test:

- current filter policy;
- progressive Bloom; and
- progressive binary fuse.

Do not multiply every dimension blindly. First find a compression baseline, then filter baseline, then compare schemas against that fixed result.

### 19.3 Key-shape matrix

Generate or replay:

- sequential `uint64` primary keys;
- random `uint64` primary keys;
- 20-byte, 32-byte, and 64-byte primary keys;
- UUID-like and address-like keys;
- two- and three-part composite keys;
- no order component;
- fixed-width integer/time order components;
- variable string/byte order components;
- low-cardinality index values;
- high-cardinality index values;
- common-prefix and random-prefix strings;
- 1, 3, and 8 secondary indexes per record; and
- partial indexes with representative selectivity.

Include a sanitized distributional replay of a real Bond workload if permitted. Synthetic data should be deterministic and seed-recorded.

### 19.4 Lifecycle matrix

For each serious candidate:

1. Fresh bulk load.
2. Incremental insert workload.
3. Updates that change no indexes.
4. Updates that change one/many indexes.
5. Deletes and tombstone-heavy periods.
6. Upserts.
7. Memtable flush.
8. Natural compaction to steady levels.
9. Manual full compaction as a diagnostic, not a production assumption.
10. Close and reopen.
11. WAL replay after an unclean-stop fixture.
12. Checkpoint and restore.
13. Logical dump and ingest/restore.
14. Mixed legacy/new schema compaction.
15. Writer rollback while retaining readers.

### 19.5 Read matrix

Measure cold and warm cache for:

- primary-key hit;
- primary-key miss;
- unique secondary exact lookup;
- secondary prefix scan;
- short `SeekGE` bounds;
- forward scan;
- reverse scan;
- pagination resume;
- range-bounded ordered scan;
- low/high selectivity intersection;
- index-only projection; and
- record materialization after index scan.

### 19.6 Metrics

Record:

- total DB bytes;
- total SST bytes;
- data-block, index-block, filter, properties, and value bytes;
- bytes per primary row;
- bytes per secondary index entry;
- logical versus physical key bytes where instrumentation permits;
- WAL bytes;
- memtable memory;
- L0 file count and overlap;
- files per level;
- write amplification;
- compaction read/write bytes;
- flush and compaction CPU/time;
- foreground write throughput and p50/p95/p99 latency;
- query throughput and p50/p95/p99 latency;
- allocations/op;
- block-cache hit/miss behavior;
- filter false-positive rate;
- bytes read per operation; and
- open/recovery/restore time.

Capture `pebble.DB.Metrics()` and SST/table-property inspection output alongside benchmark results.

### 19.7 Correctness oracle

For every generated dataset, compare candidate engines to a logical oracle:

- exact primary record set;
- exact ordered index entry sequence;
- exact forward and reverse query results;
- query bounds at every generated prefix edge;
- delete/update visibility across snapshots;
- reopen equivalence;
- backup/restore equivalence; and
- mixed-schema compaction equivalence.

### 19.8 Reproducibility

Every result directory should record:

- Bond SHA;
- Pebble SHA;
- Go version;
- OS/kernel/architecture;
- benchmark seed;
- catalog/layout version;
- schema names and bundle sizes;
- complete Pebble options;
- data generator parameters;
- warm-up/run counts; and
- machine/storage information.

### 19.9 Benchmark commands

Adapt the existing benchmark package rather than creating an untracked one-off program:

```bash
cd /home/peter/Dev/0xsequence/bond/_benchmarks
go test -timeout=25m -bench=. -benchmem
```

Add focused named benchmarks so configurations can be compared with `benchstat`. Large lifecycle runs may need a separate command under `_benchmarks`, but it should emit machine-readable JSON/CSV and a manifest.

## 20. Correctness test plan

### 20.1 Key schema unit tests

- empty key if Pebble can present one as a bound;
- one-byte and otherwise truncated Bond-like keys;
- minimum complete primary key;
- primary and secondary layouts;
- empty index/order values;
- long keys;
- adjacent shared prefixes of every length;
- duplicate user keys with different internal sequence/kind;
- ordered and random input;
- exact seek and in-between seek;
- before-first and after-last seek;
- forward/reverse round trip;
- prefix iteration;
- randomized comparison against bytewise ordering;
- encoded block round trip;
- corruption/truncation failure behavior;
- independent concurrent seekers;
- synthetic prefix/suffix transforms; and
- bundle-size variants.

### 20.2 Bond integration tests

- primary insert/get/update/delete;
- every current index operation;
- unique and non-unique indexes;
- empty secondary values;
- partial indexes;
- query intersections;
- pagination;
- reverse scans;
- snapshots and batches;
- cross-table atomic batch;
- close/reopen;
- WAL replay;
- range deletion;
- ingest;
- backup/restore;
- logical dump/restore;
- version checks;
- format migration; and
- old and new schemas in one database.

### 20.3 Range-schema tests

- catalog compiles sorted disjoint routes;
- adjacent identical routes coalesce;
- unknown schema names fail before open;
- a flush crossing table boundaries creates correctly named SSTs;
- compaction output changes schema exactly at the table boundary;
- empty policy schema uses global default;
- files remain readable after the active writer changes;
- removing a required reader causes a clear deterministic open failure;
- per-table primary and every index decode correctly;
- policy boundary does not lose or duplicate a key; and
- file fragmentation metrics stay within the agreed budget.

### 20.4 Fuzz/property tests

Property tests should generate valid and invalid keys and prove:

```text
Compare(logicalA, logicalB)
    == sign(position(decode(encode(A))) - position(decode(encode(B))))

decode(encode(key)) == key

seek(target) == first key K where Compare(K, target) >= 0

Split(decodedKey) == Split(originalKey)
```

For typed schemas, generate all supported table/index variants and random short bounds that stop in every field.

## 21. Phased implementation plan

Each phase is independently reviewable. Do not begin a later phase merely because an earlier branch exists.

### Phase 0: freeze baselines and create reproducible fixtures

Goal: establish trustworthy before/after comparison.

Steps:

1. Record Bond and Pebble SHAs, Go version, and current Pebble options.
2. Add deterministic benchmark data generators for the key-shape matrix.
3. Capture a sanitized real-world key-shape histogram if available.
4. Measure the current pinned engine after steady-state compaction.
5. Save table properties, Pebble metrics, result counts, and benchmark outputs.
6. Add a logical correctness oracle used by every later candidate.

Verification:

- repeated runs with the same seed produce the same logical dataset and near-stable size;
- all existing tests pass; and
- benchmark manifests contain enough information to reproduce a run.

STOP if baseline variance is too high to distinguish a 5–10% size change.

### Phase 1: upgrade Bond to reviewed Pebble master

Goal: adopt current Pebble APIs and fixes without changing logical keys, active KeySchema, or format-major version.

Steps:

1. Create a dedicated upgrade branch.
2. Update the Pebble module revision to the approved reviewed commit—not an unrecorded moving `master`.
3. Update Bloom/table-filter imports and option APIs.
4. Convert `SpanPolicyFunc` to the bounds-based signature.
5. Adapt `ValueStoragePolicy` and validate semantic defaults.
6. Keep `PebbleDBFormat = pebble.FormatV2BlobFiles`.
7. Keep the legacy/default KeySchema active.
8. Run focused tests, full tests, race tests, and restore/version tests.
9. Repeat Phase 0 benchmarks and investigate any unexplained change.

Verification:

```bash
go test ./...
go test -race ./...
```

Expected outcome: a clean upgrade-only commit/PR with no compact-key claim.

STOP if upgrade semantics cannot be isolated or existing database reopen/restore fails.

### Phase 2: correct compression policy and select filter baseline

Goal: capture low-complexity storage wins before custom schema work.

Steps:

1. Add a regression test showing how `PreferFastCompression` affects writer compression.
2. Remove the all-keyspace `PreferFastCompression=true`, or scope it only to a documented range.
3. Benchmark Bond's balanced and good compression profiles.
4. With compression fixed, compare current, progressive Bloom, and progressive binary-fuse table filters.
5. Select one baseline configuration for all schema experiments.
6. Document compaction CPU/write-latency tradeoffs, not only final bytes.

Verification:

- configured deep-level compression is observable in SST properties;
- filter false-positive and size metrics are collected; and
- all query results remain identical.

STOP if a proposed default improves space but violates the agreed compaction/latency budget. It may remain an opt-in profile.

### Phase 3: implement and prove `bond/full-key/v1`

Goal: test complete-key physical prefix encoding on stock Pebble master.

Steps:

1. Implement schema construction in a small isolated Bond package/file.
2. Use immutable names for bundle-size candidates.
3. Implement KeyWriter and KeySeeker with no logical key changes.
4. Port/adapt Pebble custom-schema unit and randomized test patterns.
5. Add Bond-specific key corpus and short-bound tests.
6. Add mixed legacy/full-key SST integration tests.
7. Benchmark b16/b32/b64 against the selected Phase 2 baseline.
8. Inspect data, index, filter, and total SST bytes separately.
9. Select a winner or stop without shipping a custom schema.

Verification:

- `decode(encode(key)) == key` across fuzz corpus;
- ordering and seek oracle pass;
- all Bond tests pass with both writer schemas;
- reopen succeeds with mixed schemas; and
- performance/size results meet the approved gate.

STOP on any unexplained iterator, prefix, reverse-scan, or short-bound mismatch.

### Phase 4: reader registry, metadata, and tooling

Goal: make schema use operationally safe before any production writer changes.

Steps:

1. Centralize Bond's Pebble options construction after comparer initialization.
2. Always register legacy/default and approved Bond schemas.
3. Add required-schema/storage-reader metadata to backups/checkpoints.
4. Update restore validation.
5. Update `MigratePebbleFormatVersion` and every tool that opens Pebble.
6. Add an inspector that lists SST schema names/counts/bytes.
7. Add metrics for unknown/encountered/active schema names.
8. Release reader-only support while the legacy writer remains active.

Verification:

- old DB + new binary opens;
- new-schema test DB + reader-only binary opens;
- intentionally omitted schema fails clearly;
- backup restore checks required schemas before destructive replacement; and
- logical dump round-trips across schemas.

STOP if any operational open path bypasses centralized registry construction.

### Phase 5: canary the global schema writer

Goal: validate production-like behavior with a reversible writer switch.

Steps:

1. Add an explicit option/feature flag for the active writer schema.
2. Default it conservatively according to rollout policy.
3. Enable on a canary copy/workload after all readers are deployed.
4. Observe natural mixed-schema compaction.
5. Exercise checkpoint, restore, restart, rollback, and manual compaction.
6. Compare canary metrics to Phase 2 baseline over a meaningful duration.
7. Decide whether to make the global schema default, opt-in, or abandoned.

Verification:

- switching back to legacy writer requires no data rewrite;
- both schema readers remain present;
- no forced full compaction is required; and
- long-running metrics meet the agreed gate.

STOP if canary rollback requires removing new readers or downgrading binaries.

### Phase 6: introduce the declarative catalog

Goal: describe storage identities and layouts independently of physical routing.

Steps:

1. Specify immutable catalog/table/index descriptor types.
2. Separate durable descriptor/version strings from Go extractor callbacks.
3. Validate IDs, names, layouts, and schema-family compatibility.
4. Compile bound typed handles.
5. Preserve the existing dynamic API as global-schema fallback.
6. Persist/validate a storage-definition fingerprint.
7. Add schema-diff diagnostics explaining incompatible changes.
8. Add the hypothetical multi-table usage as a compile-tested example using the final API.
9. Do not yet enable per-table schema routing.

Verification:

- existing API tests pass;
- catalog examples open one DB and atomically write multiple tables;
- duplicate/drifted definitions fail deterministically; and
- catalog-bound and legacy tables coexist.

STOP if the catalog requires serializing opaque callback code or breaks existing callers without a migration path.

### Phase 7: implement/propose Pebble `SpanPolicy.KeySchemaName`

Goal: select physical KeySchema by output range with a minimal generic Pebble change.

Steps:

1. Write Pebble-native API documentation and tests first.
2. Add the optional policy field and include it in default/string/equality behavior.
3. Resolve the registered schema into output writer options.
4. Verify flush, compaction, manual compaction, ingest/rewrite, and range-key paths.
5. Test exact output boundaries and mixed schema inputs.
6. Run Pebble invariants, race, metamorphic, and relevant stress tests.
7. Propose upstream with issue #4380 context.
8. If a fork is temporarily required, pin the exact commit and isolate the patch.

Verification:

```bash
cd /home/peter/Dev/other/pebble
go test -tags invariants .
go test -tags invariants ./...
```

STOP if correct flush behavior requires persisting per-write hints or a broad WAL/memtable redesign. Revisit the architecture rather than expanding the patch invisibly.

### Phase 8: typed per-table schema families

Goal: measure incremental gains from decomposing the repeated PK tail.

Steps:

1. Implement safe `pk-u64`, `pk-u32`, `pk-bytes`, and opaque families.
2. Compile catalog table ranges to family names.
3. Coalesce adjacent identical policies.
4. Add parser/fuzz tests for every primary/index key variant.
5. Start with a copied benchmark DB, not production writer activation.
6. Measure storage gain beyond the global full-key schema.
7. Measure file counts, L0 overlap, read amplification, and compaction splitting.
8. Select per-table routing only for families with convincing gains.

Verification:

- every table's complete logical key corpus round-trips;
- output SST schema names match routes;
- range boundaries do not change result sets; and
- fragmentation remains within an approved limit.

STOP if incremental savings do not justify the Pebble extension and catalog/runtime complexity.

### Phase 9: evaluate per-index typed schemas

Goal: determine whether selected hot/large indexes merit their own schema.

Steps:

1. Rank indexes by bytes and workload importance.
2. Prototype only the top one or two shapes.
3. Measure incremental data/index bytes and scan performance.
4. Quantify forced SST boundaries and files-per-level.
5. Require explicit catalog opt-in.

STOP unless the per-index gain is materially larger than per-table routing and operational costs remain acceptable.

### Phase 10: separately plan logical key/index v2 features

Goal: pursue remaining high-value opportunities without contaminating physical-schema compatibility.

Order of consideration:

1. Fix/version ambiguous variable-width tuple fields.
2. Productize versioned partial-index predicates.
3. Add key-only index projections.
4. Prototype surrogate row-ID table mode for long-PK/high-index-count workloads.
5. Prototype unique-index PK-in-value layout.
6. Prototype posting lists only after row IDs.
7. Consider covering values and separator work when measurements point there.

Each item requires its own migration, compatibility, benchmark, and rollback plan.

## 22. Evidence and prioritization table

| Finding/opportunity | Impact | Effort | Risk | Confidence | Evidence/notes |
|---|---:|---:|---:|---:|---|
| Global `PreferFastCompression` overrides deep compression | High potential space impact | Small | Low–medium | High | Bond `options.go:342-346`; Pebble `compaction.go:2882-2888` |
| Upgrade to Pebble master | Medium–high | Small–medium | Medium | High | Temporary compatibility run passed full tests; API churn concentrated in options |
| Global full-key PrefixBytes schema | Medium potential | Medium | Medium–high correctness risk | Medium | Default leaves Bond suffix raw; stock master supports global custom schema |
| Per-range schema via `SpanPolicy` | Enables typed routing | Medium | Medium | High architectural confidence | Master already splits output at policy range ends |
| Typed per-table PK-tail schema | Medium–high on suitable IDs | Medium–large | High | Medium | Repeated PK corpus is large; `DataTypeUint` can compact/delta encode |
| Progressive/binary-fuse filters | Small–medium | Small | Low | Medium | Present on master; workload-dependent |
| Catalog before open | High enabling value | Large | Medium | High | Registry and route policy must be known at `pebble.Open` |
| Ambiguous variable-width key fields | Correctness impact | Medium migration | High | High | Raw adjacent variable fields in `keys.go:122-131` |
| Logical key v2 | High potential | Large | High | Medium | Removes overhead everywhere but requires rebuild/migration |
| Surrogate row IDs | High for long PK + many indexes | Large | High | Medium | Replaces repeated long tails; changes lookup and allocation semantics |
| Unique PK-in-value | Targeted | Medium | Medium | Medium | Valid only for explicitly unique indexes |
| Posting lists | High for high fanout | Very large | High | Low–medium | Requires row IDs and new concurrency/update design |
| Multiple Pebble DBs | Negative architectural tradeoff | Large | High | High | Loses cross-table atomicity and multiplies resources |

## 23. Risks and mitigations

### 23.1 Custom schema silently misorders or mis-seeks keys

Mitigation:

- preserve bytewise comparer semantics;
- property-test against a simple oracle;
- include partial/short bounds;
- port Pebble's custom-schema tests;
- run invariants/race/metamorphic tests; and
- deploy readers before writers.

### 23.2 Too many schema boundaries fragment SSTs

Mitigation:

- start global;
- then route per table;
- coalesce identical adjacent policies;
- instrument forced boundaries and files per level; and
- require a stronger gate for per-index routing.

### 23.3 A backup needs a schema a restore binary lacks

Mitigation:

- record required schema names/reader epoch;
- validate before restore;
- centralize registry construction; and
- never remove used readers.

### 23.4 Rolling downgrade cannot read new SSTs

Mitigation:

- reader-only release first;
- define rollback as writer selection, not binary downgrade;
- keep schema readers indefinitely; and
- test mixed-version operational procedures.

### 23.5 Compression gain is mistaken for schema gain

Mitigation:

- phase and benchmark compression/filter changes first;
- freeze the selected baseline; and
- report component-level SST bytes.

### 23.6 Catalog definition drifts from on-disk data

Mitigation:

- stable IDs and explicit versions;
- deterministic fingerprint;
- open-time validation;
- storage-definition diff diagnostics; and
- rebuild/migration commands for intentional changes.

### 23.7 Typed parser encounters legacy/unknown key form

Mitigation:

- use total parsers with safe fallback only when ordering remains provably correct;
- route legacy/dynamic ranges to opaque/global schema;
- validate representative keys before activation; and
- use new names for every parser version.

### 23.8 Pebble fork drifts

Mitigation:

- keep extension tiny and generic;
- upstream it;
- pin exact SHAs;
- rerun Pebble invariants/metamorphic tests on rebases; and
- avoid Bond-specific Pebble code.

### 23.9 Better size harms latency or CPU

Mitigation:

- define multidimensional gates;
- measure cold/warm read latency and compaction CPU;
- retain opt-in profiles; and
- choose operationally balanced defaults rather than minimum bytes at any cost.

## 24. Observability and operational tooling

Before writer activation, expose enough state to diagnose a mixed-schema database.

Recommended inspection output:

```text
active writer schema: bond/full-key/v1-b32
registered readers:
  DefaultKeySchema(bond-comparer,16)
  bond/full-key/v1-b32
  bond/pk-u64/v1

SST schemas:
  DefaultKeySchema(...):  47 files, 18.2 GiB
  bond/full-key/v1-b32:    9 files,  2.1 GiB
  bond/pk-u64/v1:          3 files,  0.4 GiB

catalog routes:
  [0x01,0x02): bond/pk-u64/v1
  [0x02,0x03): bond/full-key/v1-b32
```

Metrics/logs should include:

- active writer schema;
- schemas encountered while opening tables;
- unknown-schema failures;
- bytes/files by schema;
- policy boundary splits;
- table/index range to schema mapping;
- compression/filter choices by level; and
- backup required-schema set.

Avoid high-cardinality metric labels when schema/table names are application-controlled. A diagnostic endpoint or command can provide full detail.

## 25. Documentation deliverables

If implementation proceeds, update/add:

- architecture documentation explaining logical versus physical key format;
- catalog definition and compatibility rules;
- schema naming/versioning policy;
- upgrade and reader-before-writer rollout guide;
- rollback guide;
- backup/restore schema requirements;
- benchmark methodology and retained results;
- custom KeySchema implementation notes;
- Pebble patch/upstream link; and
- examples equivalent to the three-table API above.

The documentation must explicitly say that `Set` does not accept a schema and that one Bond DB still means one Pebble DB.

## 26. Done criteria

The initial compact-schema initiative is complete—not necessarily enabled by default—when:

- Bond is pinned to an approved Pebble master-derived revision.
- Existing databases, backups, restores, and migrations pass against it.
- The global fast-compression policy is understood and deliberately configured.
- Filter candidates have isolated benchmark results.
- `bond/full-key/v1` is either rejected with data or fully proven by unit, property, integration, mixed-schema, and lifecycle tests.
- Benchmark results cover representative primary-key shapes and index counts.
- Reader registry construction is centralized.
- Backups/checkpoints declare or conservatively guarantee required schema readers.
- Rollback is demonstrated by selecting an older writer while retaining newer readers.
- Any production activation has reader-before-writer rollout instructions.

The per-table initiative is separately complete when:

- a catalog API and compatibility model are stable;
- the Pebble range-schema extension is upstreamed or sustainably isolated;
- schema routes are validated and coalesced;
- typed family parsers are fuzz/property tested;
- table-boundary SST splitting is correct;
- incremental gains over the global schema justify complexity; and
- file fragmentation/read amplification remain acceptable.

## 27. Open decisions for the future owner

These should be answered with benchmark or API prototypes, not guessed now:

1. Which Pebble commit/release should Bond pin after the reviewed master baseline?
2. Should Bond's default profile prefer balanced or good compression once the global fast override is removed?
3. Which progressive filter policy best fits Bond's negative-lookup and scan workload?
4. Does full-key PrefixBytes provide enough benefit to ship?
5. Which bundle size wins the lifecycle benchmark?
6. Should the catalog be a new top-level API or an optional builder attached to current definitions?
7. Should catalog fingerprints live in reserved Bond keys, backup metadata, or both?
8. Is per-table routing sufficient, making per-index schemas unnecessary?
9. Which schema families justify permanent compatibility support?
10. What maximum forced-boundary/file-count increase is acceptable?
11. Should `bond/full-key/v1` remain the fallback for all opaque/dynamic tables?
12. What migration path fixes ambiguous variable-width logical fields?
13. Which real workload is the acceptance corpus, and who owns regression thresholds?
14. Is the Pebble patch acceptable upstream, or must Bond carry a fork temporarily?

## 28. Recommended order, in one view

```text
measure current Bond
        │
        ▼
upgrade to pinned Pebble master revision
        │
        ▼
fix global fast-compression policy
        │
        ▼
choose filter baseline
        │
        ▼
prototype global BondFullKeyV1 on stock Pebble
        │
        ├── no meaningful gain ──► stop schema work; keep config wins
        │
        ▼
reader registry + backup/tool support
        │
        ▼
canary global writer, retain rollback readers
        │
        ▼
introduce declarative catalog
        │
        ▼
add/upstream SpanPolicy KeySchema selection
        │
        ▼
prototype typed per-table families
        │
        ├── weak incremental gain ──► keep global schema only
        │
        ▼
selective per-index experiment
        │
        ▼
separate logical key-v2 / row-ID / posting-list projects
```

## 29. Final recommendation

The idea behind issue #4380 makes more sense on current Pebble than it did when the issue was opened. Pebble now already supports durable schema names per SST and range-aware output splitting. Bond does not need multiple databases and should not add a schema parameter to `Set`.

The pragmatic path is deliberately incremental:

1. recover easy space first by correcting compression policy;
2. test a global full-key physical schema on unmodified Pebble master;
3. make schema readers and backups operationally safe;
4. add a catalog that describes, rather than executes, table writes;
5. add one generic schema-name field to Pebble's range policy; and
6. use typed schemas per table only where measurements show a real gain.

This order delivers useful checkpoints early, preserves one-store atomicity, avoids logical migrations until justified, and makes every writer change reversible at the configuration level as long as schema readers remain registered.
