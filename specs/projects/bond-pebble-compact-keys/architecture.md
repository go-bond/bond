---
status: complete
---

# Architecture: Bond Pebble Compact Keys

## Authority and Decision Precedence

The root [`PLAN.md`](../../../PLAN.md) supplies the detailed storage analysis, key corpus, schema invariants, benchmark matrix, rollout model, and rejected alternatives. The explicit project overrides take precedence in four places: exact Pebble commit, all-dependency upgrades, permission for breaking Bond APIs, and `pebble.FormatNewest` rather than `FormatV2BlobFiles`.

At the pinned Pebble commit, `FormatNewest` is 30. Using it is a deliberate one-way on-disk ratchet; compatibility is established through fixtures, backups, and restore tests rather than avoided by retaining the old format.

## System Shape

```text
application records
        |
        v
pre-open Catalog -- validates identities/layouts, builds fingerprint
        |
        +--> bound Table/Index handles -- build unchanged logical keys
        |                                  |
        |                                  v
        |                            pebble.Batch.Set
        |
        +--> OpenConfigBuilder
                |- comparer and Split
                |- FormatNewest
                |- compression/filter/value policies
                |- active global KeySchema
                |- durable reader registry
                `- stock range policy
                         |
                         v
                   one pebble.Open
                         |
              WAL/memtables: logical keys
                         |
                  flush/compaction
                         |
              SST: one named physical schema
```

One Pebble instance owns one WAL, LSM, manifest, cache, and recovery path. Schema choice occurs at SST construction, never at record mutation.

## Dependency and Options Baseline

Phase 1 performs dependency work as one reviewable change. It reruns `go-outdated`, updates the graph within existing module paths, tidies, and adapts Bond to Pebble `8fb150d9135d6f94e183a874475e0bd1afb18f63`. The pin is verified with `go list -m -json`; build logs record the resolved pseudo-version and origin hash.

Replace the current three independently assembled Pebble profiles with a shared builder:

```go
type OpenConfig struct {
    Profile      PerformanceProfile
    Catalog      *Catalog
    WriterSchema string // accepted durable schemas only
    Filter       TableFilterProfile
}

func BuildPebbleOptions(cfg OpenConfig) (*pebble.Options, error)
```

Profile-specific sizes and concurrency are applied after common correctness settings. The builder always installs `DefaultKeyComparer()`, `pebble.FormatNewest`, a validated schema registry, and the stock bounds-based span policy. All calls to `pebble.Open`, including migration and tooling, consume this builder or a lower-level validated result returned by it.

`WriterSchema` is a Phase 4 rollout control for schemas that have passed their activation gates. Phase 3's rejected full-key candidates do not add this field to the production `PebbleOptionsConfig`, do not register production readers, and do not alter Bond mutation behavior.

Phase 1 ports moved Bloom/filter packages, stabilized option fields, current `SpanPolicyFunc`, value-storage settings, and validation requirements. Tests assert effective settings so compilation cannot conceal semantic drift.

## Benchmark Harness

Extend `_benchmarks` rather than creating a one-off program. Separate packages/files own deterministic dataset descriptions, loaders, lifecycle execution, queries/oracle, metric capture, and manifests. A candidate configuration is immutable for a run and emits machine-readable results suitable for `benchstat` plus a human summary.

Datasets derive every random choice from a recorded seed. The oracle stores logical records and expected ordered index tuples independently of Pebble. Lifecycle runs use isolated temporary directories and capture pre/post-compaction table properties and `DB.Metrics()`.

Policy experiments are sequenced to preserve attribution:

1. upgraded engine with current semantics;
2. corrected fast-compression behavior and selected compression profile;
3. current versus progressive Bloom versus progressive binary fuse;
4. full-key schema bundle variants against the frozen winner;
5. any typed schema against the accepted global baseline.

## Full-Key Physical Schema

The controlled `bond/full-key/v1-b{16,32,64}` experiment uses a complete-key `PrefixBytes` column. Its KeyWriter computes physical shared prefix across complete keys but returns `KeyComparison.PrefixLen = comparer.Split(key)`. Its KeySeeker reconstructs the exact logical bytes and delegates ordering decisions to bytewise semantics.

The pinned PrefixBytes implementation cannot safely represent a stored empty key, so the candidate fails Bond's existing logical-key contract in addition to missing the size gate. Its implementation therefore lives in `internal/fullkeyexperiment`, with construction and writer selection available only to repository tests and benchmarks. Production options retain only Pebble's legacy schema, and Bond's public empty-key behavior remains unchanged. Tests reuse Pebble-native writer/seeker patterns and preserve the non-empty experimental corpus, empty seek targets, corruption cases, mixed-schema lifecycle evidence, and the exact benchmark results.

For any future accepted schema, the active writer is a validated name rather than an implementation pointer exposed to callers. Selecting a different accepted writer changes only future flush/compaction outputs. Natural compaction may leave mixed schemas indefinitely. No such selection is active for the rejected Phase 3 candidates.

## Durable Registry, Backup, and Tools

The registry is an append-only compatibility map assembled before open:

```go
type SchemaRegistry struct {
    readers map[string]*pebble.KeySchema
}

func NewSchemaRegistry(cmp *pebble.Comparer) (*SchemaRegistry, error)
func (r *SchemaRegistry) SelectWriter(name string) (string, error)
func (r *SchemaRegistry) RequiredNames() []string
```

Names are sorted for stable diagnostics/fingerprints. Unknown names and duplicate names with different implementations are fatal before open.

Backup metadata evolves through a versioned optional field so old backups remain readable:

```go
type StorageCompatibility struct {
    ReaderEpoch       uint32
    FormatMajor       uint64
    RequiredKeySchema []string
}
```

Restore validates compatibility before any destructive destination step. Checkpoints and copied SST backups derive names from table properties where the API exposes them; conservative registry recording is acceptable when it does not. Inspectors share the registry and report schema file counts/bytes.

## Catalog and Bound Handles

The catalog is immutable after validation. Stable storage descriptors are separate from extraction callbacks:

```go
type Catalog struct { /* name, version, tables, compiled fingerprint */ }

type TableSchema[T any] struct {
    Name, LayoutVersion, CodecVersion, KeyDescriptor string
    TableID byte
    Codec Codec[T]
    PrimaryKey KeyExtractor[T]
    PhysicalSchema string
}

type IndexSchema[T any] struct {
    Name, LayoutVersion, KeyDescriptor, OrderDescriptor string
    PredicateVersion string
    IndexID byte
    Unique bool
}

func NewCatalog(name, version string) *Catalog
func DefineTable[T any](c *Catalog, s TableSchema[T]) (*TableDefinition[T], error)
func DefineIndex[T any](t *TableDefinition[T], s IndexSchema[T]) (*IndexDefinition[T], error)
func (c *Catalog) Validate() error
func BindTable[T any](db *DB, d *TableDefinition[T]) (*Table[T], error)
```

Exact generic shapes may adapt to Go's language constraints, but ownership does not: definitions belong to a pre-open catalog, `Open` validates/persists its fingerprint, and bound handles write ordinary logical KV operations. Since breaking API changes are permitted, the implementation may replace current dynamic construction instead of maintaining two parallel models.

Fingerprint input contains only explicit storage-relevant strings, IDs, booleans, and schema names in canonical order. Callback code and process addresses are excluded. A mismatch returns a structured diff identifying fields that changed.

## Stock Pebble Range Feasibility

At the approved commit, stock Pebble supports a single active `Options.KeySchema`, a registry of readers, durable schema names per SST, and output splitting via range-aware `SpanPolicyFunc`. It does not expose a KeySchema field in `SpanPolicy`; therefore registration alone cannot route new SST writers per table.

The feasibility phase first confirms this against the pinned module, then tests typed families globally or in isolated copies. It must not patch `/home/peter/Dev/other/pebble` or introduce an unapproved replace/fork. If stock support is absent, the output is a measured no-go and a bounded upstream proposal for a separate decision. If support has appeared without changing the pin (for example through a misunderstood existing API), routes are compiled by table and adjacent identical policies are coalesced.

Typed parsers operate only on unambiguous outer Bond layout and explicitly described primary-key tails. The existing adjacent variable-width field ambiguity blocks deeper typed decomposition until a versioned logical-key project fixes it.

## Error Handling and Observability

Configuration and catalog errors are returned before open. Schema decode corruption returns Pebble-compatible corruption errors and never silently falls back to another schema. Operational logs identify active writer and reader epoch without dumping keys. Metrics use bounded labels; full table/schema detail belongs in inspection output.

Every retained benchmark and compatibility operation records active writer, registered readers, format version, filter/compression profile, and catalog fingerprint. Writer rollback selects an older registered writer; it never removes newer readers or promises binary downgrade across a format ratchet.

## Verification Strategy

- Unit/property/fuzz tests for writers, seekers, catalog validation, fingerprints, route compilation, metadata compatibility, and option construction.
- Integration tests for primary/index operations, batches, snapshots, prefix/reverse scans, range deletes, ingests, reopen, WAL replay, mixed schemas, migration, backup/restore, and logical dump.
- Full `go test ./...`, `go test -race ./...`, focused version/restore suites, build/vet/format checks, and reproducible benchmark comparisons.
- Drift gates recheck the exact Bond/Pebble commits and stock capabilities at every phase that depends on them.

Detailed internal contracts are split into component designs because schema correctness, benchmark attribution, and catalog/compatibility each warrant independent review.
