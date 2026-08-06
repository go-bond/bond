# Typed KeySchema feasibility on stock Pebble

Phase 6 evaluated typed primary-key-tail schemas against Bond's exact pinned Pebble commit, `8fb150d9135d6f94e183a874475e0bd1afb18f63`. The production decision is a no-go for per-table schema routing on this stock API. Bond continues to use one Pebble database, one global legacy writer, and one production reader.

## What stock Pebble supports

Pebble stores a durable KeySchema name in each columnar SST and can read mixed-schema SSTs when every name is registered. `Options.KeySchema` selects the schema copied into writer options for every new flush or compaction output. `SpanPolicyFunc` can return range boundaries plus compression, value-storage, and tiering policy.

`SpanPolicy` has no KeySchema or schema-name field. Its range boundary may split output SSTs, but it cannot change the KeySchema assigned to either output. Registering multiple readers does not route writers. `TestStockPebbleRangeSchemaCapability` makes this API conclusion executable against the pin.

Consequently, Bond does not:

- claim that the Phase 5 catalog's physical-family descriptor routes SST writers;
- register or select a typed family in production options;
- add a physical schema argument to `Set`, `Delete`, or batch mutations;
- add a local Pebble `replace`, fork, or checkout modification; or
- split the single Bond/Pebble database into per-table databases.

## Isolated typed experiments

The internal `typedschemaexperiment` package implements these immutable experimental names:

- `bond/pk-u64/v1`
- `bond/pk-u32/v1`
- `bond/pk-bytes/v1`

The parser uses only Bond's unambiguous outer key framing. It retains the exact comparer-defined prefix column. For a secondary key, it validates `OrderLen | Order | PrimaryKey`, then separates a one-field primary-key tail into a uint or byte column. Primary rows, prefix forms, malformed/truncated suffixes, and primary keys that do not match the family use an exact opaque suffix representation. Arbitrary short seek and range bounds are compared against reconstructed bytewise keys rather than parsed as point keys.

This scope deliberately stops before adjacent variable-width inner fields. Bond's current `KeyBuilder` does not delimit those fields, so decomposing them would guess at an ambiguous logical layout. Fixing that requires a separate versioned logical-key migration.

The schemas are available only to repository tests and the isolated lifecycle benchmark. Empty point keys are outside any routed Bond table range and cannot be accepted by the PrefixBytes prefix column, which is another reason these schemas are not safe global production writers.

## Measured result

The retained Phase 6 run used five deterministic 2,000-row datasets, three secondary indexes, the frozen compression/filter policy, format 30, one warmup-free measurement set of three retained repetitions per incumbent/candidate, and an independent lifecycle oracle through load, mutation, snapshot, flush, compaction, point queries, checkpoint, reopen, and ordered scans.

| Dataset | Typed family | SST byte change | Median hit p95 change | Median miss p95 change |
|---|---|---:|---:|---:|
| sequential u64 | `pk-u64` | -3.95% | +35.2% | +18.3% |
| random u64 | `pk-u64` | +3.00% | -9.9% | +19.0% |
| sequential u32 | `pk-u32` | -3.53% | +23.9% | +4.3% |
| random u32 | `pk-u32` | -0.06% | +37.3% | +69.7% |
| 32-byte keys | `pk-bytes` | +2.99% | +22.7% | +18.7% |

Physical sizes were identical across all three repetitions for a candidate. No family reached the project's meaningful 5% size threshold, two candidates grew, and most point-latency medians regressed (the short timing runs are noisier than the byte measurements). A focused `-benchmem` run found no typed allocation regression. The result is therefore a measured rejection even for isolated activation, not merely a routing limitation. The implementation remains useful correctness/proposal evidence but is not an accepted production reader or writer.

The complete machine-readable manifests and CSV are under `specs/projects/bond-pebble-compact-keys/artifacts/phase_6/typed-candidates`.

## Separately scoped upstream proposal

A future Pebble project may propose a generic optional schema name on `SpanPolicy`. It should be considered only through a separate approval and should include at least:

1. Add an optional field such as `KeySchemaName string` to `SpanPolicy`, including default, equality, string, and validation behavior.
2. Resolve a non-empty policy name through the immutable `Options.KeySchemas` registry when each flush or compaction output writer is created. An unknown name must fail before writing the output.
3. Keep `Options.KeySchema` as the default when the range policy omits a name.
4. End outputs exactly at policy boundaries before changing schemas; prove flush, compaction, manual compaction, mixed-schema input, range-key, ingestion/rewrite, and restart behavior.
5. Run Pebble invariants, race, metamorphic, and stress suites in the Pebble project.
6. Only after upstream support exists, compile catalog routes per table, coalesce adjacent identical complete policies, and measure forced boundaries, file counts, L0 overlap, read amplification, and compaction effects before considering any per-index route.

This Bond project does not implement that extension, patch another checkout, or promise that the proposal will be accepted upstream.
