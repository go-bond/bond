---
status: complete
---

# Component: Dependency Baseline and Benchmark Harness

## Purpose and Scope

Own the reproducible engine baseline, exact dependency resolution, effective Pebble profiles, deterministic data generation, lifecycle runner, logical oracle, and result manifests. It does not implement KeySchema encoding or catalog APIs.

## Dependency Procedure

1. Preserve a pre-change status/diff and Phase 0 hashes so user-owned module edits are not mistaken for approved state.
2. Run `go-outdated` and retain the complete report.
3. Resolve Pebble from exact commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`; never retain `v1.1.5` or substitute a moving branch/tag.
4. Upgrade all other modules to latest compatible versions within their existing module paths, including transitive requirements; run `go mod tidy`.
5. Verify with `go list -m -json github.com/cockroachdb/pebble`, record the pseudo-version plus origin hash, and fail unless the hash is exact.
6. Rerun `go-outdated`; record every justified holdback. Pebble's exact pin is expected.
7. Set `PebbleDBFormat = pebble.FormatNewest` and assert the pinned revision's value (30) in compatibility tests.
8. Adapt APIs and run build, vet, full, race, reopen, format migration, backup, and restore suites before policy experiments.

## Harness Contracts

Suggested internal interfaces:

```go
type DatasetSpec struct { Seed int64; Rows, Indexes int; KeyShape, OrderShape string }
type EngineSpec struct { Name, WriterSchema, Filter, Compression string; BundleSize int }
type RunManifest struct { Revisions, Runtime, Machine, Options, Dataset, Results any }

func Generate(spec DatasetSpec) Dataset
func Load(ctx context.Context, db *bond.DB, data Dataset) error
func Verify(ctx context.Context, db *bond.DB, oracle Oracle) error
func RunLifecycle(ctx context.Context, engine EngineSpec, data Dataset) (RunManifest, error)
```

All paths are supplied by the test runner and live below an isolated temporary root. Large retained results are opt-in artifacts; unit tests use bounded fixtures.

## Metrics and Gates

Collect SST component bytes, logical counts, files by level, L0 overlap, WAL/memtable/cache metrics, write amplification, compaction I/O/CPU/time, latency percentiles, allocations, false-positive observations, reopen, and restore duration. Results compare multiple repeated runs with the same seed.

Compression is selected before filters; filters before schemas. Reject or retain as opt-in any default that wins space but exceeds the recorded latency/CPU budget. Stop schema conclusions when size variance exceeds the smallest meaningful 5–10% effect.

## Test Plan

- `TestDatasetGeneratorDeterministic`: identical spec produces identical logical records and index order.
- `TestDatasetGeneratorCoversShapes`: each required key/order/index shape has a bounded fixture.
- `TestOracleDetectsRecordAndOrderDrift`: mutated record and index order both fail comparison.
- `TestManifestRoundTrip`: all reproducibility fields survive machine-readable serialization.
- `TestBuildPebbleOptionsProfiles`: common correctness settings and profile differences are effective.
- `TestPebbleModuleOrigin`: resolved module origin hash is the approved commit.
- `TestFormatNewestMigration`: old fixture ratchets to pinned `FormatNewest`, reopens, backs up, and restores.
- `BenchmarkBondLifecycle*`: named candidates produce comparable metrics and manifests.
