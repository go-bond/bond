# Phase 2 verification

Verified on 2026-08-06 with Go 1.26.5 on linux/amd64. Repository test commands were run serially because existing tests share fixed database paths.

## Revision and drift gates

| Gate | Result |
| --- | --- |
| Bond `git rev-parse HEAD` | `72108f117ce3916e72b1e0ad5ee59293d9fb347b` |
| selected Pebble module | `v0.0.0-20260707124150-8fb150d9135d` |
| Pebble module origin | `https://github.com/cockroachdb/pebble`, hash `8fb150d9135d6f94e183a874475e0bd1afb18f63` |
| external Pebble checkout | exact same hash; clean worktree |
| format gate | `PebbleDBFormat == pebble.FormatNewest == 30` |
| schema gate | default writer remains `DefaultKeySchema(leveldb.BytewiseComparator,16)`, bundle size 16 |
| policy gate | `PreferFastCompression=false`; exact `ValueStorageLowReadLatency` retained |

## Checks and tests

| Command | Result |
| --- | --- |
| `gofmt -d` on changed Go sources | pass; no diff |
| `git diff --check` | pass |
| `go vet ./...` | pass |
| `go build ./...` | pass |
| `cd _benchmarks && go vet ./compactkeys/...` | pass |
| `cd _benchmarks && go build ./...` | pass |
| focused option/profile/SST-property tests | pass |
| `cd _benchmarks && go test -p 1 ./compactkeys/... -count=1` | pass |
| `go test -p 1 ./... -count=1` | pass; root 36.431s, backup 345.600s |
| `go test -p 1 -race ./... -count=1` | pass; root 38.734s, backup 349.020s |
| `cd _benchmarks && go test -p 1 -race ./compactkeys/... -count=1` | pass |
| focused Bond version/migration/restore tests | pass |
| focused backup complete/incremental/restore/format-migration tests | pass |
| repeated named benchmark with `-benchmem` | pass; six samples for every candidate retained |
| `benchstat` over raw benchmark output | pass; time, custom metrics, B/op, and allocs/op retained |

The broad command `cd _benchmarks && go vet ./...` also inspects pre-existing generated benchmark test fixtures and fails at `_benchmarks/suites/table_common_test.go:19` because `TokenBalance.MarshalMsg` is absent. This is outside the new harness and is not reached by the repository-root vet command because Go excludes underscore-prefixed directories. The complete `_benchmarks` build passes, and the new `compactkeys` tree passes vet, build, normal tests, race tests, and benchmark execution.

## Retained benchmark evidence

The 20,000-row baseline command completed one warmup plus three retained repetitions for each of five candidates. Evidence consists of 15 JSON manifests and `baselines/summary.csv`. Every manifest records:

- corpus digest `4d0e5e997e9db4506464c418d2b95d60fdd8a08f4aafbc610ba8eeef9be5bd6e`;
- source fingerprint `1af9fc936fb50bfcbb4d993a108846584d85bd0a9a0ebd4f1746e7e47850bd0a`;
- exact Pebble commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`;
- format 30, unchanged logical-key layout, active writer, sorted readers, and actual bundle size 16;
- full effective options plus structured span/value-storage policy;
- post-flush and post-compaction level/metric/SST snapshots and manual-compaction deltas;
- physical SST sizes separately from explicitly labeled table-property byte counters;
- global Pebble `Metrics.Total().WriteAmp()` results;
- oracle-verified point misses and separate prefix-filter diagnostics.

`benchmarks/raw-benchmark.txt` retains six `-benchmem` samples for all five named candidates. `benchmarks/benchstat.txt` records the corresponding summary produced with `golang.org/x/perf` version `v0.0.0-20260709024250-82a0b07e230d`, including allocated bytes and allocations.

The incumbent-relative analysis in `analysis.md` freezes corrected legacy compression plus uniform Bloom. `DBCompressionGood` is size-neutral (+0.28%) and does not meet the 5% benefit gate; neither progressive filter meets that gate.
