# Phase 2 baseline analysis

## Decision

Freeze the Phase 2 non-schema baseline as:

- compression: Bond's corrected legacy profile (Snappy at L0/L1 and ZSTD at L2-L6), without the former span-wide fast-compression override;
- table filters: Pebble `DBTableFilterPolicyUniform` (10-bit Bloom at every level);
- value storage: exact `ValueStorageLowReadLatency` span policy;
- format/schema: `FormatNewest` = 30 with `DefaultKeySchema(leveldb.BytewiseComparator,16)` and bundle size 16.

`DBCompressionBalanced`, `DBCompressionGood`, progressive Bloom, and progressive binary fuse remain explicit opt-in experiment profiles. The default is not changed unless an alternative produces at least a 5% total-SST reduction against the actual incumbent while keeping compaction CPU, hit p95, and point-miss p95 regressions within 10%. A neutral result preserves the last proven configuration.

## Reproduction

The retained selection run used one warmup and three measured repetitions for each candidate:

```sh
cd _benchmarks
go run ./compactkeys/cmd/compact-key-baseline \
  --seed=20260806 \
  --rows=20000 \
  --warmups=1 \
  --repetitions=3 \
  --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_2/baselines
```

All 15 manifests contain corpus digest `4d0e5e997e9db4506464c418d2b95d60fdd8a08f4aafbc610ba8eeef9be5bd6e` and source fingerprint `1af9fc936fb50bfcbb4d993a108846584d85bd0a9a0ebd4f1746e7e47850bd0a`. Each records the exact Bond/Pebble revisions, Go/runtime and host/storage context, full effective Pebble options, structured span/value policy, active writer, sorted registered readers, actual bundle size, format, dataset, post-flush/post-compaction storage snapshots, compaction deltas, SST properties, point hits/misses, and separate prefix-filter observations. The machine-readable aggregate is `baselines/summary.csv`.

The deterministic representative corpus contains 20,000 logical rows and three secondary indexes. Its mixed fixture covers sequential/random integer keys, 20/32/64-byte keys, UUIDs, addresses, two/three-field composites, absent/fixed/variable-length/mixed order shapes, common/random prefixes, low/high/mixed cardinality, and partial indexes. The oracle verifies exact point misses with `Get`/`pebble.ErrNotFound` in the initial, mutated, snapshot, compacted, checkpoint, and reopened states. Prefix-seek probes remain separate and are used only for L6 filter diagnostics.

## Retained selection results

Values below are arithmetic means of three retained 20,000-row repetitions. Latencies are p95 nanoseconds. Compression candidates all use uniform Bloom. Filter candidates all use corrected legacy compression.

| Candidate | Physical SST | Filter property | Observed FP rate | Compaction wall | Compaction CPU | Hit p95 | Point-miss p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy compression + uniform Bloom | 1,684,118 | 41,157 | 1.514% | 28.555 ms | 31.643 ms | 1,336 ns | 945 ns |
| balanced compression + uniform Bloom | 1,942,412 | 41,157 | 1.514% | 22.898 ms | 24.899 ms | 1,389 ns | 1,119 ns |
| good compression + uniform Bloom | 1,688,908 | 41,157 | 1.514% | 24.031 ms | 26.119 ms | 1,219 ns | 902 ns |
| legacy compression + progressive Bloom | 1,675,926 | 32,965 | 2.295% | 25.158 ms | 26.909 ms | 1,142 ns | 2,314 ns |
| legacy compression + progressive binary fuse | 1,662,920 | 19,982 | 5.957% | 27.542 ms | 29.676 ms | 1,362 ns | 915 ns |

`DBCompressionBalanced` increases physical SST bytes by 15.3% versus the incumbent, so it is rejected. `DBCompressionGood` is size-neutral (+0.28%). Although its CPU and sampled p95 means were lower in this rerun, it does not meet the required 5% size improvement and therefore does not justify replacing the proven legacy default. This incumbent-relative decision also avoids relying on noisy host latency/CPU measurements to change Phase 3's baseline.

With compression fixed to corrected legacy, progressive Bloom reduces the complete SST by only 0.49%; its filter property is 19.9% smaller, its observed false-positive rate is 51.6% higher, and one point-miss run was a large outlier. Progressive binary fuse reduces the complete SST by only 1.26%; its filter property is 51.5% smaller, but its observed false-positive rate is 3.94 times uniform Bloom. Neither meets the 5% total-SST gate, so uniform Bloom remains the default.

The retained write-amplification values use Pebble's global `Metrics.Total().WriteAmp()` semantics. SST `physical_bytes` comes from physical table sizes. Data, filter, value-block, and uncompressed-index values are explicitly labeled table properties and are not subtracted from physical bytes or presented as a reconciling layout.

## Raw benchmark and allocations

`benchmarks/raw-benchmark.txt` retains six samples per candidate from the named 1,000-row Go benchmark with `-benchmem`. `benchmarks/benchstat.txt` was produced with `golang.org/x/perf` version `v0.0.0-20260709024250-82a0b07e230d` and summarizes time, custom SST/filter/CPU metrics, bytes allocated, and allocations. Median allocations range from 14.68k to 14.72k allocations/op. Median allocated bytes are 5.208-5.366 MiB/op for the legacy/Balanced/Good/progressive-Bloom profiles and 8.105 MiB/op for progressive binary fuse.

The raw Go benchmark is bounded allocation/variance evidence, not the configuration-selection dataset. Candidate selection uses the 20,000-row manifests above, where the SST-byte coefficient of variation is 0% except Balanced at 0.77%. This comfortably distinguishes the project's smallest meaningful 5% size effect. CPU and nanosecond latencies are preserved per run and are not treated as deterministic.

Phase 3 must use corrected legacy compression, uniform Bloom, the exact pinned Pebble revision, format 30, bundle size 16, and this corpus digest when attributing changes to a KeySchema candidate.
