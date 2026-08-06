# Phase 3 full-key schema analysis

## Decision

Reject `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64` for production reader and writer rollout. Keep the corrected-legacy Phase 2 schema as Bond's sole production schema.

All three candidates preserve the non-empty logical oracle and are safe to read in controlled mixed-schema databases, but none meets the Phase 2 incumbent-relative activation gate of at least a 5% total-SST reduction. The smallest candidate, b16, saves only 2.71%. The plan's suggested 10% secondary-index target is therefore also not established. The candidates also cannot preserve Bond's existing stored-empty-key contract through pinned Pebble's PrefixBytes implementation. Their immutable implementations and reader registry now live only in `internal/fullkeyexperiment` for focused tests and benchmarks; production options expose neither the readers nor writer selection. No per-range routing or logical-key change was introduced.

## Reproduction

The retained lifecycle comparison used one warmup and three measured repetitions for each candidate:

```sh
cd _benchmarks
go run ./compactkeys/cmd/compact-key-schema \
  --seed=20260806 \
  --rows=20000 \
  --warmups=1 \
  --repetitions=3 \
  --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_3/candidates
```

Every retained manifest records corpus digest `4d0e5e997e9db4506464c418d2b95d60fdd8a08f4aafbc610ba8eeef9be5bd6e`, source fingerprint `373ad00ef5de9585a821d09ee93ae32578646163f280cc2136c15f9e080af298`, Pebble commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`, format 30, corrected-legacy compression, uniform Bloom filters, the controlled benchmark writer, bundle size, and the four sorted benchmark-only readers.

The legacy rerun produced exactly 1,684,118 physical SST bytes in every repetition, matching the frozen Phase 2 corrected-legacy mean exactly. This anchors the candidate deltas directly to the incumbent rather than to a reconstructed or changed baseline.

## Retained lifecycle results

Values are arithmetic means of three 20,000-row repetitions. SST and component sizes had 0% coefficient of variation, so the run can distinguish the 5% activation threshold. CPU and nanosecond latency samples were noisier and are retained per run rather than treated as deterministic.

| Candidate | Physical SST | Change vs incumbent | Data property | Uncompressed index property | Filter property | Database bytes | Compaction CPU | Hit p95 | Point-miss p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy b16 | 1,684,118 | baseline | 1,640,403 | 4,964 | 41,157 | 1,687,842 | 27.693 ms | 1,129 ns | 895 ns |
| full-key b16 | 1,638,399 | -2.71% | 1,594,799 | 4,544 | 41,157 | 1,642,144 | 26.378 ms | 1,112 ns | 1,119 ns |
| full-key b32 | 1,643,370 | -2.42% | 1,599,760 | 4,586 | 41,157 | 1,647,115 | 26.301 ms | 1,152 ns | 978 ns |
| full-key b64 | 1,649,986 | -2.03% | 1,606,386 | 4,562 | 41,157 | 1,653,731 | 26.472 ms | 1,086 ns | 985 ns |

b16 is best on total/data bytes; b32 and b64 do not provide a compensating benefit. Filter bytes are identical, as expected from holding the Phase 2 filter policy fixed. The uncompressed index property falls 7.6-8.5%, but it is too small a component to produce the required total-SST improvement.

Lifecycle CPU and latency samples remain noisy. All candidate compaction CPU means are below the incumbent in this rerun, while point-hit/miss distributions move in both directions. These observations do not override the deterministic failure of the size gate.

## Raw Go benchmark and allocations

`benchmarks/raw-benchmark.txt` retains six `-benchmem` samples per candidate. `benchmarks/benchstat.txt` was produced with `golang.org/x/perf` version `v0.0.0-20260709024250-82a0b07e230d`.

```sh
cd _benchmarks
go test ./compactkeys -run '^$' -bench '^BenchmarkBondLifecycleFullKeySchemas$' -benchmem -count=6
```

Median benchmark wall time is within noise: b16 is 0.8% faster, b32 1.9% faster, and b64 0.6% faster. All full-key candidates allocate about 18.74k allocations/op versus 14.75k for legacy, an increase of roughly 27%; allocated bytes rise 2.6-5.2%. The schema is intentionally correctness-first, and these allocation results are an additional reason not to activate a candidate that already misses the size gate.

## Correctness and operational result

The internal implementation proved exact decoded bytes, bytewise ordering, comparer `Split`, exact/gap/boundary seeks, short and empty seek bounds, duplicate user keys with different internal trailers, forward/reverse and strict-prefix iteration, synthetic prefix/suffix transforms, reset/reuse, independent concurrent seekers, and safe corruption reporting. Stock PrefixBytes rejects a zero-length stored slice. An attempted fixed leading marker preserved ordering but consumed the columnar iterator's mandatory one-byte pointer-arithmetic spare, which fails checkptr validation and cannot be corrected through the KeySchema API because the iterator allocation is based on logical maximum-key length. Pinned Pebble's invariant `DataBlockValidator` also initializes its ordering oracle with an empty sentinel and rejects an empty first decoded key. This is a rejection condition rather than a reason to narrow Bond's API: production database and batch methods retain their existing synchronous empty-key behavior, and the rejected schema is not reachable through production options. Randomized and fuzz properties cover non-empty experimental stored keys plus empty seek targets across all bundle sizes.

The controlled mixed-schema test creates legacy and b32 SSTs with the internal registry, verifies both names are present, reopens, compacts, checkpoints, reopens the checkpoint with a different active writer, and rolls future output back to legacy while retaining all experimental readers. The logical oracle remains identical throughout.

A Bond-level table lifecycle runs with legacy, b16, b32, and b64 writers. It exercises batched primary/secondary-index inserts, point reads, forward index scans, updates that move index entries, flush/reopen, reverse index scans, and deletes without exposing any physical schema argument to mutations.

Range-tombstone coverage runs under controlled legacy, b16, b32, and b64 writers and verifies multiple disjoint/overlapping deletions, point reads, forward/reverse iteration, flush, reopen, and manual compaction against an independent oracle.

An API exposure audit confirms that production `PebbleOptionsConfig`, `BuildPebbleOptionsWithConfig`, and default open paths contain no full-key writer selector or candidate reader registration. The only constructor and configurator are protected by Go's `internal` import boundary, while the retained benchmark harness deliberately imports that package to preserve reproducibility.
