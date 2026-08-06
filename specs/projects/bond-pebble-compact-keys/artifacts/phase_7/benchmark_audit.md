# Phase 7 benchmark reproducibility and regression audit

Phase 7 reran all retained compact-key decisions into the temporary output root
`/tmp/bond-phase7-benchmarks.371IaJ`. That directory is intentionally left
untouched for immediate review; the versioned Phase 2, 3, and 6 manifests remain
the durable project evidence.

## Reproduction commands

```sh
cd _benchmarks
go run ./compactkeys/cmd/compact-key-baseline \
  --seed=20260806 --rows=20000 --warmups=1 --repetitions=3 \
  --output=/tmp/bond-phase7-benchmarks.371IaJ/phase2
go run ./compactkeys/cmd/compact-key-schema \
  --seed=20260806 --rows=20000 --warmups=1 --repetitions=3 \
  --output=/tmp/bond-phase7-benchmarks.371IaJ/phase3
go run ./compactkeys/cmd/typed-key-schema \
  --seed=20260806 --rows=2000 --warmups=0 --repetitions=3 \
  --output=/tmp/bond-phase7-benchmarks.371IaJ/phase6
```

The commands emitted 15 baseline, 12 full-key, and 30 typed manifests. Every
lifecycle oracle passed load/mutation, exact point and ordered scans, snapshot,
flush, compaction, reopen, and checkpoint-open validation. Each manifest records
the exact Pebble origin hash, current dirty Bond revision and source fingerprint,
runtime, host/storage context, dataset parameters and digest, complete effective
options, format, schema writer/readers/bundle size, warmups/repetitions, SST
properties, metrics, and latency distributions.

## Deterministic comparison

The baseline and full-key corpus digest reproduced exactly as
`4d0e5e997e9db4506464c418d2b95d60fdd8a08f4aafbc610ba8eeef9be5bd6e`.
Legacy, good-compression, and both filter-policy SST sizes were byte-identical
across repetitions and consistent with the retained decision. The balanced
compression result retained its already-known size variance and still lost
decisively. No production policy changed.

The full-key candidates reproduced every retained SST size byte-for-byte in all
three repetitions:

| Candidate | SST bytes | Result |
| --- | ---: | --- |
| legacy b16 | 1,684,118 | incumbent |
| full-key b16 | 1,638,399 | -2.71%; below 5% gate |
| full-key b32 | 1,643,370 | -2.42%; below gate |
| full-key b64 | 1,649,986 | -2.03%; below gate |

The typed run reproduced each pair's retained dataset digest and every SST size
byte-for-byte in all three repetitions:

| Dataset | Legacy SST | Typed SST | Gate result |
| --- | ---: | ---: | --- |
| sequential u64 | 137,256 | 131,839 | -3.95%; below 5% |
| random u64 | 152,045 | 156,607 | +3.00%; reject |
| sequential u32 | 137,296 | 132,448 | -3.53%; below 5% |
| random u32 | 154,174 | 154,078 | -0.06%; neutral |
| bytes-32 | 295,846 | 304,690 | +2.99%; reject |

Source fingerprints differ from older retained phases because the current
implementation and module graph are intentionally newer; all three Phase 7
reruns agree on fingerprint
`eb3d70919937e7a7716175a1fe1e38449c36cdc32ea254696b6c4d306cf646a9`.
Dataset identity, effective policies, origin pin, and physical conclusions are
unchanged.

## Allocation/regression probe

The harness-focused tests passed in 1.410s. One bounded combined benchmark run
of baseline, full-key, and typed lifecycle cases with `-benchmem -benchtime=1x
-count=1` passed in 3.125s. Legacy and typed cases remained near 14.9k
allocations per operation. Full-key cases remained near 18.9k versus roughly
15.0k for legacy, the already-recorded approximately 27% penalty that reinforces
their rejection. No accepted production policy showed an unexplained size,
correctness, or allocation regression.
