# Phase 3 verification

Verified on 2026-08-06 with Go 1.26.5 on linux/amd64. Repository tests were run serially because existing suites share fixed `test_db` paths.

## Revision and policy gates

| Gate | Result |
| --- | --- |
| selected Pebble module | `v0.0.0-20260707124150-8fb150d9135d` |
| selected/external Pebble hash | exact `8fb150d9135d6f94e183a874475e0bd1afb18f63`; external checkout clean |
| fork/replace gate | no Pebble `replace`, workspace override, or source patch |
| format gate | `PebbleDBFormat == pebble.FormatNewest == 30` |
| policy gate | corrected legacy compression, uniform Bloom, exact low-read-latency span value policy |
| reader gate | production options register legacy only; b16/b32/b64 readers exist only in the internal test/benchmark registry |
| writer gate | production options expose no writer selection; controlled internal configuration rejects unknown experimental names |
| activation gate | rejected: best candidate b16 saves 2.71%, below required 5%, and the candidate cannot preserve stored empty keys |

## Checks and tests

| Command | Result |
| --- | --- |
| `gofmt -d` on changed Go sources | pass; no diff |
| `git diff --check` | pass |
| `go vet ./...` | pass |
| `go build ./...` | pass |
| `cd _benchmarks && go vet ./compactkeys/...` | pass |
| `cd _benchmarks && go build ./...` | pass |
| focused internal full-key writer/seeker/registry/mixed-schema tests | pass; focused race 2.853s |
| focused Bond table lifecycle under controlled legacy/b16/b32/b64, normal and race | pass |
| `cd _benchmarks && go test -p 1 ./compactkeys/... -count=1` | pass; compactkeys 0.667s |
| `go test -p 1 ./... -count=1` | pass; root 39.078s, backup 346.215s, total 415.40s |
| `go test -p 1 -race ./... -count=1` | pass; root 40.271s, backup 348.814s, total 445.69s |
| `cd _benchmarks && go test -p 1 -race ./compactkeys/... -count=1` | pass; compactkeys 3.177s |
| active `FuzzFullKeySchemaRoundTripAndSeek`, 10 seconds | pass; 293,147 executions |
| focused Bond version/migration/query/index/backup tests | pass |
| focused cross-package backup/restore tests | pass |
| focused format-30 complete/incremental restore tests | pass |
| retained 20,000-row lifecycle comparison | pass; 12 manifests plus `summary.csv` |
| six-sample named full-key benchmark with `-benchmem` | pass; raw output and `benchstat` retained |

## Correctness coverage

- Writer comparison proves comparer-derived logical prefix length, logical-prefix common length, complete-key physical encoding, duplicate comparison, materialization, and reset.
- Seeker tests prove exact/gap/before/after lower bounds, bytewise order, logical prefix equality, short and empty bounds/targets, forward/reverse traversal, synthetic prefix/suffix transforms, and independent concurrent use.
- Randomized and fuzz properties prove decoded bytes, `Comparer.Split`, ordering, empty-target handling, and lower-bound seeks across b16/b32/b64.
- Production Bond database and batch point mutations retain their pre-experiment synchronous empty-key behavior. The incompatible full-key schemas are absent from production options, while empty experimental seek/iterator bounds remain supported.
- Block-level truncation and PrefixBytes-offset corruption return errors through `colblk.InitDataBlockMetadata`; a checksum-valid SST with a corrupted custom data column opens structurally and fails safely when its data-block metadata is initialized.
- Range tombstones under controlled legacy/b16/b32/b64 writers preserve the point-and-iteration oracle through flush, reopen, manual compaction, and a second reopen.
- The bounded Bond lifecycle oracle covers load, updates, deletes, snapshots, scans, compaction, point hits/misses, checkpoint restore, and reopen under every candidate.
- A high-level Bond table/index lifecycle passes under legacy and every full-key writer, including batch insert, index-changing update, reverse scan, reopen, and delete.
- Controlled mixed legacy/b32 SSTs preserve all keys through reopen, compaction, checkpoint open, and writer rollback while the internal readers remain registered.
- Production options expose only Pebble's legacy schema; the rejected constructor/configurator are protected by the repository `internal` boundary and imported only by tests and `_benchmarks`.

## Retained evidence

The Phase 3 candidate manifests use the exact Phase 2 corpus digest and reproduce its incumbent physical SST size exactly. Each manifest records the exact module origin, source fingerprint, machine/runtime context, full effective options, active writer and bundle, sorted reader registry, storage format/policies, file and byte counts by schema, SST components, global write amplification, compaction timing/CPU, latency distributions, and recovery/checkpoint timing.
