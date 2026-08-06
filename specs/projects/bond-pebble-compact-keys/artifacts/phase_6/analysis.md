# Phase 6 typed/per-range feasibility decision

Decision: **no production typed schema and no per-range routing on stock pinned Pebble**.

## Stock capability evidence

Resolved module:

- version: `github.com/cockroachdb/pebble v0.0.0-20260707124150-8fb150d9135d`
- origin hash: `8fb150d9135d6f94e183a874475e0bd1afb18f63`
- origin URL: `https://github.com/cockroachdb/pebble`

Anchors in that immutable module source:

- `options.go:934-949`: `Options.KeySchema` names the schema used for all newly written SSTs; `Options.KeySchemas` is the reader registry.
- `options.go:2832-2850`: `Options.MakeWriterOptions` resolves `o.KeySchema` directly into `writerOpts.KeySchema` and receives no span policy.
- `internal/base/span_policy.go:18-67`: `SpanPolicy` contains key range, fast-compression, value-storage, and tiering policy only. It has no schema selector.
- `sstable/colblk_writer.go:185`: the selected writer records its one schema name in table properties.

`TestStockPebbleRangeSchemaCapability` checks the missing selector and global writer resolution. Range policy can cause an SST boundary, but both sides still use the global `Options.KeySchema`. Reader registration cannot change this.

No `go.mod`/`go.sum` change or `replace` was made. `/home/peter/Dev/other/pebble` remained at the approved hash and was not modified.

## Typed experiment scope

The internal-only experiment implements `bond/pk-u64/v1`, `bond/pk-u32/v1`, and `bond/pk-bytes/v1` at immutable bundle size 16. It decomposes only the final one-field PK tail of a secondary key after validating the unambiguous outer `OrderLen | Order` framing. The exact logical prefix is retained in PrefixBytes; primary/prefix/incompatible/truncated forms retain an opaque exact suffix. Empty or arbitrary short seek/range bounds require no parsing and compare against reconstructed bytewise keys.

Adjacent variable-width KeyBuilder fields remain opaque because the current logical format cannot parse them unambiguously. Empty point keys are outside a routed table range and remain unsupported by the typed PrefixBytes column, preventing global production activation independently of measurements.

## Retained measurement

Command:

```text
cd _benchmarks
go run ./compactkeys/cmd/typed-key-schema \
  --seed=20260806 \
  --rows=2000 \
  --warmups=0 \
  --repetitions=3 \
  --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_6/typed-candidates
```

Common settings: three secondary indexes, 70% partial-index inclusion, mixed fixed/variable order fields, mixed index cardinality/prefix locality, 160-byte values, frozen legacy compression, uniform Bloom, format 30, and bundle size 16. Each incumbent/candidate pair has an identical dataset digest. Every retained lifecycle passed the independent logical oracle, checkpoint open, and reopen.

| Dataset | Legacy SST bytes | Typed SST bytes | Delta | Legacy/typed median hit p95 ns | Legacy/typed median miss p95 ns |
|---|---:|---:|---:|---:|---:|
| sequential u64 | 137,256 | 131,839 | -3.95% | 852 / 1,152 | 881 / 1,042 |
| random u64 | 152,045 | 156,607 | +3.00% | 1,212 / 1,092 | 842 / 1,002 |
| sequential u32 | 137,296 | 132,448 | -3.53% | 882 / 1,093 | 961 / 1,002 |
| random u32 | 154,174 | 154,078 | -0.06% | 832 / 1,142 | 862 / 1,463 |
| bytes-32 | 295,846 | 304,690 | +2.99% | 882 / 1,082 | 861 / 1,022 |

SST sizes were byte-identical across the three repetitions for each candidate, so the result can distinguish the target 5% effect. No candidate reached it; two grew. Point and compaction timings were noisier, but most point-latency medians regressed and random-u32's miss median increased 69.7%.

A focused one-shot `-benchmem` lifecycle run found no meaningful allocation regression:

| Dataset | Legacy allocs/op | Typed allocs/op |
|---|---:|---:|
| sequential u64 | 15,196 | 15,040 |
| random u64 | 14,945 | 14,980 |
| sequential u32 | 14,907 | 14,970 |
| random u32 | 14,883 | 14,964 |
| bytes-32 | 14,899 | 14,965 |

## Gate outcome

- Correct typed behavior on isolated stock-Pebble copies: **proven**.
- Meaningful 5%+ size win within accepted latency/CPU budget: **failed**.
- Stock per-table/per-range writer selection: **absent**.
- Production reader/writer registration: **rejected and unchanged**.
- Per-index experiment: **not authorized**, because the prerequisite per-table route is absent and the measured families do not justify additional boundaries.

The bounded future proposal is documented in `docs/07-typed-schema-feasibility.md`. It is a separate upstream/fork decision and is not implemented by Phase 6.
