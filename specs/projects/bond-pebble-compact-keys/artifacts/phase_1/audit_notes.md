# Phase 1 Dependency and Pebble API Audit

Captured on 2026-08-05–06 with `go1.26.5 linux/amd64`. Bond retains its
`go 1.25.3` module directive. The pinned Pebble revision also requires Go
1.25.3, and no selected non-main module declares a higher Go version.

## Resolution and upgrade procedure

- The Phase 0 worktree hashes for the incorrect intermediate `go.mod`, `go.sum`, and unchanged `options.go` matched before Phase 1 edits.
- `go-outdated-before.txt` is the complete audit of that intermediate graph.
- Pebble was resolved from the full query `8fb150d9135d6f94e183a874475e0bd1afb18f63`, not from a tag or branch. `pebble-module.json` is the requested `go list -m -json github.com/cockroachdb/pebble@8fb150d9135d6f94e183a874475e0bd1afb18f63` result; it records pseudo-version `v0.0.0-20260707124150-8fb150d9135d`, the selected module directory, and `Origin.Hash` equal to the full approved commit. `pebble-direct-origin.json` independently records the same hash from a direct VCS resolution.
- `go get -u ./...` upgraded the packages in Bond's actual build/test graph. Pebble was repinned immediately afterward, and `go mod tidy` removed requirements that do not provide packages needed by Bond.
- No graph-only overlay is retained. The prior 109-module overlay was removed because it promoted modules reachable only through upstream module metadata, including unused GPL-3.0 `github.com/firefart/nonamedreturns` and MPL-2.0 `github.com/go-sql-driver/mysql`. Neither module is present in `go.mod` or `go.sum`; `go mod why` reports that Bond does not need `nonamedreturns`, and `mysql` is no longer in the selected module graph.
- A final `go mod tidy` also removed the temporary `crypto11 v1.6.7`, `pkcs11 v1.1.2`, and `pool v0.0.2` compatibility-only requirements and sums because no Bond package needs them. `dependency-compatibility-probe.txt` retains the useful proof that v1.6.7 succeeds at the old crypto11 path while v1.6.8 declares `github.com/eclipse-keypont/crypto11`; it is explicitly not a selected-version override.
- `go-outdated-after.txt` contains 64 version suggestions from selected module metadata. `dependency-graph-updates.tsv` records `go mod why -m` for every row: none provides a package needed by the main module, and none is an explicit `go.mod` requirement. This uses Go package/tidy semantics rather than pinning unused modules merely to empty an updater report.
- `dependency-licenses.tsv` is the retained tab-separated output of `go run github.com/google/go-licenses/v2@v2.0.1 report ./... github.com/mfridman/tparse`. Its complete license set is Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, and MIT; the retained package dependency surface contains no GPL, AGPL, LGPL, MPL, EPL, CDDL, CPL, EUPL, OSL, SSPL, or BUSL classification.
- `go mod tidy -go=1.25.3 -diff` is clean. This protects Go 1.25.3–1.25.7 users from an unnecessary toolchain-version ratchet.
- The full and full-race suites run serially, never concurrently, because repository tests share fixed `test_db` paths.
- Compile, focused, full, race, backup, restore, and version checks provide compatibility evidence for the final tidy-derived graph.

## Pebble semantic review

| Area | Phase 1 decision and evidence |
|---|---|
| Filters | Replaced removed `pebble/bloom`, `FilterPolicy`, and `FilterType` APIs with `sstable/tablefilters/bloom` and `LevelOptions.TableFilterPolicy`. Every level retains a 10-bit table Bloom filter. |
| Compression | Retained Snappy for L0/L1 and Zstd for L2-L6. The existing all-keyspace `PreferFastCompression` span override is deliberately preserved for an attributable Phase 1 baseline; Phase 2 owns correcting and measuring it. |
| Value separation | Moved fields out of `Options.Experimental`, retained enabled value blocks/blob separation, 64-byte minimum, reference depth 10, and 60-second rewrite age. Added the now-required positive `MinimumMVCCGarbageSize` at 64 bytes and explicit current 0.10/0.20 garbage-ratio defaults. The span adjustment uses `pebble.ValueStorageLowReadLatency`, preserving both `DisableSeparationBySuffix=true` and `DisableBlobSeparation=true` from the pre-upgrade policy. |
| Iterator and comparer behavior | Kept bytewise ordering and Bond's `Split` result unchanged for well-formed keys. The split helper is now total for truncated and synthetic bounds required by Pebble's invariant comparer. No iterator options or mutation key behavior changed; the full package and race suites exercise forward/reverse/query behavior. |
| Ingestion and restore | Kept the default KeySchema writer and comparer used by ingested tables. Existing logical dump/restore and checkpoint backup/restore suites run against the upgraded engine, and `TestFormatNewestMigration` restores a format-30 checkpoint and compares every logical KV. |
| Cache accounting | Retained `CacheSize` profile values (128 MiB, 256 MiB, and 1 GiB) and Pebble-owned cache construction. No separate cache object or accounting ownership changed. |
| Span policy | Ported the start-key callback to the bounds-based `SpanPolicyFunc`. The returned unconstrained policy remains valid across the complete requested bounds and preserves the former all-keyspace behavior. |
| Open and migration paths | `BuildPebbleOptions` now owns the common correctness configuration for all three default profiles. Normal opens and `MigratePebbleFormatVersion` consume it. Migration now rejects downgrades, verifies Pebble reached the requested ratchet, and synchronously writes the observed version. |
| KeySchema | `EnsureDefaults` installs Pebble's default KeySchema and matching reader map. Phase 1 does not activate a compact writer; tests assert the selected writer is present in the reader map. |
| Storage format | `PebbleDBFormat` is `pebble.FormatNewest`, asserted as numeric 30. `TestFormatNewestMigration` starts from a retained Bond v0.2.17/Pebble-format-26 fixture, verifies it with the historical binary, migrates and reopens it, performs a complete checkpoint backup/restore with exact KV comparison, and proves the historical Pebble engine rejects the restored format-30 database. |

No source or replacement directive points at `/home/peter/Dev/other/pebble`; the external reviewed checkout remains unmodified.

During the initial race run, Pebble's invariant comparer exercised `Split` with a synthetic six-byte upper bound containing an index length of `0xffffffff`. The legacy `_KeyPrefixSplit` trusted that length and panicked. `_KeyPrefixSplit` and the matching internal `_KeyPrefix` helper now treat truncated or synthetic lengths as an opaque full-key prefix. `TestDefaultKeyComparerSplitHandlesSyntheticBounds` covers empty, short, valid, truncated, and maximum-length synthetic bounds. The affected race-focused backup/restore tests and the complete race suite pass with this adaptation.
