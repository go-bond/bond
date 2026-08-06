---
status: complete
---

# Phase 1: Establish the Pebble and Dependency Baseline

## Overview

This phase replaces the incorrect intermediate Pebble `v1.1.5` selection with the exact reviewed commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`, upgrades the rest of Bond's module graph within existing module paths, and ports Bond's storage options to the pinned Pebble APIs. It also deliberately ratchets new and migrated databases to `pebble.FormatNewest` (numeric format 30 at the approved commit) while retaining the legacy/default KeySchema writer. Retained dependency evidence and lifecycle tests make the resulting build baseline reproducible before policy or compact-key experiments begin.

## Steps

1. Recheck the Phase 0 Bond/Pebble drift gate and retain `go-outdated` output from the incorrect pre-upgrade module graph in `specs/projects/bond-pebble-compact-keys/artifacts/phase_1/`.
2. Resolve `github.com/cockroachdb/pebble` from commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`; retain `go list -m -json github.com/cockroachdb/pebble` output and verify that `Origin.Hash` and the resolved module directory identify that exact commit rather than a tag or moving branch.
3. Upgrade every package dependency in Bond's actual build/test graph with `go get -u ./...`, repin Pebble after automated upgrades, run `go mod tidy`, and retain a post-upgrade `go-outdated` report plus `go mod why` evidence distinguishing unused module-metadata candidates from required packages. Probe `crypto11`'s latest compatible old-path release without retaining it when no Bond package needs it, and record compatibility and license classifications.
4. Refactor `options.go` so all performance profiles are constructed from one common correctness baseline while preserving their documented cache, memtable, file-size, concurrency, and compression differences.
5. Port Bloom configuration from legacy `FilterPolicy`/`FilterType` fields to `TableFilterPolicy`, convert `spanPolicyFunc` to `func(pebble.UserKeyBounds) (pebble.SpanPolicy, error)`, and add the positive `MinimumMVCCGarbageSize` required by the pinned value-separation validation.
6. Set `PebbleDBFormat = pebble.FormatNewest`, keep Pebble's default KeySchema active, and ensure normal opens and `MigratePebbleFormatVersion` use equivalent comparer/filter/compression/value-separation/span-policy settings.
7. Add focused tests that lock down the effective common/profile options, the exact module origin, `FormatNewest == 30`, older-format migration and ratchet behavior, reopen behavior, checkpoint restore behavior, and rejection when an older format configuration attempts to open a ratcheted Bond database.
8. Format and vet the code, run `make build`, the focused version/reopen/backup/restore suites, then run the full and full-race suites serially because repository tests share fixed `test_db` paths; retain the final module verification and audit evidence.

## Tests

- `TestBuildPebbleOptionsProfiles`: verifies common comparer, format, default KeySchema, filters, compression, value separation, span policy, and the intended differences among low/medium/high profiles.
- `TestPebbleModuleOrigin`: verifies `go list -m -json` resolves Pebble to the approved full VCS hash and a populated module directory.
- `TestFormatNewestMigration`: extracts a retained database created by Bond v0.2.17/Pebble format 26, proves the historical binary can read it, migrates it to format 30, verifies exact data after reopen and checkpoint restore, and proves the historical Pebble engine rejects the ratcheted database.
- `TestDefaultKeyComparerSplitHandlesSyntheticBounds`: verifies the pinned Pebble invariant comparer can safely split empty, short, truncated, and synthetic upper bounds without changing well-formed Bond key splits.
- Existing `TestBond_Open`, `TestBond_VersionCheck`, and `Test_BondVersionMigrate`: protect normal open and explicit migration behavior.
- Existing root, `tests`, and `backup` restore suites: protect logical dump/restore and complete/incremental checkpoint backup behavior against the upgraded module graph.
