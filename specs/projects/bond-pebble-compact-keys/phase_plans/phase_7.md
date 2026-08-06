---
status: complete
---

# Phase 7: Final Validation and Decision Audit

## Overview

This phase closes the compact-key project with one deliberate final validation sequence. It re-establishes dependency, exact-Pebble-origin, and `FormatNewest` provenance; runs the exact build, full, race, focused compatibility, benchmark, example, vet, formatting, and diff gates that earlier phases deferred; audits every production open path and required document; and maps the implementation and retained decisions back to `PLAN.md`, the completed project specifications, and the user's objective. Genuine failures are fixed and only their affected checks are repeated. The phase retains evidence and final accepted/rejected decisions without changing or replacing the separate Pebble checkout.

## Steps

1. Record the Bond worktree/commit, Go/runtime/host details, module hashes, external Pebble checkout state, absence of a Pebble `replace`, and relevant source fingerprints. Re-run `go-outdated`, the package-graph latest-compatible upgrade/tidy probe, exact commit resolution, selected-module origin verification, and a tidy diff; preserve the result under `artifacts/phase_7`.
2. Verify that the selected pinned module defines `pebble.FormatNewest` as numeric format 30 and that production options, migrations, compatibility metadata, tests, and documentation consistently use that ratchet.
3. Run the exact repository `make build`, followed serially by `go test -p 1 ./... -count=1` and `go test -p 1 -race ./... -count=1`, because repository tests share fixed database paths.
4. Run one focused version/reopen/format-migration/backup/checkpoint/restore/rollback/old-metadata/old-format matrix across the root, `tests`, `backup`, CLI inspection, full-key mixed-schema, and storage-compatibility tests.
5. Re-run the retained compact-key baseline, full-key, and typed-schema measurements with their recorded seeds, row counts, warmups, repetitions, and fixed policies into temporary output directories. Compare dataset digests, manifests, repeated SST sizes, lifecycle oracle results, source fingerprints, and decision gates against the retained Phase 2/3/6 artifacts; run the focused lifecycle `-benchmem` benchmarks once to detect material regressions.
6. Compile and run the catalog example, then run repository and applicable nested-module vet/build checks, `gofmt -d` across tracked Go files, `go mod tidy -go=1.25.3 -diff`, and `git diff --check`.
7. Audit all production `pebble.Open`/checkpoint/restore/migration/inspection/benchmark paths for centralized registry/options use; audit public documentation for the one-Bond-DB/one-Pebble-DB model, schema-free mutation API, immutable reader/writer rollout rules, format ratchet, backup/restore compatibility, catalog migration, rejected full-key decision, and stock routing no-go.
8. Publish Phase 7 provenance, verification, benchmark-reproducibility/regression results, final accepted/rejected decisions, and a requirement-by-requirement completion audit. Confirm `/home/peter/Dev/other/pebble` remains unmodified at the approved commit and the Bond module has no local replacement.

## Tests

- Dependency/origin drift gate: current package dependencies are latest compatible or explicitly justified; Pebble resolves to full hash `8fb150d9135d6f94e183a874475e0bd1afb18f63` from the CockroachDB origin with no `replace`.
- `TestPebbleModuleOrigin`, `TestBuildPebbleOptionsProfiles`, and `TestFormatNewestMigration`: exact pin, format 30, old fixture ratchet, reopen, checkpoint restore, and historical-engine rejection remain proven.
- Full repository suite: all packages pass once serially without race instrumentation.
- Full repository race suite: all packages pass once serially under the race detector.
- Focused compatibility matrix: version/open, complete and incremental backup, checkpoint metadata, pre-mutation restore rejection, old metadata, old format, inspection, and mixed-schema writer rollback all pass.
- Benchmark reproducibility: repeated baseline/full-key/typed runs reproduce logical dataset digests and stable size conclusions; lifecycle oracles pass and no accepted production policy has an unexplained regression.
- Example/tooling checks: the three-table catalog example compiles and runs; root and compact-key vet/build checks, formatting, tidy diff, and diff whitespace checks pass.
- Open-path/documentation/completion audit: all production opens are centralized and every functional, architecture, component, plan, and user requirement has implementation or retained no-go evidence.
