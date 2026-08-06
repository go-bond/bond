---
status: complete
---

# Phase 0: Materialize Specs and Freeze Repository Baselines

## Overview

This phase creates the active spec project and records enough repository/environment state to distinguish approved work from pre-existing user changes. It intentionally performs only documentation and state work safe before the dependency upgrade. The benchmark fixtures and oracle described by the root plan's original Phase 0 move to this project's Phase 2, after Phase 1 establishes the exact dependency and build baseline.

No `go.mod`, `go.sum`, or implementation source changes are authorized in this phase. Their pre-existing changes remain user-owned and are preserved for Phase 1.

## Steps Completed

1. Read the approved root `PLAN.md` and materialized it as completed project overview, functional, architecture, component, and implementation-plan artifacts.
2. Set `.specs_skill_state/current_project.md` to `/specs/projects/bond-pebble-compact-keys` and ignored the per-worktree `.specs_skill_state/` directory.
3. Recorded Bond branch, commit, upstream, status, worktree hashes, Go/platform context, current Pebble option anchors, and the local reviewed Pebble checkout.
4. Preserved the untracked root `PLAN.md` in place and named it as an authoritative planned artifact.
5. Did not modify Go module files, source, tests, generated assets, or the external Pebble checkout.

## Baseline Capture

Captured on 2026-08-05 in `/home/peter/Dev/0xsequence/bond`:

| Item | Observed value | Drift/meaning |
|---|---|---|
| Bond branch | `next-gen` | No upstream is configured. The plan recorded `master`, but the content commit is identical. |
| Bond HEAD | `fea660a96e4685be235b88c694f2c630d09235d0` | Matches the root plan's Bond planning baseline. |
| Tracked worktree changes | `go.mod`, `go.sum` | Pre-existing user changes; preserved byte-for-byte in Phase 0. Diff stat: 20 insertions, 43 deletions total. |
| Untracked worktree artifact | `PLAN.md` | Approved authoritative plan; SHA-256 `799d1a33a564e7269af541ae21d502ca164d910a823eb5f2d69b78bb1a891773`. |
| Worktree `go.mod` | SHA-256 `0b05e68e87e1aab8159c5e2252654e504cf39290dcaf0e014bbcdf697ed0ac34` | Selects incorrect Pebble `v1.1.5`; also contains pre-existing dependency edits. |
| Worktree `go.sum` | SHA-256 `545d946c8c9831138d6d24937423c79cbcb372942a5f1f9ebbda668416c3b879` | Corresponding pre-existing user dependency state. |
| HEAD `go.mod` | SHA-256 `5b69179dd5473d4a4a5942169685037e68bc0e237d6ecfa00484a3ec85993d26` | Selects prior pseudo-version `v0.0.0-20251103183323-1df7d4aae653`; not the final target. |
| HEAD `go.sum` | SHA-256 `f61cfdea9b719b640456cda3dbd4535120779a70b86ad88f863d9809dcc37d0d` | Committed dependency baseline. |
| `options.go` | SHA-256 `d1ce4cdb0df6c010c31cdeac94dc9227b595b8d68f766a53821d337606c1b84d` | Clean versus HEAD; currently uses `FormatV2BlobFiles`, legacy filter fields, experimental span policy, and all-range fast compression. |
| Go runtime | `go1.26.5 linux/amd64`; module directive `go 1.25.3` | Runtime is newer than the module directive; Phase 1 records the toolchain used for validation. |
| Go workspace/proxy | no `GOWORK`; `GOTOOLCHAIN=auto`; `https://proxy.golang.org,direct` | Reproducibility inputs for dependency resolution. |
| Host | NixOS Linux `7.1.5`, x86_64 | Benchmark manifests must capture full machine/storage context later. |
| Local Pebble checkout | `/home/peter/Dev/other/pebble`, clean `master...origin/master` at `8fb150d9135d6f94e183a874475e0bd1afb18f63` | Matches the exact approved target and remains unmodified. |
| Pinned Pebble `FormatNewest` | numeric 30 at the approved checkout | User override requires this instead of the root plan's `FormatV2BlobFiles`. |
| Stock per-range schema selector | absent from `SpanPolicy` at the approved checkout | `Options.KeySchema` is global; Phase 6 must verify and report feasibility without a fork. |

The read-only `go-outdated` audit was available and invoked during inventory; it reported many stale direct/transitive modules. Its results are time-sensitive and were not used to edit the graph. Phase 1 must rerun it, retain the complete report, perform the approved upgrades, and rerun it after `go mod tidy`.

## Phase 1 Drift Gate

Before any source adaptation, Phase 1 must:

1. Recheck Bond status/HEAD and compare the three preserved worktree hashes above. If user-owned module changes differ, inventory them rather than overwriting them.
2. Recheck the local/reference Pebble API at exact commit, but resolve the Go module by commit rather than relying on a local checkout or branch name.
3. Replace `v1.1.5` with the exact commit and verify `go list -m -json github.com/cockroachdb/pebble` identifies origin hash `8fb150d9135d6f94e183a874475e0bd1afb18f63`.
4. Rerun `go-outdated` because latest compatible versions are time-sensitive.
5. Stop and re-audit if Bond key layout/comparer, Pebble schema registry, span output splitting, backup metadata, open paths, or `FormatNewest` have drifted materially.

## Planned Files

- Existing authoritative artifact: `PLAN.md` (preserved untracked).
- Project state: `.gitignore`, `.specs_skill_state/current_project.md`.
- Project artifacts: `specs/projects/bond-pebble-compact-keys/{project_overview.md,functional_spec.md,architecture.md,implementation_plan.md,Makefile}`.
- Component designs: `components/{dependency_and_benchmarks.md,key_schema_and_compatibility.md,catalog_and_routing.md}`.
- This execution record: `phase_plans/phase_0.md`.

## Checks

- Every spec/project/component/phase Markdown artifact has valid `status: complete` YAML frontmatter.
- The active-project pointer matches the new project path and is ignored by Git.
- `git diff --check` reports no whitespace errors.
- The project Makefile parses in a no-execute dry run.
- Post-edit hashes of `go.mod`, `go.sum`, `options.go`, and `PLAN.md` match this capture.
- Git status contains the pre-existing module changes and plan plus only the new approved spec/state artifacts.

## Tests

No implementation or runtime behavior changed, so no tests are written or run in Phase 0. Phase 1 owns build/runtime tests once the incorrect dependency state is replaced with the approved pin.
