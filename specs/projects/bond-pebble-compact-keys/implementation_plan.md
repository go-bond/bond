---
status: complete
---

# Implementation Plan: Bond Pebble Compact Keys

## Authoritative Artifacts

The root `PLAN.md` is retained as the approved detailed source and is part of this project's planned artifacts. These completed specs apply the user's later overrides where they conflict with that plan.

## Phases

- [x] Phase 0: Materialize the approved spec project and freeze the current Bond/Pebble branch, worktree, environment, option, and artifact baselines without touching Go modules or implementation source.
- [x] Phase 1: Establish the dependency/build baseline—pin Pebble exact commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`, verify it with `go list -m`, run `go-outdated`, upgrade all other dependencies to latest compatible versions, move to pinned `FormatNewest`, adapt APIs, and prove build/reopen/restore behavior.
- [ ] Phase 2: Add the deterministic benchmark/oracle harness, correct compression policy, compare table filters independently, retain reproducible manifests, and freeze the winning non-schema baseline.
- [ ] Phase 3: Implement and prove global `bond/full-key/v1` bundle candidates on stock Pebble with unit, fuzz/property, integration, mixed-schema, and lifecycle benchmarks; accept or reject with data.
- [ ] Phase 4: Centralize the reader registry and writer selection, add backup/checkpoint compatibility metadata and restore validation, update all open paths and inspection tooling, and exercise reversible writer rollout.
- [ ] Phase 5: Implement the pre-open declarative catalog, stable fingerprints/diffs, bound typed table/index handles, breaking API migration guidance, and compile-tested multi-table examples.
- [ ] Phase 6: Evaluate typed schema families and per-range routing strictly with stock pinned Pebble; implement measured supported behavior or document a no-go and separately scoped upstream proposal without modifying/forking Pebble here.
- [ ] Phase 7: Run the final full/race/focused compatibility suites, benchmark reproducibility and regression audit, backup/restore/rollback exercises, open-path and documentation audit, and publish the final accepted/rejected decisions.

Each later phase starts by rerunning its relevant drift checks and stops at the gates in the functional and component specifications.
