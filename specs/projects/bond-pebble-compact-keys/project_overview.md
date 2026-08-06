---
status: complete
---

# Bond Pebble Compact Keys

## Purpose

Modernize Bond's Pebble integration and determine, with reproducible evidence, whether columnar key schemas can materially reduce storage without changing the logical meaning or ordering of Bond keys. The project keeps one Bond database backed by one Pebble database, so atomic batches, recovery, cache ownership, compaction, and backup remain unified.

The root [`PLAN.md`](../../../PLAN.md) is the authoritative design source. These project artifacts make that approved design executable through the spec workflow. When the plan and the user's later directions differ, the following approved overrides govern implementation.

## Fixed Overrides

- Pin `github.com/cockroachdb/pebble` to exact commit `8fb150d9135d6f94e183a874475e0bd1afb18f63` from Pebble `master`. A release tag or a different commit is not equivalent.
- Run `go-outdated` and upgrade every dependency to its latest compatible version at implementation time. Pebble remains fixed to the exact commit even if the audit suggests another version.
- Breaking Bond API changes are permitted when they produce a clearer catalog, options, or table/index API. Compatibility shims are optional, not an acceptance requirement.
- Use `pebble.FormatNewest` from the pinned Pebble revision. At the approved revision this is format major version 30. The implementation must test reopen, restore, and migration behavior around this one-way ratchet.

## Intended Outcomes

- A clean dependency and build baseline on the approved Pebble commit and current compatible dependencies.
- Deterministic benchmark fixtures, a logical correctness oracle, and attributable compression/filter results.
- A proven or data-rejected global `bond/full-key/v1` physical schema that reconstructs every logical key byte and preserves comparer and prefix semantics.
- Central schema registration for normal opens, migrations, backups, restores, inspectors, and benchmarks.
- A declarative pre-open catalog with stable table/index identities, typed handles, durable descriptors, and compile-tested multi-table examples.
- A stock-Pebble feasibility decision for typed schemas and per-range routing. The project does not silently grow a Pebble fork if stock Pebble lacks the necessary selector.
- A final compatibility, correctness, performance, and documentation audit.

## Boundaries

The first schema is a physical SST encoding only. It does not add schema hints to `Set`, change WAL or memtable key bytes, or create a Pebble database per table. Logical tuple-key v2, surrogate row IDs, posting lists, unique-index value layouts, and other logical storage redesigns require separate projects after this work reports measured results.

The existing root `PLAN.md`, currently an untracked user artifact, remains in place and is included in this project's planned artifacts. It must not be rewritten or discarded during implementation.
