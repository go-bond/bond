# Phase 6 provenance

| Item | Value |
|---|---|
| Bond revision at retained run | `eab122f1daab97b029861c59358f117a3482e4ad` plus dirty Phase 6 source fingerprint |
| Phase 6 source fingerprint | `3439367c190a255c02ab52197a7e9ff62800bd0820fd5d0d8251e320245c111d` |
| Pebble module | `github.com/cockroachdb/pebble v0.0.0-20260707124150-8fb150d9135d` |
| Pebble origin hash | `8fb150d9135d6f94e183a874475e0bd1afb18f63` |
| Pebble origin URL | `https://github.com/cockroachdb/pebble` |
| Go runtime | `go1.26.5 linux/amd64` |
| CPU | `AMD Ryzen 9 7950X 16-Core Processor` (32 logical CPUs) |
| Storage format | 30 (`pebble.FormatNewest` at the pin) |
| Compression/filter | frozen legacy compression / uniform Bloom |
| Typed bundle size | 16, immutable under every v1 name |

`go.mod` and `go.sum` are unchanged in Phase 6 and contain no local Pebble replace. The separate `/home/peter/Dev/other/pebble` checkout was verified clean at the approved hash after implementation and measurements.

The manifest's `bond_dirty=true` is expected because the retained benchmark measures the uncommitted Phase 6 implementation. Reproducibility uses the recorded content fingerprint rather than claiming the parent commit alone identifies the experiment source.
