# Phase 3 provenance

Captured on 2026-08-06 in `/home/peter/Dev/0xsequence/bond`.

| Field | Value |
| --- | --- |
| Bond HEAD recorded by manifests | `c7a57a4d352e01082de14a2ca52b8d8ea5e9ea3a` (dirty with reviewed Phase 3 changes) |
| Selected Pebble module | `v0.0.0-20260707124150-8fb150d9135d` |
| Pebble origin | `https://github.com/cockroachdb/pebble` |
| Pebble origin hash | `8fb150d9135d6f94e183a874475e0bd1afb18f63` |
| Module directory | Go module cache; no `replace` or workspace override |
| External Pebble checkout | `/home/peter/Dev/other/pebble`, exact same hash, clean worktree |
| Go | `go1.26.5 linux/amd64` |
| Kernel | `Linux 7.1.5 x86_64` |
| Format | `pebble.FormatNewest == 30` |
| Compression/filter baseline | corrected legacy / uniform Bloom |
| Dataset seed / rows | `20260806` / `20000` |
| Dataset digest | `4d0e5e997e9db4506464c418d2b95d60fdd8a08f4aafbc610ba8eeef9be5bd6e` |
| Benchmark source fingerprint | `373ad00ef5de9585a821d09ee93ae32578646163f280cc2136c15f9e080af298` |
| Manifest version | 2 |

Benchmark-only readers in every retained run, sorted:

```text
DefaultKeySchema(leveldb.BytewiseComparator,16)
bond/full-key/v1-b16
bond/full-key/v1-b32
bond/full-key/v1-b64
```

The production and retained writer remains `DefaultKeySchema(leveldb.BytewiseComparator,16)`. The three full-key names bind the rejected v1 non-empty complete-key algorithm and their bundle sizes inside `internal/fullkeyexperiment`; they are not registered by production options. The retained fingerprint identifies the exact benchmark source used for the recorded runs before the access-boundary correction; moving the unchanged encoding into the internal package does not rewrite historical manifests. No Pebble fork, patch, `replace`, production schema selector, catalog route, or logical-key change is present in this phase.
