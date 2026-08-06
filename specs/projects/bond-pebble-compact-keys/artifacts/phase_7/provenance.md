# Phase 7 provenance

Verified on 2026-08-06 before the Phase 7 commit. The worktree is intentionally
dirty with the reviewed project implementation and final-validation fixes.

| Item | Value |
| --- | --- |
| Bond branch / HEAD | `next-gen` / `cbad595962ed62a7eefbc6df44f545884f0233cf` |
| upstream relation | one commit ahead of `origin/next-gen`, zero behind |
| Go runtime | `go1.26.5 linux/amd64`, module language version `1.25.3` |
| host | `pax`, AMD Ryzen 9 7950X, 32 logical CPUs, Linux 7.1.5 |
| selected Pebble module | `v0.0.0-20260707124150-8fb150d9135d` |
| approved Pebble commit | `8fb150d9135d6f94e183a874475e0bd1afb18f63` |
| Pebble origin | `https://github.com/cockroachdb/pebble` |
| `go.mod` SHA-256 | `fa2fb5892ba6384ba730ddf4356207aa52d921432e7c0e3e036fe0ffdd078b49` |
| `go.sum` SHA-256 | `ad435a7f5fa7bf635ff270f8e926171810f565cbe00de1b95ced53dd6f999151` |
| final benchmark source fingerprint | `eb3d70919937e7a7716175a1fe1e38449c36cdc32ea254696b6c4d306cf646a9` |

`go list -m -json github.com/cockroachdb/pebble` selected the pseudo-version
above. Resolving the approved revision directly reported origin URL
`https://github.com/cockroachdb/pebble` and full origin hash
`8fb150d9135d6f94e183a874475e0bd1afb18f63`. The main module contains no
`replace`; `go list -m -u -json all` reported zero replacements.

The separate checkout `/home/peter/Dev/other/pebble` was inspected read-only.
It remained on branch `master` at the exact approved hash, with origin
`https://github.com/cockroachdb/pebble.git` and an empty `git status --short`.
Phase 7 did not edit it.

## Dependency drift result

The final package-graph upgrade probe ran `go get -u ./...`, restored the exact
Pebble pin, and ran `go mod tidy`. It found one real direct update:
`github.com/klauspost/compress` moved from `v1.19.1` to `v1.19.2`.
`github.com/cockroachdb/errors v1.14.0` is now correctly classified as direct
because Phase 6 source imports it; its version did not change.

Afterward, `go list -m -u -json all` reported 22 direct modules and no direct
module with an available update. Its 65 remaining update suggestions are
module-metadata-only indirect entries that no package in the tidied build graph
requires: a `go mod why -m` pass over all 65 reported `NEEDED=0`.
`go-outdated` independently reported the same condition: every row with a
non-empty newer version had `DIRECT=false`. Pebble is the approved exact pin and
remains an explicit exception to automated latest selection.

`go mod tidy -go=1.25.3 -diff` produced no output.

## Format provenance

The pinned module's `format_major_version.go` defines `FormatNewest` as its
latest stable format. Bond binds `PebbleDBFormat` directly to it; the selected
revision's numeric value is 30. Production option construction, new opens,
explicit migration, checkpoint compatibility metadata, restore validation,
offline inspection bounds, and focused tests all use that value. The retained
pre-upgrade fixture proves the format-26 to format-30 ratchet and rejection by
the historical engine.
