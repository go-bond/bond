# Phase 1 Verification Evidence

Captured on 2026-08-06 with `go1.26.5 linux/amd64`.

| Command or gate | Result |
|---|---|
| `go list -m -json github.com/cockroachdb/pebble@8fb150d9135d6f94e183a874475e0bd1afb18f63` | PASS: `v0.0.0-20260707124150-8fb150d9135d`, populated module directory, full `Origin.Hash` match. |
| `go get -u ./...`, Pebble repin, `go mod tidy -go=1.25.3`, and `go mod tidy -go=1.25.3 -diff` | PASS: final requirements follow the actual build/test package graph, retain the Go 1.25.3 directive, and a fresh tidy produces no diff. The unused 109-module overlay and later crypto11/pkcs11/pool probe requirements were removed. |
| `crypto11` compatible-path probe | PASS: retained research proves v1.6.7 succeeds at the old path and v1.6.8 declares a different module path. No crypto11 override is selected because `go mod why` reports it unneeded. |
| `go-outdated` plus `go mod why` audit | PASS: before/after reports retained. All 64 final module-metadata suggestions report that the main module does not need their packages, and none is an explicit requirement. |
| Dependency license scan | PASS: `go-licenses v2.0.1` reports only Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, and MIT across Bond packages and the tparse tool. GPL/MPL graph-only pins are absent from `go.mod` and `go.sum`. |
| `gofmt`, `go vet ./...`, `make build`, `git diff --check` | PASS. |
| `TestBuildPebbleOptionsProfiles`, `TestPebbleModuleOrigin`, comparer, and Bond version/open tests | PASS in 0.211s. The span-policy assertions cover both suffix and blob-separation disable flags. |
| `TestFormatNewestMigration` | PASS in 1.660s independently: retained Bond v0.2.17/Pebble-format-26 fixture verification, format-30 ratchet, downgrade rejection, reopen, complete checkpoint backup/restore, exact KV comparison, and physical rejection by the historical Pebble engine. |
| Historical helper `go test ./...` and `go vet ./...` | PASS against its isolated v0.2.17 module. |
| `go test ./tests -run '^TestBackupRestore_' -count=1` | PASS in 16.151s. |
| `go test ./backup -run '^Test(RestoreComplete|RestoreWithIncrementals|BackupIncremental|FormatNewestMigration)$' -count=1` | PASS in 16.018s. |
| Focused race profile/comparer/migration tests | PASS: root 1.019s and backup 2.576s. |
| `go test ./... -count=1` | PASS: root 36.292s, backup 347.134s, integration tests 15.615s, all remaining packages green. |
| `go test -race ./... -count=1` | PASS: root 39.856s, backup 350.279s, integration tests 22.316s, all remaining packages green. |

The full and full-race commands are always run serially because repository
tests share fixed `test_db` paths.
