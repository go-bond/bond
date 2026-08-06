# Pre-upgrade format helper

This nested module builds a historical Bond binary for
`backup.TestFormatNewestMigration`. It is intentionally pinned to Bond
`v0.2.17`, whose module graph selects Pebble
`v0.0.0-20251103183323-1df7d4aae653` and writes Pebble format 26.

The explicit `github.com/cockroachdb/swiss` override is only a compiler
compatibility adjustment: the version selected by the historical graph uses
removed runtime symbols and does not compile with Go 1.26. The helper still
uses the historical Bond and Pebble versions that define its storage behavior.

The retained `backup/testdata/pre-upgrade-v0.2.17.tar.gz` fixture was created
with this helper's `create` command. Its SHA-256 digest is
`80de58c6c0f5f955d6ab132914812ef23d5c572fadee25aca46cd0320afc3e42`, and it
contains the known logical key/value checked by both the historical helper and
the current migration test.

The test builds the helper into a temporary directory. It first proves that
the historical binary can read the retained fixture, then migrates and restores
the fixture with current Bond. Finally, it rewrites only Bond's format sidecar
to 26 so the historical Pebble engine itself reaches and rejects the physical
format-30 marker.
