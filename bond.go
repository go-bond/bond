package bond

import "github.com/cockroachdb/pebble"

type DB struct {
	*pebble.DB
}
