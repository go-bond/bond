package reporters

import "testing"

type Reporter interface {
	Report(results []testing.BenchmarkResult) error
}
