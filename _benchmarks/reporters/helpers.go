package reporters

import "github.com/go-bond/bond/_benchmarks/bench"

func findMaxLength(results []bench.BenchmarkResult) (nameLen int, opTimeLen int, memLen int) {
	for _, result := range results {
		cNameLen := len(result.Name)
		if nameLen < cNameLen {
			nameLen = cNameLen
		}

		cOpTimeLen := len(result.Result.String())
		if opTimeLen < cOpTimeLen {
			opTimeLen = cOpTimeLen
		}

		cMemLen := len(result.Result.MemString())
		if memLen < cMemLen {
			memLen = cMemLen
		}
	}
	return
}
