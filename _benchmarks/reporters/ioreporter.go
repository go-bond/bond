package reporters

import (
	"fmt"
	"io"

	"github.com/go-bond/bond/_benchmarks/bench"
)

type IOReporter struct {
	w io.Writer
}

func NewIOReporter(writer io.Writer) *IOReporter {
	return &IOReporter{w: writer}
}

func (r *IOReporter) Report(results []bench.BenchmarkResult) error {
	nameLen, opTimeLen, memLen := findMaxLength(results)
	format := fmt.Sprintf("%%-%ds %%-%ds %%-%ds\n", nameLen, opTimeLen, memLen)

	for _, result := range results {
		_, err := fmt.Fprintf(r.w, format, result.Name, result.String(), result.MemString())
		if err != nil {
			return err
		}
	}

	return nil
}
