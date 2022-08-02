package reporters

import (
	"fmt"
	"io"

	"github.com/go-bond/bond/_benchmarks/bench"
)

type CSVReporter struct {
	w io.Writer
}

func NewCSVReporter(writer io.Writer) *CSVReporter {
	return &CSVReporter{w: writer}
}

func (r *CSVReporter) Report(results []bench.BenchmarkResult) error {
	err := r.writeHeader()
	if err != nil {
		return err
	}

	for _, result := range results {
		err = r.writeRow(result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *CSVReporter) writeHeader() error {
	_, err := r.w.Write([]byte("Test Case Name,ns/op,ops/s,B/op,allocs/op\n"))
	return err
}

func (r *CSVReporter) writeRow(result bench.BenchmarkResult) error {
	_, err := fmt.Fprintf(r.w, "%s,%d,%.3f,%d,%d\n",
		result.Benchmark.Name, result.NsPerOp(), result.OpsPerSec(), result.AllocedBytesPerOp(),
		result.AllocsPerOp())
	return err
}
