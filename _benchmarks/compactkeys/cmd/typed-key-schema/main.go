package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/_benchmarks/compactkeys"
)

func main() {
	output := flag.String("output", "typed-key-schema-results", "directory for manifests and summary.csv")
	workRoot := flag.String("work-root", "", "parent directory for temporary benchmark databases")
	rows := flag.Int("rows", 10_000, "logical rows per isolated family run")
	seed := flag.Int64("seed", 20260806, "deterministic base dataset seed")
	warmups := flag.Int("warmups", 1, "warmup runs per candidate")
	repetitions := flag.Int("repetitions", 3, "retained runs per candidate")
	flag.Parse()

	if *rows <= 0 || *warmups < 0 || *repetitions <= 0 {
		fatalf("rows and repetitions must be positive; warmups must not be negative")
	}
	repoRoot, err := findRepositoryRoot()
	if err != nil {
		fatalf("find repository: %v", err)
	}
	outputDir, err := filepath.Abs(*output)
	if err != nil {
		fatalf("resolve output: %v", err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		fatalf("create output: %v", err)
	}
	tempParent := *workRoot
	if tempParent == "" {
		tempParent = os.TempDir()
	}
	tempParent, err = filepath.Abs(tempParent)
	if err != nil {
		fatalf("resolve work root: %v", err)
	}
	if err := os.MkdirAll(tempParent, 0o755); err != nil {
		fatalf("create work root: %v", err)
	}

	var manifests []compactkeys.RunManifest
	for _, suite := range compactkeys.TypedFamilySuites(bond.MediumPerformance, *seed, *rows) {
		dataset, err := compactkeys.Generate(suite.Dataset)
		if err != nil {
			fatalf("generate %s dataset: %v", suite.Name, err)
		}
		for _, candidate := range suite.Engines {
			for warmup := 1; warmup <= *warmups; warmup++ {
				fmt.Printf("warmup suite=%s candidate=%s run=%d/%d\n", suite.Name, candidate.Name, warmup, *warmups)
				_, err := run(repoRoot, tempParent, candidate, dataset, compactkeys.RunSpec{
					Warmups: *warmups, Repetitions: *repetitions, Run: -warmup, BatchSize: 512,
				})
				if err != nil {
					fatalf("warmup %s: %v", candidate.Name, err)
				}
			}
			for repetition := 1; repetition <= *repetitions; repetition++ {
				fmt.Printf("retain suite=%s candidate=%s run=%d/%d\n", suite.Name, candidate.Name, repetition, *repetitions)
				manifest, err := run(repoRoot, tempParent, candidate, dataset, compactkeys.RunSpec{
					Warmups: *warmups, Repetitions: *repetitions, Run: repetition, BatchSize: 512,
				})
				if err != nil {
					fatalf("run %s: %v", candidate.Name, err)
				}
				path := filepath.Join(outputDir, fmt.Sprintf("%s-run-%02d.json", candidate.Name, repetition))
				if err := manifest.Write(path); err != nil {
					fatalf("write manifest: %v", err)
				}
				manifests = append(manifests, manifest)
			}
		}
	}
	if err := writeSummary(filepath.Join(outputDir, "summary.csv"), manifests); err != nil {
		fatalf("write summary: %v", err)
	}
	fmt.Printf("wrote %d manifests and summary.csv to %s\n", len(manifests), outputDir)
}

func run(
	repoRoot, tempParent string,
	engine compactkeys.EngineSpec,
	dataset compactkeys.Dataset,
	runSpec compactkeys.RunSpec,
) (compactkeys.RunManifest, error) {
	runRoot, err := os.MkdirTemp(tempParent, "bond-typed-key-schema-")
	if err != nil {
		return compactkeys.RunManifest{}, err
	}
	defer os.RemoveAll(runRoot)
	return compactkeys.RunLifecycle(context.Background(), compactkeys.RunRequest{
		RepoRoot: repoRoot,
		RootDir:  runRoot,
		Engine:   engine,
		Run:      runSpec,
		Dataset:  dataset,
	})
}

func writeSummary(path string, manifests []compactkeys.RunManifest) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	rows := [][]string{{
		"candidate", "key_shape", "writer_schema", "run", "dataset_digest", "database_bytes",
		"sst_physical_bytes", "property_raw_key_bytes", "property_data_bytes", "property_index_uncompressed_bytes",
		"compaction_ns", "compaction_cpu_ns", "write_amplification", "hit_p95_ns", "miss_p95_ns", "registered_readers",
	}}
	for _, manifest := range manifests {
		result := manifest.Results
		readers, err := json.Marshal(manifest.Schema.RegisteredReaders)
		if err != nil {
			file.Close()
			return err
		}
		rows = append(rows, []string{
			manifest.Engine.Name,
			string(manifest.Dataset.KeyShape),
			manifest.ActiveKeySchema,
			strconv.Itoa(manifest.Run.Run),
			manifest.DatasetDigest,
			strconv.FormatUint(result.DatabaseBytes, 10),
			strconv.FormatUint(result.AfterCompaction.SST.PhysicalBytes, 10),
			strconv.FormatUint(result.AfterCompaction.SST.PropertyRawKeyBytes, 10),
			strconv.FormatUint(result.AfterCompaction.SST.PropertyDataBytes, 10),
			strconv.FormatUint(result.AfterCompaction.SST.PropertyIndexUncompressedBytes, 10),
			strconv.FormatInt(result.CompactionNS, 10),
			strconv.FormatInt(result.ProcessCompactionCPUNS, 10),
			strconv.FormatFloat(result.AfterCompaction.Metrics.WriteAmplification, 'g', -1, 64),
			strconv.FormatInt(result.HitLatency.P95NS, 10),
			strconv.FormatInt(result.PointMissLatency.P95NS, 10),
			string(readers),
		})
	}
	if err := writer.WriteAll(rows); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func findRepositoryRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			if filepath.Base(directory) == "_benchmarks" {
				return filepath.Dir(directory), nil
			}
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("go.mod not found")
		}
		directory = parent
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
