package backup

// DefaultConcurrency is the number of parallel uploads/downloads when
// Concurrency is not set (zero value) in BackupOptions or RestoreOptions.
const DefaultConcurrency = 4

// DefaultRateLimit is the aggregate rate limit in bytes per second applied
// across all concurrent streams when RateLimit is not set (zero value).
const DefaultRateLimit float64 = 100 * 1024 * 1024 // 100 MB/s

// resolvePerStreamRate computes the per-stream rate limit in bytes/sec.
// Returns 0 if rate limiting is disabled (rateLimit < 0).
func resolvePerStreamRate(rateLimit float64, concurrency int) float64 {
	if rateLimit < 0 {
		return 0
	}
	if rateLimit == 0 {
		rateLimit = DefaultRateLimit
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	return rateLimit / float64(concurrency)
}

// ProgressEvent is emitted after each file transfer completes.
type ProgressEvent struct {
	File       string // file just completed
	FileSize   int64  // size of that file
	FilesDone  int    // files completed so far
	FilesTotal int    // total files in operation
	BytesDone  int64  // cumulative bytes completed
	BytesTotal int64  // total bytes in operation
}

// ProgressFunc is called after each file upload or download completes.
// Implementations must be safe for concurrent use.
type ProgressFunc func(event ProgressEvent)
