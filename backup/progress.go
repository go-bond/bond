package backup

// DefaultConcurrency is the number of parallel uploads/downloads when
// Concurrency is not set (zero value) in BackupOptions or RestoreOptions.
const DefaultConcurrency = 4

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
