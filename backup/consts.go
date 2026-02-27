package backup

import "time"

// DefaultConcurrency is the number of parallel uploads/downloads when
// Concurrency is not set (zero value) in BackupOptions or RestoreOptions.
const DefaultConcurrency = 4

// DefaultMaxUploadBPS is the aggregate upload rate limit in bytes per second
// applied across all concurrent streams when MaxUploadBPS is not set (zero value).
const DefaultMaxUploadBPS int64 = 100 * 1024 * 1024 // 100 MB/s

// DefaultMaxDownloadBPS is the aggregate download rate limit in bytes per second
// applied across all concurrent streams when MaxDownloadBPS is not set (zero value).
const DefaultMaxDownloadBPS int64 = 100 * 1024 * 1024 // 100 MB/s

// DefaultMaxUploadRetries is the number of retries per file after the first
// failed upload when MaxUploadRetries is not set (zero value).
const DefaultMaxUploadRetries = 3

// DefaultMaxDownloadRetries is the number of retries per file after the first
// failed download when MaxDownloadRetries is not set (zero value).
const DefaultMaxDownloadRetries = 3

// DefaultInitialRetryBackoff is the delay before the first retry when
// InitialRetryBackoff is not set (zero value).
const DefaultInitialRetryBackoff = 1 * time.Second

// MaxRetryBackoff caps the retry delay so long-running retries don't wait too long.
const MaxRetryBackoff = 30 * time.Second
