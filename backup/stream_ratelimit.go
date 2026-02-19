package backup

// resolvePerStreamRate computes the per-stream rate limit in bytes/sec.
// Returns 0 if rate limiting is disabled (rateBPS < 0).
func resolvePerStreamRate(rateBPS int64, defaultBPS int64, concurrency int) float64 {
	if rateBPS < 0 {
		return 0
	}
	if rateBPS == 0 {
		rateBPS = defaultBPS
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	return float64(rateBPS) / float64(concurrency)
}
