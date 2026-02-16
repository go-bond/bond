package backup

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
