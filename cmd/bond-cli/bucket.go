package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/gcs"
	"github.com/thanos-io/objstore/providers/s3"
	"github.com/urfave/cli/v2"
)

// Shared flags.
var (
	_FlagStorage = &cli.StringFlag{
		Name:     "storage",
		Usage:    "storage backend type: s3, gcs, fs",
		Required: true,
	}
	_FlagPrefix = &cli.StringFlag{
		Name:  "prefix",
		Usage: "object storage prefix for backups (e.g. backups/)",
	}
)

// S3 flags.
var (
	_FlagS3Endpoint = &cli.StringFlag{
		Name:    "s3-endpoint",
		Usage:   "S3 endpoint URL",
		EnvVars: []string{"S3_ENDPOINT"},
	}
	_FlagS3Bucket = &cli.StringFlag{
		Name:    "s3-bucket",
		Usage:   "S3 bucket name",
		EnvVars: []string{"S3_BUCKET"},
	}
	_FlagS3AccessKey = &cli.StringFlag{
		Name:    "s3-access-key",
		Usage:   "S3 access key",
		EnvVars: []string{"AWS_ACCESS_KEY_ID"},
	}
	_FlagS3SecretKey = &cli.StringFlag{
		Name:    "s3-secret-key",
		Usage:   "S3 secret key",
		EnvVars: []string{"AWS_SECRET_ACCESS_KEY"},
	}
	_FlagS3Region = &cli.StringFlag{
		Name:    "s3-region",
		Usage:   "S3 region",
		EnvVars: []string{"AWS_REGION"},
	}
	_FlagS3Insecure = &cli.BoolFlag{
		Name:  "s3-insecure",
		Usage: "use HTTP instead of HTTPS for S3",
	}
)

// GCS flags.
var (
	_FlagGCSBucket = &cli.StringFlag{
		Name:    "gcs-bucket",
		Usage:   "Google Cloud Storage bucket name",
		EnvVars: []string{"GCS_BUCKET"},
	}
	_FlagGCSServiceAccount = &cli.StringFlag{
		Name:    "gcs-service-account",
		Usage:   "path to GCS service account JSON key file (falls back to Google ADC if omitted)",
		EnvVars: []string{"GOOGLE_APPLICATION_CREDENTIALS"},
	}
)

// Filesystem flags.
var (
	_FlagFSDirectory = &cli.StringFlag{
		Name:  "fs-directory",
		Usage: "local directory to use as the bucket root",
	}
)

// bucketFlags contains all storage flags for all backends.
var bucketFlags = []cli.Flag{
	_FlagStorage,
	_FlagPrefix,
	// S3
	_FlagS3Endpoint,
	_FlagS3Bucket,
	_FlagS3AccessKey,
	_FlagS3SecretKey,
	_FlagS3Region,
	_FlagS3Insecure,
	// GCS
	_FlagGCSBucket,
	_FlagGCSServiceAccount,
	// Filesystem
	_FlagFSDirectory,
}

// newBucket creates an objstore.Bucket based on the --storage flag.
func newBucket(ctx *cli.Context) (objstore.Bucket, error) {
	switch ctx.String("storage") {
	case "s3":
		return newS3Bucket(ctx)
	case "gcs":
		return newGCSBucket(ctx)
	case "fs":
		return newFSBucket(ctx)
	default:
		return nil, fmt.Errorf("unknown storage type %q; supported: s3, gcs, fs", ctx.String("storage"))
	}
}

func newS3Bucket(ctx *cli.Context) (objstore.Bucket, error) {
	endpoint := ctx.String("s3-endpoint")
	bucketName := ctx.String("s3-bucket")
	accessKey := ctx.String("s3-access-key")
	secretKey := ctx.String("s3-secret-key")

	if endpoint == "" {
		return nil, fmt.Errorf("--s3-endpoint is required when using S3 storage")
	}
	if bucketName == "" {
		return nil, fmt.Errorf("--s3-bucket is required when using S3 storage")
	}
	if accessKey == "" {
		return nil, fmt.Errorf("--s3-access-key is required when using S3 storage")
	}
	if secretKey == "" {
		return nil, fmt.Errorf("--s3-secret-key is required when using S3 storage")
	}

	cfg := s3.Config{
		Bucket:    bucketName,
		Endpoint:  endpoint,
		Region:    ctx.String("s3-region"),
		AccessKey: accessKey,
		SecretKey: secretKey,
		Insecure:  ctx.Bool("s3-insecure"),
	}

	bucket, err := s3.NewBucketWithConfig(log.NewNopLogger(), cfg, "bond-cli", nil)
	if err != nil {
		return nil, fmt.Errorf("create S3 bucket client: %w", err)
	}
	return bucket, nil
}

func newGCSBucket(ctx *cli.Context) (objstore.Bucket, error) {
	bucketName := ctx.String("gcs-bucket")
	if bucketName == "" {
		return nil, fmt.Errorf("--gcs-bucket is required when using GCS storage")
	}

	cfg := gcs.Config{
		Bucket: bucketName,
	}

	if saPath := ctx.String("gcs-service-account"); saPath != "" {
		data, err := os.ReadFile(saPath)
		if err != nil {
			return nil, fmt.Errorf("read service account file %q: %w", saPath, err)
		}
		cfg.ServiceAccount = string(data)
	}

	bucket, err := gcs.NewBucketWithConfig(ctx.Context, log.NewNopLogger(), cfg, "bond-cli", func(rt http.RoundTripper) http.RoundTripper { return rt })
	if err != nil {
		return nil, fmt.Errorf("create GCS bucket client: %w", err)
	}
	return bucket, nil
}

func newFSBucket(ctx *cli.Context) (objstore.Bucket, error) {
	dir := ctx.String("fs-directory")
	if dir == "" {
		return nil, fmt.Errorf("--fs-directory is required when using filesystem storage")
	}

	bucket, err := filesystem.NewBucket(dir)
	if err != nil {
		return nil, fmt.Errorf("create filesystem bucket: %w", err)
	}
	return bucket, nil
}
