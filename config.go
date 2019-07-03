package blobfs

import "time"

// Config holds BlobFileSystem configs
type Config struct {
	// RequestTimeout time out for requests going to buckets
	RequestTimeout time.Duration
}
