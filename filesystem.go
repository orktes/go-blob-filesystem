package blobfs

import (
	"context"
	"net/http"

	"gocloud.dev/blob"
)

// BlobFileSystem implements http.FileSystem using a *gocloud.dev/blob.Bucket
type BlobFileSystem struct {
	bucket *blob.Bucket
	ctx    context.Context
	config Config
}

// NewWithConfig returns a new BlobFileSystem for given bucket and config
func NewWithConfig(bucket *blob.Bucket, config Config) *BlobFileSystem {
	return &BlobFileSystem{
		bucket: bucket,
		ctx:    context.Background(),
		config: config,
	}
}

// New returns a new BlobFileSystem for given bucket
func New(bucket *blob.Bucket) *BlobFileSystem {
	return &BlobFileSystem{
		bucket: bucket,
		ctx:    context.Background(),
	}
}

// Open returns a http.File from the blob store for given filepath/name
func (blobfs *BlobFileSystem) Open(name string) (http.File, error) {
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}

	f := &blobFile{
		name:   name,
		bucket: blobfs.bucket,
		ctx:    blobfs.ctx,
		config: blobfs.config,
	}
	return f, nil
}
