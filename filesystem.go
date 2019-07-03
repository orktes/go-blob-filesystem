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
}

// New returns a new GCSFileSystem for given bucket
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
	}
	return f, nil
}
