package blobfs

import (
	"context"
	"os"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

type blobFileInfo struct {
	name   string
	bucket *blob.Bucket
	ctx    context.Context

	isDir bool
	attrs *blob.Attributes
}

func newBlobFileInfo(ctx context.Context, name string, bucket *blob.Bucket) (*blobFileInfo, error) {
	bfi := &blobFileInfo{
		name:   name,
		bucket: bucket,
		ctx:    ctx,
	}

	attrs, err := bucket.Attributes(ctx, name)
	if err != nil {
		if gcerrors.Code(err) != gcerrors.NotFound {
			return nil, err
		}

		// Object was not found. It might be a "directory"
		iter := bucket.List(&blob.ListOptions{Prefix: name, Delimiter: "/"})
		_, err := iter.Next(ctx)
		if err != nil {
			if gcerrors.Code(err) != gcerrors.NotFound {
				return nil, os.ErrNotExist
			}
			return nil, err
		}

		bfi.isDir = true
	}

	bfi.attrs = attrs

	return bfi, nil
}

func (fi *blobFileInfo) Name() string {
	parts := strings.Split(strings.Trim(fi.name, "/"), "/")
	return parts[len(parts)-1]
}

func (fi *blobFileInfo) Size() int64 {
	if fi.isDir {
		return 0
	}
	return fi.attrs.Size
}

func (fi *blobFileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir
	}

	// Mode() is not used by http.FileServer
	return os.FileMode(0)
}

func (fi *blobFileInfo) ModTime() time.Time {
	if fi.isDir {
		return time.Time{}
	}
	return fi.attrs.ModTime
}

func (fi *blobFileInfo) IsDir() bool {
	return fi.isDir
}

func (fi *blobFileInfo) Sys() interface{} {
	return nil
}
