package blobfs

import (
	"context"
	"testing"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func generateTestBucket(t *testing.T, files map[string][]byte) *blob.Bucket {
	t.Helper()

	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)

	for name, data := range files {
		if err := bucket.WriteAll(ctx, name, data, nil); err != nil {
			t.Fatal(err)
		}
	}

	return bucket
}
