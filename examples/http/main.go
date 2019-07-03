package main

import (
	"context"
	"log"
	"net/http"

	blobfs "github.com/orktes/go-blob-filesystem"
	"gocloud.dev/blob/memblob"
)

func main() {
	ctx := context.Background()

	// This can be any blob store. GCS, S3 etc.
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bucket.WriteAll(ctx, "foo", []byte("foo content"), nil)
	bucket.WriteAll(ctx, "bar", []byte("bar content"), nil)
	bucket.WriteAll(ctx, "biz/fuz", []byte("fuz content"), nil)

	log.Fatal(http.ListenAndServe(":8080", http.FileServer(blobfs.New(bucket))))
}
