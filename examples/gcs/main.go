package main

import (
	"context"
	"fmt"
	"log"

	blobfs "github.com/orktes/go-blob-filesystem"

	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
)

func main() {

	// Variables set up elsewhere:
	ctx := context.Background()

	// Your GCP credentials.
	// See https://cloud.google.com/docs/authentication/production
	// for more info on alternatives.
	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Create an HTTP client.
	// This example uses the default HTTP transport and the credentials
	// created above.
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds))
	if err != nil {
		log.Fatal(err)
	}

	// Create a *blob.Bucket.
	bucket, err := gcsblob.OpenBucket(ctx, client, "some_bucket", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()

	// Create new blob fs
	blob := blobfs.New(bucket)
	file, err := blob.Open("/some_path")
	fmt.Printf("%+v %+v\n", file, err)

	files, err := file.Readdir(0)
	for _, f := range files {
		fmt.Printf("%s", f.Name())
	}
}
