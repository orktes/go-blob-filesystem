package main

import (
	"context"
	"log"
	"net/http"

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
	bucket, err := gcsblob.OpenBucket(ctx, client, "some_gcs_bucket", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()

	// Create a web server
	log.Fatal(http.ListenAndServe(":8080", http.FileServer(blobfs.New(bucket))))
}
