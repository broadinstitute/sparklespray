package v100

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

type GCSTransferClient struct {
	gcsClient *storage.Client
}

type TransferClient interface {
	uploadFile(ctx context.Context, localPath, destPath string) error
	downloadFile(ctx context.Context, sourcePath, localPath string) error
}

func (tc *GCSTransferClient) uploadFile(ctx context.Context, localPath, gcsPath string) error {
	bucket, object, err := parseGCSPath(gcsPath)
	if err != nil {
		return err
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening local file: %w", err)
	}
	defer f.Close()

	w := tc.gcsClient.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return fmt.Errorf("uploading to %s: %w", gcsPath, err)
	}
	return w.Close()
}

func (tc *GCSTransferClient) downloadFile(ctx context.Context, gcsPath, localPath string) error {
	bucket, object, err := parseGCSPath(gcsPath)
	if err != nil {
		return err
	}
	r, err := tc.gcsClient.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("opening GCS object: %w", err)
	}
	defer r.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("creating local file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("downloading object: %w", err)
	}
	return nil
}

func parseGCSPath(gcsPath string) (bucket, object string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path %q: must start with gs://", gcsPath)
	}
	path := strings.TrimPrefix(gcsPath, "gs://")
	bucket, object, _ = strings.Cut(path, "/")
	return bucket, object, nil
}
