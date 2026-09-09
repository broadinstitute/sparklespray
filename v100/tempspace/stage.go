package tempspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

// defaultStageInterval is long-lived by design: Stage is used to cache local
// files that are commonly resubmitted unchanged across many invocations
// (e.g. repeated `sparkles submit` runs against the same script), and
// content-addressing means an unchanged file's staged copy should act as a
// durable cache rather than being re-uploaded shortly after each submit.
const defaultStageInterval = 90 * 24 * time.Hour

// Stager stages local files into a self-expiring, content-addressed GCS
// scratch space (see self-expiring-CAS.md).
type Stager struct {
	client   *storage.Client
	interval time.Duration
}

// NewStager returns a Stager that uses client for all GCS operations.
func NewStager(client *storage.Client) *Stager {
	return &Stager{client: client, interval: defaultStageInterval}
}

// Stage uploads localPath into the CAS scratch space rooted at gcsPrefix (a
// "gs://bucket/prefix" string; prefix may be empty) if a blob with the same
// content is not already present there, and returns the resulting
// "gs://..." path. Identical content uploaded from different calls (even
// across separate processes/invocations) resolves to the same GCS object.
func (s *Stager) Stage(ctx context.Context, gcsPrefix, localPath string) (string, error) {
	bucket, prefix, err := parseGCSPath(gcsPrefix)
	if err != nil {
		return "", fmt.Errorf("parsing gcs_staging_prefix: %w", err)
	}

	name, err := hashFile(localPath)
	if err != nil {
		return "", fmt.Errorf("hashing %s: %w", localPath, err)
	}

	store := &gcsStore{client: s.client, bucket: bucket, prefix: prefix}
	ts := newTempspace(store, s.interval)

	path, found, err := ts.GetPathIfExists(ctx, name)
	if err != nil {
		return "", fmt.Errorf("checking staging cache for %s: %w", localPath, err)
	}
	if !found {
		if err := ts.Put(ctx, localPath, path); err != nil {
			return "", fmt.Errorf("uploading %s: %w", localPath, err)
		}
	}

	return "gs://" + bucket + "/" + store.key(path), nil
}

// hashFile returns the hex-encoded sha256 digest of the file at path,
// streamed so the whole file need not be held in memory.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
