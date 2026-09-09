// Package tempspace implements the "self-expiring CAS" protocol described in
// self-expiring-CAS.md: a content-addressed blob store on top of a flat
// object store (GCS) that garbage-collects itself via amortized, inline
// sweeps triggered on writes, without any external scheduler or TTL
// metadata.
//
// This package is intentionally standalone (it does not import package
// v100) so that v100 can import it without creating a cycle.
package tempspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// ObjStore is the object store interface required by the self-expiring-CAS
// protocol (see "Object Store Interface" in self-expiring-CAS.md). Paths are
// keys relative to whatever root the ObjStore implementation is scoped to.
type ObjStore interface {
	exists(ctx context.Context, path string) (bool, error)
	put(ctx context.Context, localPath, storePath string) error
	get(ctx context.Context, storePath, localPath string) error
	// copy performs a (preferably server-side) copy from srcPath to
	// dstPath. If srcPath does not exist, it returns an error for which
	// errors.Is(err, errObjectNotExist) is true.
	copy(ctx context.Context, srcPath, dstPath string) error
	// findPathsInRange returns paths in the half-open range [start, end),
	// sorted ascending, capped at limit entries.
	findPathsInRange(ctx context.Context, start, end string, limit int) ([]string, error)
	delete(ctx context.Context, paths []string) error
}

// errObjectNotExist is returned (wrapped) by copy when the source object
// does not exist.
var errObjectNotExist = errors.New("tempspace: object does not exist")

// gcsStore is an ObjStore backed by a single GCS bucket, with all paths
// rooted under prefix.
type gcsStore struct {
	client *storage.Client
	bucket string
	prefix string // may be empty; joined with "/" ahead of every key
}

func (s *gcsStore) key(path string) string {
	if s.prefix == "" {
		return path
	}
	return s.prefix + "/" + path
}

func (s *gcsStore) exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.Bucket(s.bucket).Object(s.key(path)).Attrs(ctx)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("checking existence of %s: %w", path, err)
}

func (s *gcsStore) put(ctx context.Context, localPath, storePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening %s: %w", localPath, err)
	}
	defer f.Close()

	w := s.client.Bucket(s.bucket).Object(s.key(storePath)).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return fmt.Errorf("uploading %s to %s: %w", localPath, storePath, err)
	}
	return w.Close()
}

func (s *gcsStore) get(ctx context.Context, storePath, localPath string) error {
	r, err := s.client.Bucket(s.bucket).Object(s.key(storePath)).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("opening %s: %w", storePath, err)
	}
	defer r.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", localPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("downloading %s: %w", storePath, err)
	}
	return nil
}

func (s *gcsStore) copy(ctx context.Context, srcPath, dstPath string) error {
	src := s.client.Bucket(s.bucket).Object(s.key(srcPath))
	dst := s.client.Bucket(s.bucket).Object(s.key(dstPath))
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("copying %s to %s: %w", srcPath, dstPath, errObjectNotExist)
		}
		return fmt.Errorf("copying %s to %s: %w", srcPath, dstPath, err)
	}
	return nil
}

func (s *gcsStore) findPathsInRange(ctx context.Context, start, end string, limit int) ([]string, error) {
	query := &storage.Query{
		Prefix:      "",
		StartOffset: s.key(start),
		EndOffset:   s.key(end),
	}
	it := s.client.Bucket(s.bucket).Objects(ctx, query)

	prefix := ""
	if s.prefix != "" {
		prefix = s.prefix + "/"
	}

	var paths []string
	for len(paths) < limit {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing objects in [%s, %s): %w", start, end, err)
		}
		paths = append(paths, strings.TrimPrefix(attrs.Name, prefix))
	}
	return paths, nil
}

func (s *gcsStore) delete(ctx context.Context, paths []string) error {
	for _, p := range paths {
		if err := s.client.Bucket(s.bucket).Object(s.key(p)).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("deleting %s: %w", p, err)
		}
	}
	return nil
}

// parseGCSPath splits a "gs://bucket/object" path into bucket and object.
// object may be empty (path pointing at the bucket root).
func parseGCSPath(gcsPath string) (bucket, object string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path %q: must start with gs://", gcsPath)
	}
	path := strings.TrimPrefix(gcsPath, "gs://")
	bucket, object, _ = strings.Cut(path, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("invalid GCS path %q: missing bucket", gcsPath)
	}
	return bucket, object, nil
}
