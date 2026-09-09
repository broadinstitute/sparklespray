package tempspace

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeObjStore is a minimal in-memory ObjStore, keyed by path, sufficient to
// exercise Tempspace's public behavior without talking to real GCS.
type fakeObjStore struct {
	blobs map[string]string // path -> contents (we just store the "localPath" string given to put/copy)
}

func newFakeObjStore() *fakeObjStore {
	return &fakeObjStore{blobs: map[string]string{}}
}

func (f *fakeObjStore) exists(ctx context.Context, path string) (bool, error) {
	_, ok := f.blobs[path]
	return ok, nil
}

func (f *fakeObjStore) put(ctx context.Context, localPath, storePath string) error {
	f.blobs[storePath] = localPath
	return nil
}

func (f *fakeObjStore) get(ctx context.Context, storePath, localPath string) error {
	return nil
}

func (f *fakeObjStore) copy(ctx context.Context, srcPath, dstPath string) error {
	v, ok := f.blobs[srcPath]
	if !ok {
		return errObjectNotExist
	}
	f.blobs[dstPath] = v
	return nil
}

func (f *fakeObjStore) findPathsInRange(ctx context.Context, start, end string, limit int) ([]string, error) {
	var paths []string
	for p := range f.blobs {
		if p >= start && p < end {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	if len(paths) > limit {
		paths = paths[:limit]
	}
	return paths, nil
}

func (f *fakeObjStore) delete(ctx context.Context, paths []string) error {
	for _, p := range paths {
		delete(f.blobs, p)
	}
	return nil
}

func newTestTempspace(store ObjStore, interval time.Duration) *Tempspace {
	ts := newTempspace(store, interval)
	// Disable amortized GC probability by default; individual tests that
	// want to exercise GC set randFloat64 explicitly.
	ts.randFloat64 = func() float64 { return 1 }
	return ts
}

func TestGetPathIfExists_FreshNameThenPut(t *testing.T) {
	ctx := context.Background()
	store := newFakeObjStore()
	ts := newTestTempspace(store, time.Hour)

	path, found, err := ts.GetPathIfExists(ctx, "myfile")
	require.NoError(t, err)
	require.False(t, found)
	require.True(t, strings.HasSuffix(path, "/myfile"))

	require.NoError(t, ts.Put(ctx, "/local/myfile", path))

	path2, found2, err := ts.GetPathIfExists(ctx, "myfile")
	require.NoError(t, err)
	require.True(t, found2)
	require.Equal(t, path, path2)
}

func TestGetPathIfExists_RefreshesFromPreviousSemispace(t *testing.T) {
	ctx := context.Background()
	store := newFakeObjStore()
	ts := newTestTempspace(store, time.Hour)

	// Simulate a write made one interval ago by writing directly to the
	// previous-semispace path.
	now := time.Now()
	prevIndex := ts.intervalIndexAt(now) - 1
	prevPath := ts.pathFor(prevIndex, "myfile")
	require.NoError(t, store.put(ctx, "/local/myfile", prevPath))

	path, found, err := ts.GetPathIfExists(ctx, "myfile")
	require.NoError(t, err)
	require.True(t, found)

	currentIndex := ts.intervalIndexAt(now)
	require.Equal(t, ts.pathFor(currentIndex, "myfile"), path)

	// The file should now also exist at the returned (current) path, since
	// it was refreshed forward.
	exists, err := store.exists(ctx, path)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGetPathIfExists_TooOldIsNotFound(t *testing.T) {
	ctx := context.Background()
	store := newFakeObjStore()
	ts := newTestTempspace(store, time.Hour)

	now := time.Now()
	staleIndex := ts.intervalIndexAt(now) - 2
	stalePath := ts.pathFor(staleIndex, "myfile")
	require.NoError(t, store.put(ctx, "/local/myfile", stalePath))

	_, found, err := ts.GetPathIfExists(ctx, "myfile")
	require.NoError(t, err)
	require.False(t, found)

	// gc() should be willing to remove it, since it's strictly older than
	// the previous interval.
	require.NoError(t, ts.gc(ctx))
	exists, err := store.exists(ctx, stalePath)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPut_AmortizedGCDeletesStaleKeys(t *testing.T) {
	ctx := context.Background()
	store := newFakeObjStore()
	ts := newTempspace(store, time.Hour)
	ts.randFloat64 = func() float64 { return 0 } // force amortized GC to fire

	now := time.Now()
	staleIndex := ts.intervalIndexAt(now) - 2
	stalePath := ts.pathFor(staleIndex, "old")
	require.NoError(t, store.put(ctx, "/local/old", stalePath))

	path, _, err := ts.GetPathIfExists(ctx, "new")
	require.NoError(t, err)
	require.NoError(t, ts.Put(ctx, "/local/new", path))

	exists, err := store.exists(ctx, stalePath)
	require.NoError(t, err)
	require.False(t, exists, "stale key should have been swept by amortized GC triggered on Put")
}
