package tempspace

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

const (
	defaultAggressionFactor = 10
	defaultMaxDeleteCount   = 50
)

// Tempspace implements the self-expiring-CAS protocol (see
// self-expiring-CAS.md) on top of an ObjStore. All keys managed by a given
// Tempspace share the same interval.
type Tempspace struct {
	store            ObjStore
	interval         time.Duration
	aggressionFactor int
	maxDeleteCount   int

	// randFloat64 returns a value in [0, 1); overridable in tests to make
	// amortized GC triggering deterministic.
	randFloat64 func() float64
}

func newTempspace(store ObjStore, interval time.Duration) *Tempspace {
	return &Tempspace{
		store:            store,
		interval:         interval,
		aggressionFactor: defaultAggressionFactor,
		maxDeleteCount:   defaultMaxDeleteCount,
		randFloat64:      rand.Float64,
	}
}

// intervalSeconds and intervalIndex implement the path/bucketing scheme from
// self-expiring-CAS.md: path = "{interval}/{interval_index % 1e6}/{name}".
func (t *Tempspace) intervalSeconds() int64 {
	return int64(t.interval.Seconds())
}

func (t *Tempspace) intervalIndexAt(when time.Time) int64 {
	return when.Unix() / t.intervalSeconds()
}

func (t *Tempspace) pathFor(intervalIndex int64, name string) string {
	return fmt.Sprintf("%d/%06d/%s", t.intervalSeconds(), intervalIndex%1_000_000, name)
}

// GetPathIfExists returns the current-semispace path for name. If name is
// found in the current semispace, or in the previous semispace (in which
// case it is refreshed forward by copying it into the current semispace),
// found is true. The returned path is always the current semispace path,
// regardless of whether the file was found, so the caller can pass it to
// Put if a write is needed.
func (t *Tempspace) GetPathIfExists(ctx context.Context, name string) (path string, found bool, err error) {
	now := time.Now()
	currentIndex := t.intervalIndexAt(now)
	currentPath := t.pathFor(currentIndex, name)

	exists, err := t.store.exists(ctx, currentPath)
	if err != nil {
		return currentPath, false, err
	}
	if exists {
		return currentPath, true, nil
	}

	prevPath := t.pathFor(currentIndex-1, name)
	prevExists, err := t.store.exists(ctx, prevPath)
	if err != nil {
		return currentPath, false, err
	}
	if !prevExists {
		return currentPath, false, nil
	}

	// Refresh: copy the previous-semispace file forward into the current
	// semispace so it survives another interval.
	if err := t.store.copy(ctx, prevPath, currentPath); err != nil {
		if errors.Is(err, errObjectNotExist) {
			// Raced with a concurrent deletion.
			return currentPath, false, nil
		}
		return currentPath, false, err
	}
	return currentPath, true, nil
}

// Put uploads localPath to destPath, which must be a path previously
// returned by GetPathIfExists, then triggers amortized GC.
func (t *Tempspace) Put(ctx context.Context, localPath, destPath string) error {
	if err := t.amortizedGC(ctx); err != nil {
		return err
	}
	if err := t.store.put(ctx, localPath, destPath); err != nil {
		return err
	}
	return nil
}

// gc performs a bounded sweep, deleting up to maxDeleteCount keys strictly
// older than the previous interval.
func (t *Tempspace) gc(ctx context.Context) error {
	now := time.Now()
	currentIndex := t.intervalIndexAt(now)

	start := t.pathFor(0, "")
	end := t.pathFor(currentIndex-1, "")

	paths, err := t.store.findPathsInRange(ctx, start, end, t.maxDeleteCount)
	if err != nil {
		return fmt.Errorf("gc: listing stale keys: %w", err)
	}
	if len(paths) == 0 {
		return nil
	}
	if err := t.store.delete(ctx, paths); err != nil {
		return fmt.Errorf("gc: deleting stale keys: %w", err)
	}
	return nil
}

// amortizedGC fires gc() with probability aggressionFactor/maxDeleteCount,
// so that deletions proceed (in expectation) aggressionFactor times faster
// than writes, without scanning storage on every put.
func (t *Tempspace) amortizedGC(ctx context.Context) error {
	p := float64(t.aggressionFactor) / float64(t.maxDeleteCount)
	if t.randFloat64() >= p {
		return nil
	}
	return t.gc(ctx)
}
