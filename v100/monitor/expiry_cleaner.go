package monitor

import (
	"context"
	"fmt"
	"log"
	"time"
)

// idleWorkPoolSummaryMaxAge is how long an idle WorkPoolSummary may go without
// an update before it is considered stale and deleted.
const idleWorkPoolSummaryMaxAge = 30 * time.Minute

// collectionsWithExpiry lists every Firestore collection that carries an expiry field.
var collectionsWithExpiry = []string{
	workpoolCollection,
	workerCollection,
	jobCollection,
	taskCollection,
	batchRequestCollection,
	jobSummaryCollection,
	jobSummaryHistoryCollection,
	workPoolSummaryCollection,
	workPoolSummaryHistoryCollection,
	eventsCollection,
	taskLogCollection,
}

// CleanExpired deletes expired documents from every collection that carries an expiry
// field, using store.DeleteExpired with the given now. It returns the number of
// documents deleted per collection, and any per-collection errors encountered; a
// failure on one collection does not stop the others from being processed.
func CleanExpired(ctx context.Context, store ExpiryStore, now time.Time) (deleted map[string]int, errs map[string]error) {
	deleted = make(map[string]int, len(collectionsWithExpiry))
	errs = make(map[string]error)
	for _, coll := range collectionsWithExpiry {
		n, err := store.DeleteExpired(ctx, coll, now)
		if err != nil {
			errs[coll] = err
			continue
		}
		deleted[coll] = n
	}
	return deleted, errs
}

func (a *Monitor) runExpiryCleaner(ctx context.Context) error {
	deleted, errs := CleanExpired(ctx, a.expiry, a.clock.Now())
	for coll, err := range errs {
		log.Printf("expiry cleaner: %s: %v", coll, err)
	}
	for coll, n := range deleted {
		if n > 0 {
			a.vlogf("expiry cleaner: deleted %d expired documents from %s", n, coll)
		}
	}

	if a.workPoolSummaries != nil {
		if err := a.cleanIdleWorkPoolSummaries(ctx); err != nil {
			log.Printf("expiry cleaner: %v", err)
		}
	}

	return nil
}

// cleanIdleWorkPoolSummaries deletes WorkPoolSummary documents for idle
// workpools that haven't been updated in over idleWorkPoolSummaryMaxAge.
func (a *Monitor) cleanIdleWorkPoolSummaries(ctx context.Context) error {
	summaries, err := a.workPoolSummaries.ListIdle(ctx)
	if err != nil {
		return fmt.Errorf("list idle workpool summaries: %w", err)
	}

	cutoff := a.clock.Now().Add(-idleWorkPoolSummaryMaxAge)
	for _, s := range summaries {
		if s.LastUpdated.After(cutoff) {
			continue
		}
		if err := a.workPoolSummaries.Delete(ctx, s.WorkpoolID); err != nil {
			log.Printf("expiry cleaner: delete idle workpool summary %s: %v", s.WorkpoolID, err)
			continue
		}
		a.vlogf("expiry cleaner: deleted idle workpool summary %s (last updated %s)", s.WorkpoolID, s.LastUpdated.Format(time.RFC3339))
	}
	return nil
}
