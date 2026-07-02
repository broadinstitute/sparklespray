package monitor

import (
	"context"
	"log"
	"time"
)

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
	return nil
}
