package monitor

import (
	"context"
	"log"
)

// collectionsWithExpiry lists every Firestore collection that carries an expiry field.
var collectionsWithExpiry = []string{
	workpoolCollection,
	workerCollection,
	jobSummaryCollection,
	jobSummaryHistoryCollection,
	workPoolSummaryCollection,
	workPoolSummaryHistoryCollection,
	eventsCollection,
	taskLogCollection,
}

func (a *Monitor) runExpiryCleaner(ctx context.Context) error {
	now := a.clock.Now()
	for _, coll := range collectionsWithExpiry {
		n, err := a.expiry.DeleteExpired(ctx, coll, now)
		if err != nil {
			log.Printf("expiry cleaner: %s: %v", coll, err)
			continue
		}
		if n > 0 {
			a.vlogf("expiry cleaner: deleted %d expired documents from %s", n, coll)
		}
	}
	return nil
}
