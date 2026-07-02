package dev

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/urfave/cli"
)

func runDevCleanExpired(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	now := time.Now()
	if c.Bool("expire-all") {
		now = now.AddDate(10, 0, 0)
	}

	ctx := context.Background()
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	store := monitor.NewFirestoreExpiryStore(fsClient)
	deleted, errs := monitor.CleanExpired(ctx, store, now)
	for coll, n := range deleted {
		log.Printf("deleted %d expired documents from %s", n, coll)
	}
	for coll, err := range errs {
		log.Printf("%s: %v", coll, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to clean %d collection(s)", len(errs))
	}
	return nil
}
