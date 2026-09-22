package dev

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/urfave/cli"
)

func runDevExport(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	collection := c.Args().First()
	if collection == "" {
		return fmt.Errorf("collection name required as first argument")
	}

	ctx := context.Background()
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	q := fsClient.Collection(collection).Query
	for _, f := range c.StringSlice("filter") {
		parts := strings.SplitN(f, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--filter %q: expected field=value", f)
		}
		q = q.Where(parts[0], "==", parts[1])
	}

	docs, err := q.Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("querying %s: %w", collection, err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	for _, doc := range docs {
		m := doc.Data()
		m["_id"] = doc.Ref.ID
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "(%d documents)\n", len(docs))
	return nil
}
