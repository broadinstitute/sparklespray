package dev

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/urfave/cli"
)

func runSetConfig(c *cli.Context) error {
	args := c.Args()
	configFile := args.Get(0)
	if configFile == "" {
		return fmt.Errorf("config json file path is required")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	return setConfig(configFile, project, c.String("db"))
}

func setConfig(configFile, project, db string) error {
	config, err := readJSON[SprinklesConfig](configFile)
	if err != nil {
		return err
	}

	ctx := context.Background()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	return writeSprinklesConfig(ctx, fsClient, config)
}

// writeSprinklesConfig writes config to SprinklesConfig/default. Exported for
// reuse by both "dev set-config" (which reads config from a JSON file) and
// "dev bootstrap-project" (which builds config directly from flags).
func writeSprinklesConfig(ctx context.Context, fsClient *firestore.Client, config *SprinklesConfig) error {
	if _, err := fsClient.Collection(sprinklesConfigCollection).Doc(sprinklesConfigDocID).Set(ctx, *config); err != nil {
		return fmt.Errorf("writing %s/%s to firestore: %w", sprinklesConfigCollection, sprinklesConfigDocID, err)
	}
	fmt.Printf("%s/%s written\n", sprinklesConfigCollection, sprinklesConfigDocID)
	return nil
}
