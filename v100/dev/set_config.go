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
	config, err := readJSON[SparklesConfig](configFile)
	if err != nil {
		return err
	}

	ctx := context.Background()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	if _, err := fsClient.Collection(sparklesConfigCollection).Doc(sparklesConfigDocID).Set(ctx, *config); err != nil {
		return fmt.Errorf("writing %s/%s to firestore: %w", sparklesConfigCollection, sparklesConfigDocID, err)
	}
	fmt.Printf("%s/%s written\n", sparklesConfigCollection, sparklesConfigDocID)

	return nil
}
