package dev

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"github.com/urfave/cli"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"cloud.google.com/go/firestore"
)

// firestoreIndexesJSON is the checked-in definition of every composite index
// sprinkles's queries need in a real (non-emulator) Firestore database --
// see firestore.indexes.json, derived by tracing every multi-filter/orderby
// query in monitor/adapters.go and dashboard_backend.go.
//
//go:embed firestore.indexes.json
var firestoreIndexesJSON []byte

type indexFieldSpec struct {
	Field string `json:"field"`
	Order string `json:"order"` // "ASCENDING" or "DESCENDING"
}

type indexSpec struct {
	Collection string           `json:"collection"`
	Fields     []indexFieldSpec `json:"fields"`
}

func runDevBootstrapProject(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")
	region := c.String("region")
	if region == "" {
		return fmt.Errorf("--region is required")
	}
	bucket := c.String("bucket")
	if bucket == "" {
		return fmt.Errorf("--bucket is required")
	}
	serviceAccount := c.String("service-account")
	if serviceAccount == "" {
		return fmt.Errorf("--service-account is required")
	}
	adminUser := c.String("admin-user")
	if adminUser == "" {
		return fmt.Errorf("--admin-user is required")
	}

	ctx := context.Background()

	zones, err := zonesInRegion(ctx, project, region)
	if err != nil {
		return err
	}
	fmt.Printf("zones in %s:        %v\n", region, zones)

	if err := createFirestoreDatabase(ctx, project, db, region); err != nil {
		return err
	}
	if err := createFirestoreIndexes(ctx, project, db); err != nil {
		return err
	}
	if err := createBucket(ctx, project, bucket, region); err != nil {
		return err
	}

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()
	if err := createTopics(ctx, psClient, project); err != nil {
		return err
	}

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	gcsPrefix := fmt.Sprintf("gs://%s/results", bucket)
	sprinklesWorkerGCSPath := fmt.Sprintf("gs://%s/bin/sprinkles-linux-amd64-initial", bucket)
	config := &SprinklesConfig{
		GCSPrefix:             gcsPrefix,
		SubscriberSA:          serviceAccount,
		SprinklesWorkerGCSPath: sprinklesWorkerGCSPath,
		ServiceAccount:        serviceAccount,
		Region:                region,
		Zones:                 zones,
	}
	if err := writeSprinklesConfig(ctx, fsClient, config); err != nil {
		return err
	}

	apiKey, err := addAPIKey(ctx, fsClient, adminUser)
	if err != nil {
		return fmt.Errorf("creating initial API key: %w", err)
	}

	fmt.Println()
	fmt.Println("Project bootstrap complete:")
	fmt.Printf("  Firestore database:    %s (region %s)\n", db, region)
	fmt.Printf("  GCS bucket:            gs://%s\n", bucket)
	fmt.Printf("  gcs_prefix:            %s\n", gcsPrefix)
	fmt.Printf("  sprinkles_worker_gcs_path: %s\n", sprinklesWorkerGCSPath)
	fmt.Printf("  Initial API key for %q: %s\n", adminUser, apiKey)
	fmt.Println()
	fmt.Println("This API key is shown only once -- save it now.")
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("  ./build.sh && ./upload-worker-binary.sh initial gs://%s/bin/sprinkles-linux-amd64-initial\n", bucket)
	fmt.Printf("  sprinkles serve --project %s --db %s\n", project, db)
	fmt.Println("Firestore composite indexes were submitted but build asynchronously --")
	fmt.Println("check status with: gcloud firestore indexes composite list --project=" + project)

	return nil
}

// zonesInRegion returns the names of every zone in region (e.g.
// "us-central1-a", not the full resource URL), used as the default set of
// zones eligible for worker VM placement.
func zonesInRegion(ctx context.Context, project, region string) ([]string, error) {
	computeSvc, err := compute.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating compute client: %w", err)
	}

	r, err := computeSvc.Regions.Get(project, region).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("looking up zones for region %s: %w", region, err)
	}

	zones := make([]string, len(r.Zones))
	for i, zoneURL := range r.Zones {
		zones[i] = path.Base(zoneURL)
	}
	return zones, nil
}

// createFirestoreDatabase creates the named Firestore (Native mode) database,
// tolerating one that already exists. Blocks until creation completes.
func createFirestoreDatabase(ctx context.Context, project, db, region string) error {
	adminClient, err := admin.NewFirestoreAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("creating firestore admin client: %w", err)
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s", project),
		Database: &adminpb.Database{
			LocationId: region,
			Type:       adminpb.Database_FIRESTORE_NATIVE,
		},
		DatabaseId: db,
	})
	if err != nil {
		if grpcstatus.Code(err) == codes.AlreadyExists {
			fmt.Printf("firestore database already exists: %s\n", db)
			return nil
		}
		return fmt.Errorf("creating firestore database %s: %w", db, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for firestore database %s to be created: %w", db, err)
	}
	fmt.Printf("firestore database created: %s\n", db)
	return nil
}

// createFirestoreIndexes submits every composite index in
// firestore.indexes.json, tolerating ones that already exist. Index builds
// happen asynchronously server-side; this does not wait for them to finish.
func createFirestoreIndexes(ctx context.Context, project, db string) error {
	var specs []indexSpec
	if err := json.Unmarshal(firestoreIndexesJSON, &specs); err != nil {
		return fmt.Errorf("parsing embedded firestore.indexes.json: %w", err)
	}

	adminClient, err := admin.NewFirestoreAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("creating firestore admin client: %w", err)
	}
	defer adminClient.Close()

	for _, spec := range specs {
		fields := make([]*adminpb.Index_IndexField, 0, len(spec.Fields))
		for _, f := range spec.Fields {
			order := adminpb.Index_IndexField_ASCENDING
			if f.Order == "DESCENDING" {
				order = adminpb.Index_IndexField_DESCENDING
			}
			fields = append(fields, &adminpb.Index_IndexField{
				FieldPath: f.Field,
				ValueMode: &adminpb.Index_IndexField_Order_{Order: order},
			})
		}

		parent := fmt.Sprintf("projects/%s/databases/%s/collectionGroups/%s", project, db, spec.Collection)
		_, err := adminClient.CreateIndex(ctx, &adminpb.CreateIndexRequest{
			Parent: parent,
			Index: &adminpb.Index{
				QueryScope: adminpb.Index_COLLECTION,
				Fields:     fields,
			},
		})
		if err != nil {
			if grpcstatus.Code(err) == codes.AlreadyExists {
				fmt.Printf("index already exists:  %s %v\n", spec.Collection, spec.Fields)
				continue
			}
			return fmt.Errorf("creating index on %s %v: %w", spec.Collection, spec.Fields, err)
		}
		fmt.Printf("index submitted:       %s %v\n", spec.Collection, spec.Fields)
	}
	return nil
}

// createBucket creates the given GCS bucket, tolerating one that already
// exists (and is already owned by this project).
func createBucket(ctx context.Context, project, bucket, region string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("creating storage client: %w", err)
	}
	defer client.Close()

	err = client.Bucket(bucket).Create(ctx, project, &storage.BucketAttrs{Location: region})
	if err != nil {
		var apiErr *googleapi.Error
		// 409 covers both "bucket already exists" (owned by another project)
		// and "you already own this bucket".
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict {
			fmt.Printf("bucket already exists: gs://%s\n", bucket)
			return nil
		}
		return fmt.Errorf("creating bucket gs://%s: %w", bucket, err)
	}
	fmt.Printf("bucket created:        gs://%s\n", bucket)
	return nil
}
