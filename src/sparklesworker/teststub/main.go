package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	// batchClient, err := batch.NewClient(ctx)
	// if err != nil {
	// 	log.Printf("err %s", err)
	// }

	projectID := "test-sparkles-2"
	// location := "us-central1"
	// it := batchClient.ListJobs(ctx, &batchpb.ListJobsRequest{Parent: fmt.Sprintf("projects/%s/locations/%s", projectID, location)})
	// log.Printf("iterator %v", it)
	// for {
	// 	job, isDone := it.Next()
	// 	if isDone == iterator.Done {
	// 		break
	// 	}
	// 	log.Printf("job %v err %v", job, isDone)
	// 	if job != nil {
	// 		log.Printf("job name %s", job.Name)
	// 	}
	// 	break
	// }

	// resp, err := batchClient.GetJob(ctx, &batchpb.GetJobRequest{Name: "sparkles-autoscaler"})
	// log.Printf("last err %s", err)
	// log.Printf("resp %v", resp)

	var instanceNames []string

	clusterID := "c-231719e6ca246bd8"
	filter := fmt.Sprintf(`labels.sparkles-cluster-uuid=%q`, clusterID)

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		panic(fmt.Errorf("creating compute client: %w", err))
	}

	for _, zone := range []string{"us-central1-c"} {
		req := &computepb.ListInstancesRequest{
			Project: projectID,
			Zone:    zone,
			Filter:  &filter,
		}

		it := instancesClient.List(ctx, req)
		for {
			instance, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				panic(fmt.Errorf("listing instances in zone %s: %w", zone, err))
			}
			instanceNames = append(instanceNames, instance.GetName())
		}
	}

	log.Printf("instances: %s", strings.Join(instanceNames, ", "))
}
