package autoscaler

// messages from Batch API
type BatchAPITaskStateChanged struct {
	jobUID   string
	taskName string
	taskUID  string
	newState string
}

type BatchAPIJobStateChanged struct {
	jobUID   string
	jobName  string
	newState string
}

// messages from sparkles CLI

// submitted after job is created (by "sub")
type SparklesJobCreated struct {
	jobName string
}

// submitted after job is modified (by "kill", "resize")
type SparklesJobUpdated struct {
	jobName string
}

type SparklesTaskUpdated struct {
	taskID string
}

// // summary state
type SparklesJobSummary struct {
	perState map[string][]string // a map of (state, exit_code) -> list of taskIds in that state
}

type SparklesJobNodeReqCounts struct {
	jobName                   string
	preemptNodesRequested     int
	nonpreemptNodesRequested  int
	preemptNodesFailed        int
	nonpreemptNodesFailed     int
	preemptNodesTerminated    int
	nonpreemptNodesTerminated int
}

type SparklesBatchAPITaskSummary struct {
	runningTaskUIDs []string // list of all running tasks
}

// on SparklesTaskUpdate (from worker and client):
// 	update perState

// on SparklesJobUpdated
// resize_cluster(max_nodes, )
//. in the event that the size has increased or decreased
//  compute number of nodes that we want based on job fields
//  compute number of nodes requested / running
//  if there are more desired, create them.
//. (should there be a ramp up? Would require trying again after a minute or two)

// on BatchAPITaskStateChanged
// recently_died_tasks = update_batch_api_task_summary(taskID)
//   handle_orphaned_tasks(recently_died_tasks)
//   resize_cluster()

// phase 1: find tasks which are marked as claimed, but are not actually running
//.  find all claimed tasks

// baby step:
//   migrate watch logic from python to golang. Use pubsub to submit a poll request. Maybe compute a cached state and store/fetch that to minimize on traffic
