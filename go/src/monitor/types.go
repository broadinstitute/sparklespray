package monitor

type Cluster struct {
	// Immutable identity fields
	uuID              string
	machineType       string
	workerDockerImage string
	workerCommandArgs []string
	pubSubInTopic     string
	pubSubOutTopic    string
	zones             []string

	// Mutable fields managed by the monitor
	maxPreemptableAttempts  int
	maxInstanceCount        int
	usedPreemptableAttempts int
	maxSuspiciousFailures   int

	// Catch-all JSON blob for any state the monitor needs between polls
	monitorState string
}
