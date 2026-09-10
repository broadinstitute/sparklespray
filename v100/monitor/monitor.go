package monitor

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/broadinstitute/sparklespray/v100/scheduler"
)

// Default values for WorkPool watchdog parameters.
const (
	defaultMaxWorkersPerRequest        = 100
	defaultMinTimeBetweenPolls         = 2 * time.Second
	defaultMaxTimeBetweenPolls         = 30 * time.Second
	defaultMaxTimeToStartWorker        = 5 * time.Minute
	defaultMaxTimeInQueue              = 15 * time.Minute
	defaultVMShutdownGracePeriod       = 1 * time.Minute
	defaultMaxZombiesBeforeAbort       = 3
	defaultMaxConsecutiveFailedBatches = 2

	expiryCleanerInterval = 30 * time.Minute

	// defaultHaltCheckWindow bounds how far back checkHaltThreshold looks in
	// the Events log. Kept short (not an all-time window) both to keep the
	// Firestore query cheap and because the failure mode we care about most
	// is "things never start" — a burst of failures right now — not a slow
	// drift over hours.
	defaultHaltCheckWindow = 1 * time.Hour

	// defaultPreemptionLookbackWindow bounds how far back
	// runProvisioningPollForWorkpool looks for zombie incidents (our proxy
	// for preemption) when enforcing MaxPreemptibleWorkerAttempts. A rolling
	// window (rather than a lifetime total) means the preemptible budget
	// recovers over time instead of being permanently consumed by one job.
	defaultPreemptionLookbackWindow = 1 * time.Hour
)

// Incident types recorded on workpool_incident events, distinguishing the
// kind of anomaly recordIncident was called for. IncidentTypeZombie is used
// as a (deliberately imprecise) proxy for "this worker was preempted" when
// enforcing MaxPreemptibleWorkerAttempts — see runProvisioningPollForWorkpool.
const (
	// IncidentTypeZombie: a worker's heartbeat expired without a clean
	// shutdown (runRequeueOrphanedTasks) — crashed, preempted, or otherwise
	// stopped responding.
	IncidentTypeZombie = "zombie"
	// IncidentTypeZombieTerminated: the cluster reconciler found a worker
	// whose heartbeat expired while its VM was still running (per GCP), and
	// terminated the VM.
	IncidentTypeZombieTerminated = "zombie_terminated"
	// IncidentTypeVMStartupFailure: a VM never registered a worker within
	// the startup grace period, while other VMs in the same batch did.
	IncidentTypeVMStartupFailure = "vm_startup_failure"
	// IncidentTypeOverProvisioned: more VMs were found running than the
	// batch expected; the batch was aborted.
	IncidentTypeOverProvisioned = "over_provisioned"
	// IncidentTypeNoWorkersRegistered: no worker ever registered for a
	// batch within the startup grace period; the batch was aborted.
	IncidentTypeNoWorkersRegistered = "no_workers_registered"
	// IncidentTypeTooManyZombies: the number of zombie workers in a batch
	// exceeded MaxZombiesBeforeAbort; the batch was aborted.
	IncidentTypeTooManyZombies = "too_many_zombies"
	// IncidentTypeBatchAPIFailure: GCP's Batch API itself reported the job
	// as failed.
	IncidentTypeBatchAPIFailure = "batch_api_failure"
)

// activeTasks is the set of task statuses that are orphaned back to pending when a worker dies.
var activeTasks = []TaskStatus{TaskStatusClaimed, TaskStatusRunning, TaskStatusWriting}

// Monitor runs provisioning, watchdog, and job-summary bookkeeping logic.
type Monitor struct {
	clock                  scheduler.Clock
	batchAPI               BatchAPIClient
	pools                  WorkPoolStore
	batches                BatchRequestStore
	workers                WorkerStore
	tasks                  TaskStore
	pubsub                 PubSubReceiver
	jobEvents              JobEventReceiver
	jobSummaries           JobSummaryStore
	jobTerminated          JobTerminatedPublisher
	workpoolStatePublisher WorkpoolStatePublisher
	batchOutcomes          BatchOutcomePublisher
	workpoolIncidents      WorkpoolIncidentPublisher
	workerEvents           WorkerEventPublisher
	workPoolSummaries      WorkPoolSummaryStore
	events                 EventStore
	lastEventTime          time.Time
	expiry                 ExpiryStore
	verbose                bool
	dbName                 string
	lingerDuration         time.Duration
	lastActivity           time.Time
}

// SetVerbose enables or disables verbose poll logging.
func (a *Monitor) SetVerbose(v bool) { a.verbose = v }

// SetJobEventReceiver sets an optional receiver for job_created events.
// When set, a new job submission triggers an immediate provisioning poll.
func (a *Monitor) SetJobEventReceiver(r JobEventReceiver) { a.jobEvents = r }

// SetJobSummaryStore sets the store used to read and write JobSummary documents.
func (a *Monitor) SetJobSummaryStore(s JobSummaryStore) { a.jobSummaries = s }

// SetWorkPoolSummaryStore sets the store used to write WorkPoolSummary documents.
func (a *Monitor) SetWorkPoolSummaryStore(s WorkPoolSummaryStore) { a.workPoolSummaries = s }

// SetEventStore sets the store used to query job_created events.
func (a *Monitor) SetEventStore(s EventStore) { a.events = s }

// SetJobTerminatedPublisher sets the publisher used to emit job_terminated events.
func (a *Monitor) SetJobTerminatedPublisher(p JobTerminatedPublisher) { a.jobTerminated = p }

// SetWorkpoolStatePublisher sets the publisher used to emit workpool_state_change events.
func (a *Monitor) SetWorkpoolStatePublisher(p WorkpoolStatePublisher) {
	a.workpoolStatePublisher = p
}

// SetBatchOutcomePublisher sets the publisher used to emit batch_failed/batch_succeeded events.
func (a *Monitor) SetBatchOutcomePublisher(p BatchOutcomePublisher) {
	a.batchOutcomes = p
}

// SetWorkpoolIncidentPublisher sets the publisher used to emit workpool_incident events.
func (a *Monitor) SetWorkpoolIncidentPublisher(p WorkpoolIncidentPublisher) {
	a.workpoolIncidents = p
}

// SetWorkerEventPublisher sets the publisher used to emit worker_stopped events.
func (a *Monitor) SetWorkerEventPublisher(p WorkerEventPublisher) {
	a.workerEvents = p
}

// SetExpiryStore sets the store used to garbage-collect expired documents.
func (a *Monitor) SetExpiryStore(s ExpiryStore) { a.expiry = s }

// SetLingerDuration sets how long the monitor runs without any workpool having
// pending or running tasks before shutting down. Zero means run forever.
func (a *Monitor) SetLingerDuration(d time.Duration) { a.lingerDuration = d }

// vlogf logs only when verbose mode is on.
func (a *Monitor) vlogf(format string, args ...any) {
	if a.verbose {
		log.Printf(format, args...)
	}
}

func New(
	clock scheduler.Clock,
	batchAPI BatchAPIClient,
	pools WorkPoolStore,
	batches BatchRequestStore,
	workers WorkerStore,
	tasks TaskStore,
	pubsub PubSubReceiver,
	dbName string,
) *Monitor {
	return &Monitor{
		clock:    clock,
		batchAPI: batchAPI,
		pools:    pools,
		batches:  batches,
		workers:  workers,
		tasks:    tasks,
		pubsub:   pubsub,
		dbName:   dbName,
	}
}

// RunMonitorLoop runs the monitor until ctx is cancelled.
// Blocks; run in a dedicated goroutine.
func (a *Monitor) RunMonitorLoop(ctx context.Context) {
	var lastActivityPrinted time.Time

	sched := scheduler.New(a.clock)

	if a.lingerDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		a.lastActivity = time.Now()
		sched.Add(defaultMinTimeBetweenPolls, defaultMinTimeBetweenPolls, func() {
			if lastActivityPrinted != a.lastActivity {
				a.vlogf("Last activity at %s. Will shut down if no activity before %s", a.lastActivity.Format(time.RFC3339), a.lastActivity.Add(a.lingerDuration).Format(time.RFC3339))
				lastActivityPrinted = a.lastActivity
			}
			now := time.Now()
			// if it's been too long since the monitor did anything, shutdown
			if now.Sub(a.lastActivity) > a.lingerDuration {
				cancel()
			}
		})
		defer cancel()
	}

	// Cluster reconciler: Triggered by PubSub notifications for started batches;
	// falls back to max_time_between_polls if no notification arrives.
	notifyClusterReconciler := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Started: Reconciling our records against google's")
		if err := a.runClusterReconciler(ctx); err != nil {
			log.Printf("cluster reconciler: %v", err)
		}
		a.vlogf("Completed: Reconciling our records against google's")
	})

	// Provisioner: Responsible for requesting VMs for pending tasks. Triggered by job_created events; falls back to 1-minute timer.
	notifyProvisioning := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Started: Checking for workpools which need new workers")
		if err := a.runProvisioningPoll(ctx); err != nil {
			log.Printf("provisioning poll: %v", err)
		}
		a.vlogf("Completed: Checking for workpools which need new workers")
	})

	// Task recovery: Fixed 30-second interval; no notification trigger.
	sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Started: Checking for ophaned jobs")
		if err := a.runRequeueOrphanedTasks(ctx); err != nil {
			log.Printf("task recovery: %v", err)
		}
		a.vlogf("Completed: Checking for ophaned jobs")
	})

	// Batch startup monitor: Triggered by PubSub notifications for pending batches; and fallback timer
	notifyBatchStartupMonitor := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Started: Checking to see if workers successfully starting")
		if err := a.runBatchStartupMonitor(ctx); err != nil {
			log.Printf("batch startup monitor: %v", err)
		}
		a.vlogf("Completed: Checking to see if workers successfully starting")
	})

	// Expiry cleaner: deletes documents whose expiry field is in the past. Fixed 30-minute interval.
	if a.expiry != nil {
		sched.Add(expiryCleanerInterval, expiryCleanerInterval, func() {
			a.vlogf("Deleting expired objects from firestore...")
			if err := a.runExpiryCleaner(ctx); err != nil {
				log.Printf("expiry cleaner: %v", err)
			}
			a.vlogf("Deleting expired objects from firestore complete")
		})
	}

	// Job summary poll: recomputes JobSummary for every non-terminal job.
	// Triggered by job_created and task_state_update events; falls back to max interval.
	var notifyJobSummary func()
	notifyJobSummary = sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Starting: Computing job summaries")
		if err := a.runJobSummaryPoll(ctx); err != nil {
			log.Printf("job summary poll: %v", err)
		}
		a.vlogf("Completed: Computing job summaries")
	})

	// WorkPool summary poll: recomputes WorkPoolSummary for every workpool.
	// Triggered by batch notifications and job events; falls back to max interval.
	var notifyWorkPoolSummary func()
	notifyWorkPoolSummary = sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Starting: Compute workpool summaries")
		if err := a.runWorkPoolSummaryPoll(ctx); err != nil {
			log.Printf("workpool summary poll: %v", err)
		}
		a.vlogf("Completed: Compute workpool summaries")
	})

	// Route PubSub notifications to the appropriate pollers in a background goroutine.
	// A notification with Err set means the receive loop failed fatally; propagate it.
	fatalErrCh := make(chan error, 1)

	if a.jobEvents != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case n := <-a.jobEvents.JobEvents():
					if n.Err != nil {
						log.Printf("job events: fatal error: %v", n.Err)
						fatalErrCh <- n.Err
						return
					}
					a.vlogf("job events: received %s event for job %s", n.EventType, n.JobID)
					if n.EventType == "job_created" || n.EventType == "workpool_state_change" {
						log.Printf("Waking up provision check due to %s event", string(n.EventType))
						notifyProvisioning()
					}
					if notifyJobSummary != nil {
						log.Printf("Waking up job summarizer due to %s event", string(n.EventType))
						notifyJobSummary()
					}
					if notifyWorkPoolSummary != nil {
						log.Printf("Waking up workpool summarizer due to %s event", string(n.EventType))
						notifyWorkPoolSummary()
					}
				}
			}
		}()
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-a.pubsub.Notifications():
				if n.Err != nil {
					log.Printf("pubsub: fatal error: %v", n.Err)
					fatalErrCh <- n.Err
					return
				}
				a.vlogf("pubsub: received notification for batch %s", n.BatchID)
				a.routeNotification(ctx, n.BatchID, notifyClusterReconciler, notifyBatchStartupMonitor)
				if notifyWorkPoolSummary != nil {
					notifyWorkPoolSummary()
				}
			}
		}
	}()

	// Main scheduler loop.
	for {
		select {
		case <-ctx.Done():
			return
		case <-fatalErrCh:
			return
		default:
		}

		timerCh, runDue := sched.GetNextCallback()
		select {
		case <-ctx.Done():
			return
		case <-fatalErrCh:
			return
		case cb := <-sched.NotifyChannel():
			cb()
		case <-timerCh:
			runDue()
		}
	}
}

// routeNotification looks up a batch and notifies the appropriate poller.
func (a *Monitor) routeNotification(ctx context.Context, batchID string, notifyClusterReconciler, notifyBatchStartupMonitor func()) {
	batch, err := a.batches.Get(ctx, batchID)
	if err != nil {
		log.Printf("notification: failed to look up batch %s: %v — notifying both the cluster reconciler and the batch startup monitor", batchID, err)
		notifyClusterReconciler()
		notifyBatchStartupMonitor()
		return
	}

	switch batch.Status {
	case BatchStatusPending:
		notifyBatchStartupMonitor()
	case BatchStatusStarted:
		notifyClusterReconciler()
		// Completed/failed batches need no notification-driven check.
	}
}

// saveState persists workpool state and publishes a workpool_state_change
// event carrying message. Publish errors are logged but not returned so
// they don't block state writes.
func (a *Monitor) saveState(ctx context.Context, state *WorkPoolState, message string) error {
	if err := a.pools.SaveState(ctx, state); err != nil {
		return err
	}
	if a.workpoolStatePublisher != nil {
		if err := a.workpoolStatePublisher.PublishWorkpoolStateChange(ctx, state.WorkpoolID, string(state.State), message); err != nil {
			log.Printf("saveState: publish workpool_state_change for %s: %v", state.WorkpoolID, err)
		}
	}
	return nil
}

// recordIncident publishes a workpool_incident event for a watchdog anomaly.
// It is purely a log-to-Events operation — it does not mutate WorkPoolState.
// The ok/unhealthy transition is decided by the WorkPool summary poll
// (updateWorkPoolSummary), which derives it (along with the
// StateMessage/LastIncidentAt/IncidentCount shown on WorkPoolSummary) from
// recent workpool_incident events — see EventStore.ListRecentWorkpoolIncidents.
func (a *Monitor) recordIncident(ctx context.Context, workpoolID, incidentType, message string) {
	if a.workpoolIncidents != nil {
		if err := a.workpoolIncidents.PublishWorkpoolIncident(ctx, workpoolID, incidentType, message); err != nil {
			log.Printf("recordIncident: publish workpool_incident for %s: %v", workpoolID, err)
		}
	}
}

// markBatchFailed records batch as failed (GCP itself reported the job as
// failed), logs an incident, publishes a batch_failed event, and checks the
// halt threshold. Use markBatchTerminated instead when the monitor is the
// one deciding to kill an otherwise-live job.
func (a *Monitor) markBatchFailed(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, reason string, now time.Time) error {
	return a.markBatchDone(ctx, ws, batch, BatchStatusFailed, IncidentTypeBatchAPIFailure, reason, now)
}

// markBatchTerminated records batch as terminated (the monitor's own
// bookkeeping — VM counts, worker registrations, heartbeats — found an
// anomaly and killed a job GCP hadn't reported any problem with), sets
// TerminationReason, logs an incident, publishes a batch_failed event (the
// same outcome event type as markBatchFailed — both mean "this batch didn't
// work out" for halt-counting purposes), and checks the halt threshold.
func (a *Monitor) markBatchTerminated(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, incidentType, reason string, now time.Time) error {
	batch.TerminationReason = reason
	return a.markBatchDone(ctx, ws, batch, BatchStatusTerminated, incidentType, reason, now)
}

// markBatchDone centralizes the pattern repeated across
// cluster_reconciler.go and batch_startup_monitor.go's failure-detection
// call sites: set batch.Status/Unhealthy, save it, log an incident, publish
// a batch_failed event, and check the halt threshold.
func (a *Monitor) markBatchDone(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, status BatchStatus, incidentType, reason string, now time.Time) error {
	batch.Status = status
	batch.Unhealthy = true
	a.recordIncident(ctx, ws.Pool.WorkpoolID, incidentType, reason)
	if err := a.batches.Save(ctx, batch); err != nil {
		return fmt.Errorf("save batch: %w", err)
	}
	if a.batchOutcomes != nil {
		if err := a.batchOutcomes.PublishBatchFailed(ctx, ws.Pool.WorkpoolID, reason); err != nil {
			log.Printf("markBatchDone: publish batch_failed for workpool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return a.checkHaltThreshold(ctx, ws.Pool, ws.State)
}

// checkHaltThreshold transitions the workpool to halted if the last N batch
// outcomes recorded in the Events log within defaultHaltCheckWindow all
// failed. Saves state if it transitions. The halt transition is reported
// only via workpool_state_change — it is not itself published as a
// workpool_incident (that event type is reserved for the anomalies that led
// up to the halt).
func (a *Monitor) checkHaltThreshold(ctx context.Context, pool *WorkPool, state *WorkPoolState) error {
	n := pool.MaxConsecutiveFailedBatches
	if n <= 0 {
		n = defaultMaxConsecutiveFailedBatches
	}

	since := a.clock.Now().Add(-defaultHaltCheckWindow)
	recent, err := a.events.ListRecentBatchOutcomes(ctx, pool.WorkpoolID, since)
	if err != nil {
		return fmt.Errorf("list recent batch outcomes for workpool %s: %w", pool.WorkpoolID, err)
	}

	// ListRecentBatchOutcomes returns most-recent-first; take only the most recent n.
	if len(recent) > n {
		recent = recent[:n]
	}

	if len(recent) == n {
		allFailed := true
		for _, o := range recent {
			if !o.Failed {
				allFailed = false
				break
			}
		}
		if allFailed {
			state.State = WorkPoolStatusHalted
			haltMessage := fmt.Sprintf(
				"Last %d batches all failed within the last hour — possible configuration problem", n)
			if err := a.saveState(ctx, state, haltMessage); err != nil {
				return fmt.Errorf("save workpool %s: %w", pool.WorkpoolID, err)
			}
		}
	}
	return nil
}

// RunRequeueOrphanedTasks runs one task-recovery pass: any tasks owned by workers
// whose heartbeats have expired are reset to pending. Exported for functional tests.
func (a *Monitor) RunRequeueOrphanedTasks(ctx context.Context) error {
	return a.runRequeueOrphanedTasks(ctx)
}

// RunJobSummaryPoll runs one job-summary pass: recomputes status for every
// non-terminal job and publishes job_terminated events as needed.
// Exported for functional tests; requires SetJobSummaryStore to have been called.
func (a *Monitor) RunJobSummaryPoll(ctx context.Context) error {
	return a.runJobSummaryPoll(ctx)
}

// param returns v if v != 0, otherwise def. Used to apply per-workpool defaults.
func param[T int | time.Duration](v, def T) T {
	if v != 0 {
		return v
	}
	return def
}
