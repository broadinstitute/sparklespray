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
	defaultMinTimeBetweenPolls         = 5 * time.Second
	defaultMaxTimeBetweenPolls         = 5 * time.Minute
	defaultMaxTimeToStartWorker        = 5 * time.Minute
	defaultMaxTimeInQueue              = 15 * time.Minute
	defaultVMShutdownGracePeriod       = 1 * time.Minute
	defaultMaxZombiesBeforeAbort       = 3
	defaultMaxConsecutiveFailedBatches = 2

	expiryCleanerInterval = 30 * time.Minute

	jobSummaryMinInterval = 1 * time.Second
	jobSummaryMaxInterval = 5 * time.Minute

	workPoolSummaryMinInterval = 1 * time.Second
	workPoolSummaryMaxInterval = 5 * time.Minute
)

// activeTasks is the set of task statuses that are orphaned back to pending when a worker dies.
var activeTasks = []TaskStatus{TaskStatusClaimed, TaskStatusRunning, TaskStatusWriting}

// Monitor runs provisioning, watchdog, and job-summary bookkeeping logic.
type Monitor struct {
	clock          scheduler.Clock
	batchAPI       BatchAPIClient
	pools          WorkPoolStore
	batches        BatchRequestStore
	workers        WorkerStore
	tasks          TaskStore
	pubsub         PubSubReceiver
	jobEvents      JobEventReceiver
	jobSummaries         JobSummaryStore
	jobTerminated        JobTerminatedPublisher
	workpoolStatePublisher WorkpoolStatePublisher
	workPoolSummaries    WorkPoolSummaryStore
	expiry            ExpiryStore
	verbose        bool
	dbName         string
	lingerDuration time.Duration
	lastActivity   time.Time
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

// SetJobTerminatedPublisher sets the publisher used to emit job_terminated events.
func (a *Monitor) SetJobTerminatedPublisher(p JobTerminatedPublisher) { a.jobTerminated = p }

// SetWorkpoolStatePublisher sets the publisher used to emit workpool_state_change events.
func (a *Monitor) SetWorkpoolStatePublisher(p WorkpoolStatePublisher) {
	a.workpoolStatePublisher = p
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

// RunJobSubmission updates workpool status when a new job is submitted.
// Should be called from job submission logic before tasks are enqueued.
func (a *Monitor) RunJobSubmission(ctx context.Context, workpoolID string) error {
	ws, err := a.pools.Get(ctx, workpoolID)
	if err != nil {
		return fmt.Errorf("get workpool %s: %w", workpoolID, err)
	}

	if ws.State.State == WorkPoolStatusIdle || ws.State.State == WorkPoolStatusHalted {
		ws.State.State = WorkPoolStatusOK
		ws.State.StateMessage = ""
		ws.State.IncidentCount = 0
		if err := a.saveState(ctx, ws.State); err != nil {
			return fmt.Errorf("save workpool %s: %w", workpoolID, err)
		}
	}
	return nil
}

// RunMonitorLoop runs the monitor until ctx is cancelled.
// Blocks; run in a dedicated goroutine.
func (a *Monitor) RunMonitorLoop(ctx context.Context) {

	sched := scheduler.New(a.clock)

	if a.lingerDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		a.lastActivity = time.Now()
		sched.Add(defaultMinTimeBetweenPolls, defaultMinTimeBetweenPolls, func() {
			now := time.Now()
			// if it's been too long since the monitor did anything, shutdown
			if now.Sub(a.lastActivity) > a.lingerDuration {
				cancel()
			}
		})
		defer cancel()
	}

	// Tier 2: cluster reconciler. Triggered by PubSub notifications for started batches;
	// falls back to max_time_between_polls if no notification arrives.
	notifyTier2 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("poll: starting cluster reconciler")
		if err := a.runClusterReconciler(ctx); err != nil {
			log.Printf("tier2: %v", err)
		}
	})

	// Provisioning poll: provisioning. Triggered by job_created events; falls back to 1-minute timer.
	notifyProvisioning := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Checking for workpools which need new workers")
		if err := a.runProvisioningPoll(ctx); err != nil {
			log.Printf("provisioning poll: %v", err)
		}
	})

	// Tier 1: task recovery. Fixed 30-second interval; no notification trigger.
	sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Checking for ophaned jobs")
		if err := a.runRequeueOrphanedTasks(ctx); err != nil {
			log.Printf("tier1: %v", err)
		}
	})

	// Tier 3: batch startup monitor. Triggered by PubSub notifications for pending batches;
	// same fallback timing as tier 2.
	notifyTier3 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("Checking to see if workers successfully starting")
		if err := a.runBatchStartupMonitor(ctx); err != nil {
			log.Printf("tier3: %v", err)
		}
	})

	// Expiry cleaner: deletes documents whose expiry field is in the past. Fixed 30-minute interval.
	if a.expiry != nil {
		sched.Add(expiryCleanerInterval, expiryCleanerInterval, func() {
			a.vlogf("poll: starting expiry cleaner")
			if err := a.runExpiryCleaner(ctx); err != nil {
				log.Printf("expiry cleaner: %v", err)
			}
		})
	}

	// Job summary poll: recomputes JobSummary for every non-terminal job.
	// Triggered by job_created and task_state_update events; falls back to max interval.
	var notifyJobSummary func()
	if a.jobSummaries != nil {
		notifyJobSummary = sched.Add(jobSummaryMinInterval, jobSummaryMaxInterval, func() {
			a.vlogf("poll: starting job summary poll")
			if err := a.runJobSummaryPoll(ctx); err != nil {
				log.Printf("job summary poll: %v", err)
			}
		})
	}

	// WorkPool summary poll: recomputes WorkPoolSummary for every workpool.
	// Triggered by batch notifications and job events; falls back to max interval.
	var notifyWorkPoolSummary func()
	if a.workPoolSummaries != nil {
		notifyWorkPoolSummary = sched.Add(workPoolSummaryMinInterval, workPoolSummaryMaxInterval, func() {
			a.vlogf("poll: starting workpool summary poll")
			if err := a.runWorkPoolSummaryPoll(ctx); err != nil {
				log.Printf("workpool summary poll: %v", err)
			}
		})
	}

	// Route PubSub notifications to the appropriate tier in a background goroutine.
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
					if n.EventType == "job_created" {
						notifyProvisioning()
					}
					if notifyJobSummary != nil {
						notifyJobSummary()
					}
					if notifyWorkPoolSummary != nil {
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
				a.routeNotification(ctx, n.BatchID, notifyTier2, notifyTier3)
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

// routeNotification looks up a batch and notifies the appropriate tier.
func (a *Monitor) routeNotification(ctx context.Context, batchID string, notifyTier2, notifyTier3 func()) {
	batch, err := a.batches.Get(ctx, batchID)
	if err != nil {
		log.Printf("notification: failed to look up batch %s: %v — notifying both tiers", batchID, err)
		notifyTier2()
		notifyTier3()
		return
	}

	switch batch.Status {
	case BatchStatusPending:
		notifyTier3()
	case BatchStatusStarted:
		notifyTier2()
		// Completed/failed batches need no notification-driven check.
	}
}

// saveState persists workpool state and publishes a workpool_state_change event.
// Publish errors are logged but not returned so they don't block state writes.
func (a *Monitor) saveState(ctx context.Context, state *WorkPoolState) error {
	if err := a.pools.SaveState(ctx, state); err != nil {
		return err
	}
	if a.workpoolStatePublisher != nil {
		if err := a.workpoolStatePublisher.PublishWorkpoolStateChange(ctx, state.WorkpoolID, string(state.State), state.StateMessage); err != nil {
			log.Printf("saveState: publish workpool_state_change for %s: %v", state.WorkpoolID, err)
		}
	}
	return nil
}

// recordIncident updates the workpool state for a watchdog anomaly.
// Mutates state in place; callers must SaveState after calling this.
func recordIncident(state *WorkPoolState, message string, now time.Time) {
	if state.State != WorkPoolStatusHalted {
		state.State = WorkPoolStatusUnhealthy
	}
	state.StateMessage = message
	state.LastIncidentAt = now
	state.IncidentCount++
}

// checkHaltThreshold transitions the workpool to halted if the last N classified batches
// all failed. Saves state if it transitions.
func (a *Monitor) checkHaltThreshold(ctx context.Context, pool *WorkPool, state *WorkPoolState) error {
	n := pool.MaxConsecutiveFailedBatches
	if n <= 0 {
		n = defaultMaxConsecutiveFailedBatches
	}

	// Pending batches are excluded — they haven't been classified yet.
	recent, err := a.batches.ListByWorkpool(ctx, pool.WorkpoolID, []BatchStatus{
		BatchStatusFailed, BatchStatusStarted, BatchStatusCompleted,
	})
	if err != nil {
		return fmt.Errorf("list recent batches for workpool %s: %w", pool.WorkpoolID, err)
	}

	// ListByWorkpool returns DESC by SubmittedAt; take only the most recent n.
	if len(recent) > n {
		recent = recent[:n]
	}

	if len(recent) == n {
		allFailed := true
		for _, b := range recent {
			if b.Status != BatchStatusFailed {
				allFailed = false
				break
			}
		}
		if allFailed {
			state.State = WorkPoolStatusHalted
			state.StateMessage = fmt.Sprintf(
				"Last %d batches all failed — possible configuration problem", n)
			state.LastIncidentAt = a.clock.Now()
			if err := a.saveState(ctx, state); err != nil {
				return fmt.Errorf("save workpool %s: %w", pool.WorkpoolID, err)
			}
		}
	}
	return nil
}

// RunRequeueOrphanedTasks runs one tier-1 pass: any tasks owned by workers
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
