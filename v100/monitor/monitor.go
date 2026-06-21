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

	provisioningPollInterval = 5 * time.Second
	tier1Interval          = 30 * time.Second
)

// activeTasks is the set of task statuses that are orphaned back to pending when a worker dies.
var activeTasks = []TaskStatus{TaskStatusClaimed, TaskStatusRunning, TaskStatusWriting}

// Monitor runs provisioning and watchdog logic against a set of workpools.
type Monitor struct {
	clock     scheduler.Clock
	batchAPI  BatchAPIClient
	pools     WorkPoolStore
	batches   BatchRequestStore
	workers   WorkerStore
	tasks     TaskStore
	pubsub    PubSubReceiver
	jobEvents JobEventReceiver
	verbose   bool
}

// SetVerbose enables or disables verbose poll logging.
func (a *Monitor) SetVerbose(v bool) { a.verbose = v }

// SetJobEventReceiver sets an optional receiver for job_created events.
// When set, a new job submission triggers an immediate provisioning poll.
func (a *Monitor) SetJobEventReceiver(r JobEventReceiver) { a.jobEvents = r }

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
) *Monitor {
	return &Monitor{
		clock:    clock,
		batchAPI: batchAPI,
		pools:    pools,
		batches:  batches,
		workers:  workers,
		tasks:    tasks,
		pubsub:   pubsub,
	}
}

// RunJobSubmission updates workpool status when a new job is submitted.
// Should be called from job submission logic before tasks are enqueued.
func (a *Monitor) RunJobSubmission(ctx context.Context, workpoolID string) error {
	pool, err := a.pools.Get(ctx, workpoolID)
	if err != nil {
		return fmt.Errorf("get workpool %s: %w", workpoolID, err)
	}

	if pool.Status == WorkPoolStatusIdle || pool.Status == WorkPoolStatusHalted {
		pool.Status = WorkPoolStatusOK
		pool.StatusMessage = ""
		pool.IncidentCount = 0
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save workpool %s: %w", workpoolID, err)
		}
	}
	return nil
}

// RunMonitorLoop runs the monitor until ctx is cancelled.
// Blocks; run in a dedicated goroutine.
func (a *Monitor) RunMonitorLoop(ctx context.Context) {
	sched := scheduler.New(a.clock)

	// Provisioning poll: provisioning. Triggered by job_created events; falls back to 1-minute timer.
	notifyProvisioning := sched.Add(provisioningPollInterval, provisioningPollInterval, func() {
		a.vlogf("poll: starting provisioning poll")
		if err := a.runProvisioningPoll(ctx); err != nil {
			log.Printf("provisioning poll: %v", err)
		}
	})

	// Tier 1: task recovery. Fixed 30-second interval; no notification trigger.
	sched.Add(tier1Interval, tier1Interval, func() {
		a.vlogf("poll: starting orphaned task requeue")
		if err := a.runRequeueOrphanedTasks(ctx); err != nil {
			log.Printf("tier1: %v", err)
		}
	})

	// Tier 2: cluster reconciler. Triggered by PubSub notifications for started batches;
	// falls back to max_time_between_polls if no notification arrives.
	notifyTier2 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("poll: starting cluster reconciler")
		if err := a.runClusterReconciler(ctx); err != nil {
			log.Printf("tier2: %v", err)
		}
	})

	// Tier 3: batch startup monitor. Triggered by PubSub notifications for pending batches;
	// same fallback timing as tier 2.
	notifyTier3 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		a.vlogf("poll: starting batch startup monitor")
		if err := a.runBatchStartupMonitor(ctx); err != nil {
			log.Printf("tier3: %v", err)
		}
	})

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
					a.vlogf("job events: received job_created for job %s, triggering provisioning poll", n.JobID)
					notifyProvisioning()
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

// recordIncident updates the workpool's status fields for a watchdog anomaly.
// Mutates pool in place; callers must Save the pool after calling this.
func recordIncident(pool *WorkPool, message string, now time.Time) {
	if pool.Status != WorkPoolStatusHalted {
		pool.Status = WorkPoolStatusUnhealthy
	}
	pool.StatusMessage = message
	pool.LastIncidentAt = now
	pool.IncidentCount++
}

// checkHaltThreshold transitions the workpool to halted if the last N classified batches
// all failed. Saves the pool if it transitions.
func (a *Monitor) checkHaltThreshold(ctx context.Context, pool *WorkPool) error {
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
			pool.Status = WorkPoolStatusHalted
			pool.StatusMessage = fmt.Sprintf(
				"Last %d batches all failed — possible configuration problem", n)
			pool.LastIncidentAt = a.clock.Now()
			if err := a.pools.Save(ctx, pool); err != nil {
				return fmt.Errorf("save workpool %s: %w", pool.WorkpoolID, err)
			}
		}
	}
	return nil
}

// param returns v if v != 0, otherwise def. Used to apply per-workpool defaults.
func param[T int | time.Duration](v, def T) T {
	if v != 0 {
		return v
	}
	return def
}
