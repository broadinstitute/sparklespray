package autoscaler

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

	autoscalerPollInterval = 1 * time.Minute
	tier1Interval          = 30 * time.Second
)

// activeTasks is the set of task statuses that are orphaned back to pending when a worker dies.
var activeTasks = []TaskStatus{TaskStatusClaimed, TaskStatusRunning, TaskStatusWriting}

// Autoscaler runs provisioning and watchdog logic against a set of workpools.
type Autoscaler struct {
	clock    scheduler.Clock
	batchAPI BatchAPIClient
	pools    WorkPoolStore
	batches  BatchRequestStore
	workers  WorkerStore
	tasks    TaskStore
	pubsub   PubSubReceiver
}

func New(
	clock scheduler.Clock,
	batchAPI BatchAPIClient,
	pools WorkPoolStore,
	batches BatchRequestStore,
	workers WorkerStore,
	tasks TaskStore,
	pubsub PubSubReceiver,
) *Autoscaler {
	return &Autoscaler{
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
func (a *Autoscaler) RunJobSubmission(ctx context.Context, workpoolID string) error {
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

// RunAutoscalerLoop runs the autoscaler until ctx is cancelled.
// Blocks; run in a dedicated goroutine.
func (a *Autoscaler) RunAutoscalerLoop(ctx context.Context) {
	sched := scheduler.New(a.clock)

	// Autoscaler poll: provisioning. Fixed 1-minute interval; no notification trigger.
	sched.Add(autoscalerPollInterval, autoscalerPollInterval, func() {
		if err := a.runAutoscalerPoll(ctx); err != nil {
			log.Printf("autoscaler poll: %v", err)
		}
	})

	// Tier 1: task recovery. Fixed 30-second interval; no notification trigger.
	sched.Add(tier1Interval, tier1Interval, func() {
		if err := a.runRequeueOrphanedTasks(ctx); err != nil {
			log.Printf("tier1: %v", err)
		}
	})

	// Tier 2: cluster reconciler. Triggered by PubSub notifications for started batches;
	// falls back to max_time_between_polls if no notification arrives.
	notifyTier2 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		if err := a.runClusterReconciler(ctx); err != nil {
			log.Printf("tier2: %v", err)
		}
	})

	// Tier 3: batch startup monitor. Triggered by PubSub notifications for pending batches;
	// same fallback timing as tier 2.
	notifyTier3 := sched.Add(defaultMinTimeBetweenPolls, defaultMaxTimeBetweenPolls, func() {
		if err := a.runBatchStartupMonitor(ctx); err != nil {
			log.Printf("tier3: %v", err)
		}
	})

	// Route PubSub notifications to the appropriate tier in a background goroutine.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batchID := <-a.pubsub.Notifications():
				a.routeNotification(ctx, batchID, notifyTier2, notifyTier3)
			}
		}
	}()

	// Main scheduler loop.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		timerCh, runDue := sched.GetNextCallback()
		select {
		case <-ctx.Done():
			return
		case cb := <-sched.NotifyChannel():
			cb()
		case <-timerCh:
			runDue()
		}
	}
}

// routeNotification looks up a batch and notifies the appropriate tier.
func (a *Autoscaler) routeNotification(ctx context.Context, batchID string, notifyTier2, notifyTier3 func()) {
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
func (a *Autoscaler) checkHaltThreshold(ctx context.Context, pool *WorkPool) error {
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
