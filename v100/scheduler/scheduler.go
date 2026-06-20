package scheduler

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can control it deterministically.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// Scheduler manages a set of callbacks each with a minimum and maximum delay between runs.
//
// Each registered callback obeys a leading-edge throttle with trailing coalescing:
//   - The first notify() call fires the callback immediately (leading edge).
//   - Additional notify() calls within minDelay schedule exactly one trailing call.
//   - Further notify() calls while a trailing call is already pending are dropped (coalescing).
//   - Regardless of notifications, the callback runs at least once every maxDelay (fallback).
//
// Intended usage:
//
//	for running {
//	    timerCh, runDue := scheduler.GetNextCallback()
//	    select {
//	    case cb := <-scheduler.NotifyChannel():
//	        cb()
//	    case <-timerCh:
//	        runDue()
//	    }
//	}
type Scheduler struct {
	clock   Clock
	mu      sync.Mutex
	entries []*schedEntry
	ch      chan func()
}

func New(clock Clock) *Scheduler {
	return &Scheduler{
		clock: clock,
		ch:    make(chan func(), 64),
	}
}

// Add registers callback to run between minDelay and maxDelay intervals.
// The returned notify func implements the leading-edge throttle: call it whenever an
// external event suggests the callback should run soon.
func (s *Scheduler) Add(minDelay, maxDelay time.Duration, callback func()) (notify func()) {
	s.mu.Lock()
	e := &schedEntry{
		s:            s,
		minDelay:     minDelay,
		maxDelay:     maxDelay,
		callback:     callback,
		registeredAt: s.clock.Now(),
	}
	s.entries = append(s.entries, e)
	s.mu.Unlock()

	return func() { e.notify(s.clock.Now()) }
}

// NotifyChannel returns the channel on which leading-edge callbacks are delivered.
// Read from this channel in a select alongside the timer returned by GetNextCallback.
func (s *Scheduler) NotifyChannel() <-chan func() {
	return s.ch
}

// GetNextCallback returns a timer channel that fires when the soonest scheduled callback
// is due, and a function to call when it fires. Call this once per loop iteration.
//
// The returned timer channel comes from the injected Clock, so a FakeClock can control
// exactly when it fires in tests.
func (s *Scheduler) GetNextCallback() (<-chan time.Time, func()) {
	s.mu.Lock()
	entries := make([]*schedEntry, len(s.entries))
	copy(entries, s.entries)
	s.mu.Unlock()

	if len(entries) == 0 {
		return nil, func() {}
	}

	now := s.clock.Now()

	var soonest time.Time
	for _, e := range entries {
		e.mu.Lock()
		due := e.nextDue()
		e.mu.Unlock()
		if soonest.IsZero() || due.Before(soonest) {
			soonest = due
		}
	}

	delay := soonest.Sub(now)
	if delay < 0 {
		delay = 0
	}

	timerCh := s.clock.After(delay)

	runDue := func() {
		now := s.clock.Now()
		s.mu.Lock()
		entries := make([]*schedEntry, len(s.entries))
		copy(entries, s.entries)
		s.mu.Unlock()

		for _, e := range entries {
			e.mu.Lock()
			due := e.nextDue()
			shouldRun := !due.After(now) && !e.inFlight
			if shouldRun {
				e.lastRan = now
				e.trailingAt = time.Time{}
			}
			e.mu.Unlock()

			if shouldRun {
				e.callback()
			}
		}
	}

	return timerCh, runDue
}

// schedEntry holds the state for one registered callback.
type schedEntry struct {
	s            *Scheduler
	mu           sync.Mutex
	minDelay     time.Duration
	maxDelay     time.Duration
	callback     func()
	registeredAt time.Time
	lastRan      time.Time // zero = never ran
	trailingAt   time.Time // zero = no trailing call pending
	inFlight     bool      // true while callback is queued in the notify channel
}

// nextDue returns when this entry should next run via the scheduled path.
// Must be called with e.mu held.
func (e *schedEntry) nextDue() time.Time {
	base := e.registeredAt
	if !e.lastRan.IsZero() {
		base = e.lastRan
	}
	maxDue := base.Add(e.maxDelay)

	if !e.trailingAt.IsZero() && e.trailingAt.Before(maxDue) {
		return e.trailingAt
	}
	return maxDue
}

// notify implements the leading-edge throttle with trailing coalescing.
func (e *schedEntry) notify(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.inFlight {
		// Callback is already queued for immediate execution.
		// Ensure we run again after the cooldown expires.
		if e.trailingAt.IsZero() {
			e.trailingAt = now.Add(e.minDelay)
		}
		return
	}

	timeSinceLastRan := now.Sub(e.lastRan)
	if e.lastRan.IsZero() || timeSinceLastRan >= e.minDelay {
		// Leading edge: push to the notify channel for immediate execution.
		e.inFlight = true
		e.trailingAt = time.Time{}

		cb := func() {
			e.callback()
			e.mu.Lock()
			e.lastRan = e.s.clock.Now()
			e.inFlight = false
			e.mu.Unlock()
		}

		select {
		case e.s.ch <- cb:
		default:
			// Channel full: fall back to a trailing call rather than dropping entirely.
			e.inFlight = false
			if e.trailingAt.IsZero() {
				e.trailingAt = now.Add(e.minDelay)
			}
		}
	} else if e.trailingAt.IsZero() {
		// Within cooldown, no trailing call scheduled yet.
		e.trailingAt = e.lastRan.Add(e.minDelay)
	}
	// else: trailing already scheduled — coalesce (do nothing).
}
