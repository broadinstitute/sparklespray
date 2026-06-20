package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	minDelay = 5 * time.Second
	maxDelay = 5 * time.Minute
)

// drainNotify reads one callback from the notify channel without blocking.
func drainNotify(s *Scheduler) (func(), bool) {
	select {
	case cb := <-s.NotifyChannel():
		return cb, true
	default:
		return nil, false
	}
}

// stepLoop runs one iteration: advances the clock by advanceBy, then fires whichever
// of the notify channel or the scheduled timer is ready. Returns true if the notify
// channel fired, false if the scheduled timer fired.
func stepLoop(s *Scheduler, clock *FakeClock, advanceBy time.Duration) bool {
	timerCh, runDue := s.GetNextCallback()
	clock.Advance(advanceBy)
	select {
	case cb := <-s.NotifyChannel():
		cb()
		return true
	case <-timerCh:
		runDue()
		return false
	}
}

func TestLeadingEdgeFirstCallImmediate(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	notify := s.Add(minDelay, maxDelay, func() { calls++ })

	notify()

	cb, ok := drainNotify(s)
	require.True(t, ok, "expected callback in notify channel after leading-edge notify")
	cb()
	assert.Equal(t, 1, calls)
}

func TestSecondNotifyWithinCooldownSchedulesTrailing(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	notify := s.Add(minDelay, maxDelay, func() { calls++ })

	// Leading edge at t=0.
	notify()
	cb, _ := drainNotify(s)
	cb()

	// Second notify at t=1s — within the minDelay cooldown.
	clock.Advance(1 * time.Second)
	notify()

	_, ok := drainNotify(s)
	assert.False(t, ok, "second notify within cooldown should not immediately queue a callback")

	// Advance to minDelay from lastRan — trailing call is now due.
	timerCh, runDue := s.GetNextCallback()
	clock.Advance(minDelay)
	select {
	case <-timerCh:
		runDue()
	default:
		t.Fatal("trailing call did not fire when due")
	}

	assert.Equal(t, 2, calls)
}

func TestAdditionalNotifiesWithinCooldownAreCoalesced(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	notify := s.Add(minDelay, maxDelay, func() { calls++ })

	// Leading edge.
	notify()
	cb, _ := drainNotify(s)
	cb()

	// Three more notifies within the cooldown — all coalesce into one trailing call.
	clock.Advance(1 * time.Second)
	notify()
	clock.Advance(1 * time.Second)
	notify()
	clock.Advance(1 * time.Second)
	notify()

	timerCh, runDue := s.GetNextCallback()
	clock.Advance(minDelay)
	select {
	case <-timerCh:
		runDue()
	default:
		t.Fatal("trailing call should have fired")
	}

	_, ok := drainNotify(s)
	assert.False(t, ok, "no further callbacks should be pending")

	assert.Equal(t, 2, calls, "exactly two runs: leading edge + one coalesced trailing")
}

func TestMaxDelayFiresWithoutNotification(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	s.Add(minDelay, maxDelay, func() { calls++ })

	timerCh, runDue := s.GetNextCallback()
	clock.Advance(maxDelay)
	select {
	case <-timerCh:
		runDue()
	default:
		t.Fatal("maxDelay fallback did not fire")
	}

	assert.Equal(t, 1, calls)
}

func TestMaxDelayResetsAfterEachRun(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	s.Add(minDelay, maxDelay, func() { calls++ })

	timerCh, runDue := s.GetNextCallback()
	clock.Advance(maxDelay)
	<-timerCh
	runDue()
	assert.Equal(t, 1, calls)

	timerCh, runDue = s.GetNextCallback()
	clock.Advance(maxDelay)
	<-timerCh
	runDue()
	assert.Equal(t, 2, calls)
}

func TestNotifyAfterCooldownIsLeadingEdgeAgain(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	notify := s.Add(minDelay, maxDelay, func() { calls++ })

	// Leading edge at t=0.
	notify()
	cb, _ := drainNotify(s)
	cb()

	// Advance well past minDelay, then notify again — should be a new leading edge.
	clock.Advance(minDelay * 2)
	notify()

	cb, ok := drainNotify(s)
	require.True(t, ok, "notify after cooldown should fire immediately as leading edge")
	cb()

	assert.Equal(t, 2, calls)
}

func TestMultipleEntriesSoonestDueFirst(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	callsA, callsB := 0, 0
	s.Add(minDelay, 10*time.Second, func() { callsA++ })
	s.Add(minDelay, 30*time.Second, func() { callsB++ })

	// At t=10s: only A is due (maxDelay=10s). B is not due until t=30s.
	timerCh, runDue := s.GetNextCallback()
	clock.Advance(10 * time.Second)
	<-timerCh
	runDue()
	assert.Equal(t, 1, callsA, "entry A should have run")
	assert.Equal(t, 0, callsB, "entry B should not have run yet")

	// At t=30s: B is due (30s from registration). A is also due again (10s from lastRan=t10).
	timerCh, runDue = s.GetNextCallback()
	clock.Advance(20 * time.Second)
	<-timerCh
	runDue()
	assert.Equal(t, 2, callsA, "entry A should have run again")
	assert.Equal(t, 1, callsB, "entry B should have run")
}

func TestInFlightLeadingEdgeSchedulesTrailingForLateNotify(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	calls := 0
	notify := s.Add(minDelay, maxDelay, func() { calls++ })

	// Leading edge in flight (not yet consumed from channel).
	notify()

	// Second notify before the first callback is consumed.
	clock.Advance(1 * time.Second)
	notify()

	// Consume the leading-edge callback.
	cb, ok := drainNotify(s)
	require.True(t, ok)
	cb()
	assert.Equal(t, 1, calls)

	// The second notify should have scheduled a trailing call.
	timerCh, runDue := s.GetNextCallback()
	clock.Advance(minDelay)
	<-timerCh
	runDue()
	assert.Equal(t, 2, calls)
}

func TestStepLoopIntegration(t *testing.T) {
	clock := NewFakeClock(epoch)
	s := New(clock)

	var log []string
	notify := s.Add(minDelay, maxDelay, func() { log = append(log, "A") })
	s.Add(minDelay, maxDelay, func() { log = append(log, "B") })

	// Notification fires on leading edge via the notify channel.
	notify()
	notified := stepLoop(s, clock, 0)
	assert.True(t, notified)
	assert.Equal(t, []string{"A"}, log)

	// Advance past both maxDelays — scheduled timer fires both.
	log = nil
	stepLoop(s, clock, maxDelay)
	assert.ElementsMatch(t, []string{"A", "B"}, log)
}
