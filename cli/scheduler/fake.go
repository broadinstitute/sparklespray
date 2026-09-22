package scheduler

import (
	"sync"
	"time"
)

// FakeClock is a manually-advanced clock for use in tests.
type FakeClock struct {
	mu      sync.Mutex
	current time.Time
	waiters []fakeWaiter
}

type fakeWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{current: start}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *FakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := f.current.Add(d)
	if d <= 0 {
		ch <- f.current
	} else {
		f.waiters = append(f.waiters, fakeWaiter{deadline: deadline, ch: ch})
	}
	return ch
}

// Advance moves the clock forward by d, firing any waiters whose deadline has passed.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.current = f.current.Add(d)
	now := f.current
	remaining := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.deadline.After(now) {
			w.ch <- now
		} else {
			remaining = append(remaining, w)
		}
	}
	f.waiters = remaining
	f.mu.Unlock()
}
