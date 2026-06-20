package scheduler

import "time"

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production clock implementation.
var RealClock Clock = realClock{}
