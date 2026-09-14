package monitor

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// DefaultErrorLogCapacity is how many recent error messages an ErrorLog
// retains before it starts dropping the oldest.
const DefaultErrorLogCapacity = 2000

// ErrorLogEntry is one recorded error message.
type ErrorLogEntry struct {
	Timestamp time.Time
	Message   string
}

// ErrorLog is an in-memory, append-only, capped log of recent error
// messages, so recent background-poller failures (see RunMonitorLoop) can be
// inspected without digging through process logs. Safe for concurrent use.
type ErrorLog struct {
	mu      sync.Mutex
	cap     int
	entries []ErrorLogEntry
}

// NewErrorLog creates an ErrorLog that retains at most capacity entries,
// dropping the oldest once full.
func NewErrorLog(capacity int) *ErrorLog {
	return &ErrorLog{cap: capacity}
}

// Add formats message and args like log.Printf, logs the result via
// log.Print, and appends it to the in-memory log, evicting the oldest entry
// if at capacity.
func (l *ErrorLog) Add(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Print(msg)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, ErrorLogEntry{Timestamp: time.Now(), Message: msg})
	if len(l.entries) > l.cap {
		l.entries = l.entries[len(l.entries)-l.cap:]
	}
}

// Entries returns a copy of the currently retained entries, oldest first.
func (l *ErrorLog) Entries() []ErrorLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ErrorLogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}
