package kubequeconsume

import (
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

// Globals! How terrible! I feel bad.
// ...but a single process watchdog is a crosscutting
// service akin to logging, so I'm going to go with it.

var watchdogEnabled = false
var notifications chan bool

func watchdogLoop(period time.Duration) {
	failureCount := 0
	for {
		select {
		case <-notifications:
		case <-time.After(period):
			failureCount++
		}
		if failureCount > 0 {
			log.Printf("Too long since main goroutine checked in with watchdog. Dumping all goroutine traces.")
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)

			if failureCount >= 2 {
				panic("Too long since last checkin with watchdog")
			}
		}
	}
}

func EnableWatchdog(period time.Duration) {
	if !watchdogEnabled {
		watchdogEnabled = true
		notifications = make(chan bool, 100)
		go watchdogLoop(period)
	} else {
		log.Printf("Warning: Watchdog already enabled, cannot start again")
	}
}

func NotifyWatchdog() {
	if watchdogEnabled {
		notifications <- true
	}
}

func NotifyUntilComplete(blockingCall func() error) error {
	errorChan := make(chan error)

	blockingCallWrapper := func() {
		err := blockingCall()
		errorChan <- err
	}

	go blockingCallWrapper()

	for {
		// loop forever, pinging the watchdog every second
		select {
		case err := <-errorChan:
			return err
		case <-time.After(time.Second):
			NotifyWatchdog()
		}
	}
}

type NotifyOnWriteWriter struct {
	nested io.Writer
}

func (w *NotifyOnWriteWriter) Write(b []byte) (int, error) {
	NotifyWatchdog()
	return w.nested.Write(b)
}

func NotifyOnWrite(w io.Writer) io.Writer {
	return &NotifyOnWriteWriter{nested: w}
}
