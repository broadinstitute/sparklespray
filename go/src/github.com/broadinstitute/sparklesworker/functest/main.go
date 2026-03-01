package main

import (
	"log"
	"time"

	"github.com/broadinstitute/sparklesworker/watchdog"
)

func main() {
	watchdog.Enable(5 * time.Second)
	i := 0
	for {
		log.Printf("Sleeping for a sec...")
		time.Sleep(time.Second)
		if i < 10 {
			log.Printf("Calling notify")
			watchdog.Notify()
		}
		i++
	}
}
