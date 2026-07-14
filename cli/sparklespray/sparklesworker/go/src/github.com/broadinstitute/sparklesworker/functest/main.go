package main

import (
	"log"
	"time"

	"github.com/broadinstitute/sparklesworker"
)

func main() {
	sparklesworker.EnableWatchdog(5 * time.Second)
	i := 0
	for {
		log.Printf("Sleeping for a sec...")
		time.Sleep(time.Second)
		if i < 10 {
			log.Printf("Calling notify")
			sparklesworker.NotifyWatchdog()
		}
		i++
	}
}
