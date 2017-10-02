package main

import (
	"log"
	"time"

	"github.com/broadinstitute/kubequeconsume"
)

func main() {
	kubequeconsume.EnableWatchdog(5 * time.Second)
	i := 0
	for {
		log.Printf("Sleeping for a sec...")
		time.Sleep(time.Second)
		if i < 10 {
			log.Printf("Calling notify")
			kubequeconsume.NotifyWatchdog()
		}
		i++
	}
}
