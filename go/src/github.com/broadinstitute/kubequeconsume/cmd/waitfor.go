package cmd

import (
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var maxWait = 0

func init() {
	rootCmd.Flags().IntVar(&maxWait, "max", 30, "Number of seconds to wait before giving up")
	rootCmd.AddCommand(waitforCmd)
}

var waitforCmd = &cobra.Command{
	Use:   "waitfor",
	Short: "waits for a file to appear",
	Run: func(cmd *cobra.Command, args []string) {
		filename := args[0]

		start := time.Now()
		for {
			_, err := os.Stat(filename)
			if err == nil {
				break
			} else {
				elapsed := time.Now().Sub(start)
				if elapsed >= time.Duration(maxWait)*time.Second {
					log.Fatalf("exhausted time waiting for %s to appear", filename)
				}

				if os.IsNotExist(err) {
					log.Printf("%s does not exist. Sleeping...", filename)
					time.Sleep(time.Second)
				} else {
					log.Fatalf("Got error checking for %s: %v", filename, err)
				}
			}
		}
	},
}
