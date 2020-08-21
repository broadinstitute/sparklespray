package cmd

import (
	"github.com/broadinstitute/kubequeconsume"
	"github.com/spf13/cobra"
)

var projectID string
var cacheDir string
var cluster string
var tasksDir string
var tasksFile string
var port int
var bucketDir string
var timeout int
var watchdogTimeoutMinutes int
var islocal bool

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().StringVar(&projectID, "project", "", "ID of google project")
	consumeCmd.Flags().StringVar(&cacheDir, "cacheDir", "", "")
	consumeCmd.Flags().StringVar(&cluster, "cluster", "", "")
	consumeCmd.Flags().StringVar(&tasksDir, "tasksDir", "", "")
	consumeCmd.Flags().StringVar(&tasksFile, "tasksFile", "", "")
	consumeCmd.Flags().IntVar(&port, "port", 0, "")
	consumeCmd.Flags().StringVar(&bucketDir, "bucketDir", "", "")
	consumeCmd.Flags().IntVar(&timeout, "timeout", 10, "")
	consumeCmd.Flags().IntVar(&watchdogTimeoutMinutes, "restimeout", 5, "")
	consumeCmd.Flags().BoolVar(&islocal, "local", false, "")
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "start a worker which will run sparkles tasks",
	Run: func(cmd *cobra.Command, args []string) {
		kubequeconsume.Consume(projectID, cacheDir, bucketDir,
			cluster, tasksDir, tasksFile,
			port, timeout, watchdogTimeoutMinutes, islocal)
	},
}
