package cmd

import (
	"github.com/broadinstitute/kubequeconsume"
	"github.com/spf13/cobra"
)

var projectID = ""
var cacheDir = ""
var cluster = ""
var tasksDir = ""
var tasksFile = ""
var port = 0
var bucketDir = ""
var timeout = 5
var restimeout = 10

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().StringVar(&projectID, "project", "", "ID of google project")
	consumeCmd.Flags().StringVar(&cacheDir, "cacheDir", "", "")
	consumeCmd.Flags().StringVar(&cluster, "cluster", "", "")
	consumeCmd.Flags().StringVar(&tasksDir, "tasksDir", "", "")
	consumeCmd.Flags().StringVar(&tasksFile, "tasksFile", "", "")
	consumeCmd.Flags().IntVar(&port, "port", 0, "")
	consumeCmd.Flags().StringVar(&bucketDir, "bucketDir", "", "")
	consumeCmd.Flags().IntVar(&timeout, "timeout", 5, "")
	consumeCmd.Flags().IntVar(&restimeout, "restimeout", 10, "")
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "start a worker which will run sparkles tasks",
	Run: func(cmd *cobra.Command, args []string) {
		kubequeconsume.Consume(projectID, cacheDir, bucketDir,
			cluster, tasksDir, tasksFile,
			port, timeout, restimeout)
	},
}
