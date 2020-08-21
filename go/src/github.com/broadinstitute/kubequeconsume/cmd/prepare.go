package cmd

import (
	"log"

	"github.com/broadinstitute/kubequeconsume"
	"github.com/spf13/cobra"
)

var prepBucketDir = ""
var buckets []string = nil
var gcsfuseExecutable = ""
var injectConsumeExe = ""

func init() {
	rootCmd.AddCommand(prepareCmd)

	prepareCmd.Flags().StringVar(&prepBucketDir, "bucketDir", "", "where to mount")
	prepareCmd.Flags().StringVar(&gcsfuseExecutable, "gscfuseexe", "gcsfuse", "where to mount")
	prepareCmd.Flags().StringSliceVar(&buckets, "bucket", []string{}, "Name of buckets to mount")
	prepareCmd.Flags().StringVar(&injectConsumeExe, "cpexe", "", "path to copy executable")
}

var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "set up gcsfuse mounts",
	Run: func(cmd *cobra.Command, args []string) {
		err := kubequeconsume.Prepare(gcsfuseExecutable, prepBucketDir, buckets, injectConsumeExe)
		if err != nil {
			log.Fatalf("Got error in prepare(%s, %s, %s, %v): %v", gcsfuseExecutable, prepBucketDir, buckets, injectConsumeExe, err)
		}
	},
}
