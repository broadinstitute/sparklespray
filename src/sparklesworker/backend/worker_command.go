package backend

import (
	"strconv"
)

func CreateWorkerCommand(clusterID string, shouldLinger bool, baseArgs []string, aetherConfig *AetherConfig) []string {
	cmd := []string{
		"bin/sparklesworker", "consume", "--cluster", clusterID,
		"--aetherRoot", aetherConfig.Root,
		"--aetherMaxSizeToBundle", strconv.Itoa(int(aetherConfig.MaxSizeToBundle)),
		"--aetherMaxBundleSize", strconv.Itoa(int(aetherConfig.MaxBundleSize)),
		"--aetherWorkers", strconv.Itoa(int(aetherConfig.Workers))}

	cmd = append(cmd, baseArgs...)

	if shouldLinger {
		cmd = append(cmd, "--shutdownAfter", strconv.Itoa(60*15))
	}

	return cmd
}
