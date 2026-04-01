package backend

import (
	"strconv"
	"time"
)

func CreateWorkerCommand(clusterID string, shouldLinger bool, baseArgs []string, aetherConfig *AetherConfig) []string {
	lingerTime := 10 * time.Minute

	cmd := []string{
		"consume", "--cluster", clusterID,
		"--aetherRoot", aetherConfig.Root,
		"--aetherMaxSizeToBundle", strconv.Itoa(int(aetherConfig.MaxSizeToBundle)),
		"--aetherMaxBundleSize", strconv.Itoa(int(aetherConfig.MaxBundleSize)),
		"--aetherWorkers", strconv.Itoa(int(aetherConfig.Workers))}

	cmd = append(cmd, baseArgs...)

	if shouldLinger {
		cmd = append(cmd, "--shutdownAfter", strconv.Itoa(int(lingerTime/time.Second)))
	}

	return cmd
}
