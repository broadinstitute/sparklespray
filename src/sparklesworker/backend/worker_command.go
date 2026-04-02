package backend

import (
	"strconv"
)

func CreateWorkerCommand(cluster *Cluster, shouldLinger bool, baseArgs []string) []string {
	if cluster.SparklesDir == "" {
		panic("nil SparklesDir")
	}
	aetherConfig := cluster.AetherConfig
	cmd := []string{
		"consume", "--cluster", cluster.ClusterID,
		"--dir", cluster.SparklesDir,
		"--aetherRoot", aetherConfig.Root,
		"--aetherMaxSizeToBundle", strconv.Itoa(int(aetherConfig.MaxSizeToBundle)),
		"--aetherMaxBundleSize", strconv.Itoa(int(aetherConfig.MaxBundleSize)),
		"--aetherWorkers", strconv.Itoa(int(aetherConfig.Workers))}

	cmd = append(cmd, baseArgs...)

	if shouldLinger {
		cmd = append(cmd, "--shutdownAfter", strconv.Itoa(cluster.MaxLingerSeconds))
	}

	return cmd
}
