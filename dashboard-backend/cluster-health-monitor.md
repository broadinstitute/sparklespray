# Monitor poll

## Purpose

`Poll` is should runs on a 60 second interval and monitor's cluster size and health.

Poll should be called for a given clusterID. It should read ClusterStatus from datastore at the start and update it at the
end. ClusterStatus is defined (approximately -- please change any field
names to be consistent with existing conventions):

```
interface ClusterStatus {
clusterID : str
lastUpdate: datetime
submittedWorkerRequests: int
shortFailedWorkerRequests: int
otherFailedWorkerRequests: int
completedWorkerRequests: int
seenCompletions: str[]
instanceInUseCount: int
orphanedTaskCount: int
idleInstanceCount: int
runningTaskCount: int
preemptableInstanceCount: int
nonpreemptableInstanceCount: int
}
```

1. query all instances within the cluster's zones, and query all batch jobs
   associated with this cluster as well as all tasks associated with this
   cluster.
2. identify new batch job completions by taking all completed/failed jobs
   and seeing if thier ID is on the seenCompletions list. If not, this is a
   "new" completion. Add the ID onto seenCompletions to make sure it's not
   counted as new on the next pass. (At the same time prune any IDs from
   seenCompletions which did not appear in the lastest list of IDs)
3. For new completions, check to see if it completed < 10 seconds. If so,
   it suggests there's a problem, and increment shortFailedWorkerRequests.
4. Otherwise if it failed, increment otherFailedWorkerRequests. If it was
   reported as a success, increment completedWorkerRequests.
5. match up the VMs with instances by comparing the owner with the instance
   name.
6. instances with an associated task where status = claimed count towards instanceInUseCount
7. instances without any associated status = claimed task count towards idleInstanceCount
8. runningTaskCount is the number of tasks with claimed status
9. orphanedTaskCount is the number of tasks which have status claimed but
   are not associated with a running instance.
10. update lastUpdate field and write cluster status back to datastore
