Things to fix:

1. Collect a history of metrics so that when metrics are requested we can immediately see the last n minutes worth

(in progress: testing yielded werid things. Added code to clean up old jobs to see if it was an artifact of old jobs. Maybe switch to submit new jobs all the time.)

Units on cpu breakdown are reported as % but are clearly in 20k

problem:
Resubmitting with same name kills cluster ... but does not wait causing one job to get picked up and then orphaned.

2. Big one: revisit how we poll in the browser. Do we like the listen to a subscription model? Should we instead try leveraging firestore listeners and make mulitple pushes.
3. worker pool summary on the right is not correctly identifing workers which are running. Why?
4. potentially large: tasks and jobs should have a UUID which can be used for caching to avoid current confusion arising from resubmissions
5. Job cancellation
6. Simulate memory exhaustion
7. Migrate API to firestore

When running 5 tasks, the plot shows the task count as 1 until 2nd task starts and then it's 2, etc.
Timeline is appears to be rendered from job start until last event, but we really want present time.
There's something super weird with cpu time. Sometimes it's reported as zero?

Merge process memory and system memory sections. For system memory report total, availible and resident sizes. Drop process count plot

Log is not updating.
