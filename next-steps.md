1. update end-to-end to to run using pubsub simulator and firestore simulator. (Is that a good idea?)
2. Maybe write unit tests for pubsub and firestore functionality using simulator?
3. Run happy path test on google cloud

Remaining gap:

- submission of autoscaler batch job

still punting on topics like ergonomic commands. How to get project set up, etc.

---

I think there's a test for caching behavior.
Remaining gap: lease should renew. Need support in aether for renewing lease on filesystem

---

in prefect we could write a method:

# populates db with a single job and a set of associated tasks

jobId, taskIds = map_and_submit(sparkles_job_definition, args)

# starts the monitor running.

future = startJob(jobId)

# runs a "watch" command per task ID. Each of these tasks wouldn't really be

# doing anything, just streaming in log output and blocking until task

# completes.

task_futures = map(watchTask, taskIds)

# let the monitor block until the queue is drained

future.wait()

# now we can read all the task futures

gather(task_futures)

This model doesn't allow us to leverage prefect caching, because the tasks
don't actually do any work.

Incrementally adding to a job isn't ideal. Perhaps we can flip this around
such that we register tasks that should run?

we'd need something like:
task result = get_from_cache(task_key)
if not result:
add task to tasks_to_run
start tasks

I don't know if I can fit that into prefect's model. Alternative is one task
per job. Would be a better fit for prefect's model. Probably where we should
start.

Decision: Do the dumb/simple thing: Just target one task per job and submit
each independently. So, prefect task should:

1. submit json as a job+sparkles task
2. wait for completion, listening to pubsub channel and printing log output

---

Big picture: what is the goal?

- Replace existing sparkles?
- add UI?
- Job submission backend for perfect (yes)
  - single task or batch of tasks execution?
  - What is going to be the first run? Daintree? genome workflows?
  - maybe trial with a simple partition, scatter, gather workflow
  - set a task cap of 10k?

---

aether changes:
All external APIs should consistenly use prefixed names:

- lease:...
- root:...
- anything else is assumed to be a named root (should we validate the names of named roots? Definitely don't want ":" to be present)
- new method ExtendLease(name, expiration) (A little strange, but probably should accept either a root name ("root:" prefixed) or a named lease. If named lease, it adds a new lease iff its > old lease. If anonymous, add a new lease)

sparkles

- add support for starting autoscalar via batch

  - create cluster (if necessary)
  - submit job
  - if autoscalar job does not exist
    - Do one autoscale poll to create the first round of jobs: 1 linger node and N additional
    - Submit autoscale job (small node)
    - (Make autoscale command have a timeout where if nothing is running any more for N seconds, shutdown)
  - Wait for autoscaler to report successful startup
  - If no acknowledgement of successful startup after 20 minutes, abort, shutting down everything

- finish getting monitor logging (also add timestamp to log and resource usage messages)
- implement mock batch API:
  - should spawn a local worker. (maybe within a goroutine?) Requests with additional volume mounts should fail(?)
- Add to claimed worker batch job name
- Goal: make submit a full test
  - should start mock autoscaler
  - shut down autoscaler when job completes

Caching behavior:
compute default cache key = root + docker image + command
(custom key: Allow cache key to be computed from a subset of above + any parameters provided)
Add a flag to tasks (taken_from_cache)

New collection:
CacheEntries(expiry, key, outputRoot, logsRoot)
when running a task, check for existing CacheEntry. If exists, update task with outputs and setting flag
if not, run task and then create CacheEntry.

What would a UI for sparkles job look like?

- List of workers with status
- List of tasks with status. Stacked bar over time as UI?
- A filterable table showing task status + parameters
- Allow for drilling in and seeing live logs + resource traces
- prefect artifact
