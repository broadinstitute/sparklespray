# Kubeque: Easy submission of batch jobs to kubernetes

# Getting started:

## Prereqs
Google's cloud SDK installed and in your path: https://cloud.google.com/sdk/

## Setting up

Create a google project.  In the below, we'll assume the project name is PROJECT_NAME.

Create a bucket for holding results and uploads.  In the following example, we'll assume the name of the bucket is BUCKET_NAME.

Create a config file "~/.kubeque" or in the current directory containing the following:

```
[config]
cas_url_prefix=gs://BUCKET_NAME/cas/
default_url_prefix=gs://BUCKET_NAME/kubeque/
project=PROJECT_NAME
cluster_name=kubeque-cluster
machine_type=g1-small
default_image=us.gcr.io/PROJECT_NAME/kubeque-example
default_resource_cpu=0.2
default_resource_memory=100M
```

## Create an docker image that your jobs will execute within

Create an image to use by our job:

```
cd examples/docker
./prepare-image PROJECT_NAME
```

## Running jobs

recorded a session as:
https://asciinema.org/a/7rl131knip6g8pkh81yewb9il

Create the cluster:

```
kubeque start 
```

Submit the sample job

```
cd examples/sample-job
kubeque sub -n sample-job python3 '^mandelbrot.py' 0 0 0.5
```

Download the results

```
kubeque fetch sample-job results-from-job
```

Submit multiple parameterized by csv file

```
kubeque sub --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'
kubeque sub --fetch results --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'
```

Resize cluster

```
gcloud container clusters resize kubeque-cluster --size 4
```

## Cleaning up

Once you're done running jobs, you can shut down the cluster.

Stop the cluster:

```
kubeque stop
```

# Notes

## Open issues:

  * Reaper marks tasks as failed when node disappears instead of reseting back to pending
  * If node disappears new pod will be created, but if all tasks are still claimed, then it will exit.  Reaper needs to detect case where job is still alive, but has no running pods.  In such a case, re-submit new kube job.
  * Race condition: sometimes new job submission exit immediately because watch() doesn't see the tasks that were just created.  Add some polling to wait for the expected number of tasks.
  * "status" should have option to get detailed per-task info.  Currently status just gives aggregate counts.  Would be good to get job parameters and status for each job.
  * Add timeline export
  * Add -xml or -json flags so that outputs can programmatically parsed
  * Should we have a command for managing pools?  Currently can do that via kubectl but we have wrappers for everything else.  Perhaps would be moot if autoscaling was enabled

### OOM handling:

Unclear how to properly detect OOM case.  It appears that what happens is OOM killer gets invoked, the child process gets killed, the kubeque-consume script receives that the child process exited with error code 137, and marks that task as complete. 
However, it is true that containerstatus[0].state == "terminated" and containerstatus[0].reason == "OOMKilled" so perhaps this is a race condition where the kubeque-consume is able to update the status of the task but 
ultimately was eventually being terminated due to OOM condition.   Perhaps kubeque-consume can ask if there has been an OOM event since the task started?
It looks like getting this would have to be by asking cAdvisor.  Similar issue:
https://groups.google.com/forum/#!msg/kubernetes-users/MH1sDDwEKZs/zvfqzYSeBAAJ
New solution: kubeque-consume should test for retcode == 137.  That means the child was killed, and in such case we should update the task status as "killed" and then in reaper we should figure out why and update the reason.
Should probably exit from kubeque-consume after child found to exit because it appears the OOMKilled event is recorded on the container level.

That didn't work so well.  Two options: It looks like cAdvisor should be capturing the OOM event, however, cannot figure out how to get it out.
It's worth note that cAdvisor executes outside of pods.  Unclear how heapster knows how to connect to cAdvisor instances.
[update: reading about cAdvisor somewhere else they mention that it only retains the 1 minute's worth of data.  So, perhaps that's why I see no OOM events.  I would need to poll frequently to catch them.]

Anyway, option 2 is we could _assume_ that any kill -9 is a result of OOM and update the task status ourselves as OOM.  May be best in short term.
I'm a little nervous that the OOM could kill the parent and the reaper wouldn't know what to do with that.

Leaning towards checking if return code == -9, then consume should set status to oom-failed.  (And documenting cavets)
Better solution would be after getting return code == -9, poll cAdvisor and ask if there was a new OOM event in this container, and then only then call it oom-failed

### Cannot reserve disk space

No solution for ensuring sufficient diskspace for scheduling:
    - it appears there's no per-pod/container way to require sufficient space.
    - best bet might be to somehow control what type of instance or mounted volume while running.
    - having trouble finding how autoscaler determines which machine type to spawn
        - answer: uses nodepool/template/managed group

For now, probably best to take a parameter from config with diskspace required per node (optionally, could also 
take number of local SSDs to attach.  Local SSD are 375GB each and currently cost 0.11/month, contrast with equiv persistent disk 0.02/month )

### Does not handle case where node disappears
Write service (reaper) which watches pods.  On change, reconcile with tasks.  mark all claimed tasks as "ready" if their owner has disappeared.

Best solution would be to have a kubernetes process run somewhere which polls kube for active nodes and reconcile 
those with the owners in the job table.  

TODO: test, does a failed pod result in a new node with a new name being created, or does the pod keep its name when its created elsewhere?
This could be a problem when detecting failures of claimed tasks.  Would like to use UID but not exposed in downward API.  Can work around by
using volume export and adding uid label, but will be more work.

Add: store job spec submitted to kube in CAS, so that it's trivial to re-submit if kube thinks the original "job" finished.  (which can happen when the queue has no unclaimed items, and a claimed task gets reaped)

### autoscaling
Currently manually enabled and handled out of band.  Also, autoscaling didn't appear to shrink cluster.  (perhaps due to the tiny VMs being used?)
Need to learn more about the default policy used to autoscale.  

* Missing support: Preemptable node support:
tutorial on creating pre-emptable nodepool
https://gist.github.com/rimusz/bd5c9fd9727522fd546329f61d4064a6
So, we should create two nodepools at startup.  One with standard VMs, and a second with preemptable nodes with autoscaling enabled.

Implement only after "un-claiming" works

### multiple job submission
Question: How do we manage multiple jobs running concurrently?   More specifically, do we put anything in to execute them in queued order?
Could use "paralellism" to manage, but definitely not the same as having a priority queue.

Best I can come up with is to periodically update paralellism.  That is, while there are pods which could not be scheduled due to insufficient resources, set parallelism to other jobs to 0 to 
effectively suspend them.

### "peek" command to see live stdout/stderr output
Would be helpful to see stdout/stderr while running.  Where/how to log?  To cloud logs?
What should cli for fetching look like?
Perhaps only keep a trailing log and keep in memory.  Have a redis service per node hold this?  
