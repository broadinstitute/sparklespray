# Kubeque: Easy submission of batch jobs to kubernetes

"Kubernetes is an open-source system for automating deployment, scaling, and management of containerized applications."

(See also https://kubernetes.io/ )

kubeque is a command line tool to make it easy to manage a kubernetes cluster via googles GKE service and submit adhoc batch jobs that cluster. 


# Getting started:

## Overview

To run a process or batch of processes via kubeque, you will need to:

1. Create a kubernetes cluster (kubeque start)
2. Create a docker image and upload into a repository that the cluster can access.   
3. Submit the actual job (kubeque sub ...)
4. You then may optionally download the output or leave it in google storage for later.  (kubeque fetch ...) 

When you're all done, you may want to remove the cluster entirely (kubeque stop)

## Prereqs
Google's cloud SDK installed and in your path: https://cloud.google.com/sdk/

run `gcloud init` to finsh sdk setup and then `gcloud components install kubectl`
to install the kubernetes command line tools.

install kubectl via `gcloud components update kubectl`

Then to provide your google credentials to kubeque, thus giving it the access needed to submit jobs, run:
 
```
gcloud auth login
gcloud auth application-default login
```


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

or
```
docker build . -t us.gcr.io/broad-achilles/demeter
gcloud docker push us.gcr.io/broad-achilles/demeter
```

GKE is simple to use with the docker repository within GKE.  To push to that repo, my buil

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

# Submitting along with multiple files that are needed by job

Files can automatically be uploaded from your local host on submission, and will be downloaded to the working directory before your job starts.  You can specify what files you'd like uploaded with the "-u" option.

For example:

```
kubeque sub -n sample-job -u mandelbrot.py python3 mandelbrot.py 0 0 0.5
```

will upload the latest mandelbrot.py and download it onto the remote machine before execution starts.   It's worth noting that this is equvilient to:
```
kubeque sub -n sample-job python3 '^mandelbrot.py' 0 0 0.5
```

If you have many files that your job depends on, it may be easier to list the files in a seperate file (one filename per line) and upload all of the files by specifying '-u @file_list'

If a directory is specified then each file within that directory will be uploaded.
When files are downloaded onto the remote node, they are always placed within the current working directory.  You can override that behavior by appending ":destination_path" onto the end of the filename.

For example "-u /users/pgm/foo" will be stored on the execution host in "./foo".     However, if you specify the file as '-u /users/pgm/foo:pgm/foo' then it will be stored in ./pgm/foo

# Simulating a submission by running it locally

The following will do all the upload data and bookkeeping normally done for jobs, but will not actually create a kubernetes job to run it.  Instead, after
all data is uploaded, it will print the equivilent docker command which you 
can run locally to simulate execution.  This can be helpful for debugging issues.

```
kubeque sub --skipkube python3 '^mandelbrot.py' 0 0 0.5
```

Submit a sample job reserving 1G of memory

```
kubeque sub -r memory=1G -n sample-job python3 '^mandelbrot.py' 0 0 0.5
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

Note: after fetching, each task's output directory will have a
`parameters.json` file which contains the key/value pairs that were used
when generating the command.

Add another class of machines for use

```
kubeque add-node-pool n-standard-16
```

Resize cluster

```
gcloud container clusters resize kubeque-cluster --size 4
```

## Cleaning up

Kubeque remembers jobs until you explicitly remove them.   To remove all non-running jobs:

```
kubeque remove "*"
```

If there were jobs that got stuck with some "claimed" tasks, you can reset the claimed status to pending via:
```
kubeque reset "*"
```

After which rerunning `kubeque remove` should clean out those jobs.

Once you're done running jobs, you can shut down the cluster.

Stop the cluster:

```
kubeque stop
```

## Debugging

You can view the kubernetes dashboard to see what's running/status of everything starting a proxy on your local machine:

```
kubectl proxy
```

Then, you can go to http://127.0.0.1:8001/ui to view the dashboard.

You can also ssh into any host by looking up its name (on the google console or via gcloud tool) and 
 
```
cloud compute ssh gke-kubeque-cluster-default-pool-9d31348f-wfqh
```

Alternatively you can connect to a specific container.  Find the name of the name of the container via `kubeque status --detailed`  
You can then spawn a process within that container and look around

```
kubectl exec 20161202-231034-334e-j5nja -i -t -- bash -il
```

You can find the working directory of the task in `/tmp/task-*`

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

### cluster monitoring

We should probably report on the cluster size while jobs are running.  Two sources of this information:
    GCP # of instances in managed groups (need to find way to go from cluster -> managed groups containing associated node pools)
    Kubernettes reports # of nodes in the system
    
### Google authentication

We should create a command "init" which prompts for a project and creates a
.kubeque file in the working directory.   Extra bonus points for creating a
credential file.  Credential file should be referenced in config file.

`client = Client.from_service_account_json('/path/to/keyfile.json')` should
be used to create client which uses stored credentials.

might be complicated to switch gcloud to use custom credentials.  Looks like
requires a complex process of using ` gcloud auth activate-service-account `
to remember credentials, create a custom config `gcloud config` and then
provide a --configuration flag for each command.   Alternatively, we could 
use google api calls for everything.   However, the catch there is not all
api calls are in the python lib.  Not clear best solution.

### Reset

It would be easier to debug/do dev if the "reset" or "remove" command also cleaned out any orphan tasks.  (note:
need to worry about multiple clusters under one project.
Perhaps we could add a "clean project" command which resets everything to a clean slate.)

### Polling efficiency

We could use the pub/sub service to publish an event whenever a task state is successfully updated.  We could then only query the
state from datastore when we receive a message for that jobid and eliminate polling.

Upon job creation, create a channel for updates.  On clean/remove delete channels.
Change jq update to write update to channel.  In this way, we can query database for initial snapshot and then listen to channel for updates.
Does something write merged view somewhere?  Skip for now...

### Requests to functionality

Need "retry" command.
Change node-pool settings to default to auto-scale (and pre-emptable?)
    (put these defaults into .kubeque?)
Write a props somewhere in GCS output directory?
    - Maybe also write out csv file with job id after fetching?
Show props in status output
Set up configuration for all gcloud ops to simplify setup (x)
    - update instructions 
    - have kubeque detect whether login is needed. (x)
Status should report where kubernetes job exists