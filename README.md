# Kubeque: Easy submission of batch jobs to google compute engine

kubeque is a command line tool to make it easy to submit adhoc batch jobs that
GCE instances.

# Getting started:

## Overview

To run a process or batch of processes via kubeque, you will need to:

1. Create a docker image and upload into a repository that the cluster can access.
(or use an existing image)
2. Submit the actual job (kubeque sub ...)
3. You then may optionally download the output or leave it in google storage for later.  (kubeque fetch ...) 

## Prereqs
Create a google project.  In the below, we'll assume the project name is PROJECT_NAME.

You'll need the following APIs enabled: "Google Cloud Pub/Sub API", "Google
Cloud Datastore API", "google cloud
storage" and "Genomics API". You can enable these through the google console API library:
https://console.cloud.google.com/apis/library. 

Create a bucket for holding results and uploads.  In the following example, we'll assume the name of the bucket is BUCKET_NAME.

Google's cloud SDK installed and in your path: https://cloud.google.com/sdk/

To set up gcloud:

```
# authenticate for the gcloud tool
gcloud auth login
# authenticate for running tools other than gcloud
gcloud auth application-default login
# setup which should be the default project and zone for your cluster
gcloud init
```

## Setting up

### Set up a python 3.5 virtual environment

kubeque uses google's services python client libraries, which in turn have a
fair number of their own dependencies, so it's really best to create virtual
environment to install kubeque into. One can probably use virtualenv do
this, but I'm including conda because that's what I personally use and have
tested with.

Create the conda environment and activate it:
```
conda create -n kubeque python=3.5
source activate kubeque
```

### Installing into virtual environment

Check out the kubeque repo and install by running in a python 3.5 virtual
environment:

```
pip install -r requirements.txt
python setup.py install
```

This will add the `kubeque` command which is used for all operations.

Then to configure kubeque, create a config file "~/.kubeque" or in the current directory containing the following
(change the values of zone, region, and account to match what you used when
running gcloud init):

```
[config]
default_url_prefix=gs://YOUR_BUCKET/PREFIX
project=YOUR_PROJECT
default_image=python
default_resource_cpu=1
default_resource_memory=100M
zones=us-east1-b
```

Once you have a config file you can test to see if your account and config
are set up correctly by running:

```
kubeque validate
```

If this completes without errors, you are good to go!

## Running jobs

Submitting a sample job

There's a sample script in the examples/sample-job directory. In order to
run, you will need to make a '.kubeque' file in that directory with the
following content:

```
[config]
default_url_prefix=gs://YOUR_BUCKET/PREFIX
project=YOUR_PROJECT
default_image=python
default_resource_cpu=1
default_resource_memory=100M
zones=us-east1-b
```

To run:

```
> kubeque sub python '^mandelbrot.py' 0 0 0.5
2017-09-15 09:49:48,062 Already in CAS cache, skipping upload of mandelbrot.py
2017-09-15 09:49:48,171 Already in CAS cache, skipping upload of /Users/pmontgom/dev/kubeque/kubeque/bin/kubequeconsume
2017-09-15 09:49:48,171 Submitting job with id: 20170915-094947-1fb5
2017-09-15 09:49:48,386 Saved task definition batch containing 1 tasks
2017-09-15 09:49:49,195 Saved job definition with 1 tasks
2017-09-15 09:49:49,891 Adding initial node for cluster
2017-09-15 09:49:50,554 Node's log will be written to: gs://broad-achilles-kubeque/test/kube/node-logs/EJCHtq7oKxjbzq-lrdL-xg8gtubt_vUYKg9wcm9kdWN0aW9uUXVldWU
2017-09-15 09:49:50,554 Waiting for job to terminate
2017-09-15 09:49:50,805 Tasks: pending: 1
2017-09-15 09:49:50,976 Nodes: (no nodes)
2017-09-15 09:50:01,603 Nodes: RUNNING: 1
2017-09-15 09:51:04,076 Tasks: complete(code=0): 1
2017-09-15 09:51:04,076 Done waiting for job to complete, results written to gs://broad-achilles-kubeque/test/kube/20170915-094947-1fb5
2017-09-15 09:51:04,076 You can download results via 'gsutil rsync -r gs://broad-achilles-kubeque/test/kube/20170915-094947-1fb5 DEST_DIR'
```

Note, it took about 10 seconds to get the first worker node started (@
09:49) and then another minute for it to pull the docker container and start
running the task. (The task itself, took less than a second and completed @ 9:51). However, if we submit a second job which has the same requirements
(# number of CPUs required, same memory required, same docker image) then we
can use the worker that is still running from this last invocation.

```
> kubeque sub python '^mandelbrot.py' 0 0 0.4
2017-09-15 09:51:18,430 Already in CAS cache, skipping upload of mandelbrot.py
2017-09-15 09:51:18,538 Already in CAS cache, skipping upload of /Users/pmontgom/dev/kubeque/kubeque/bin/kubequeconsume
2017-09-15 09:51:18,538 Submitting job with id: 20170915-095118-af10
2017-09-15 09:51:18,735 Saved task definition batch containing 1 tasks
2017-09-15 09:51:19,439 Saved job definition with 1 tasks
2017-09-15 09:51:20,086 Cluster already exists, not adding node. Cluster status: RUNNING: 1
2017-09-15 09:51:20,086 Waiting for job to terminate
2017-09-15 09:51:20,361 Tasks: claimed: 1
2017-09-15 09:51:20,524 Nodes: RUNNING: 1
2017-09-15 09:51:25,632 Tasks: complete(code=0): 1
2017-09-15 09:51:25,632 Done waiting for job to complete, results written to gs://broad-achilles-kubeque/test/kube/20170915-095118-af10
2017-09-15 09:51:25,632 You can download results via 'gsutil rsync -r gs://broad-achilles-kubeque/test/kube/20170915-095118-af10 DEST_DIR'
```

Note at 9:51 it recognizes there's already a worker running, so the task
gets picked up right away at 09:51:20 and the whole process takes only 7
seconds.

## A note on resource requirements

The CPUs and memory required are used as minimums and will determine the
smallest machine type which satisfies the requirements. (The machine types are listed 
here https://cloud.google.com/compute/docs/machine-types)
The actual machine type chosen may have more memory/cpus than was required,
and your process is free to use the additional resources. 

Expect that larger requirements will require more expensive instances in
order to run. 

If you are interested in how much memory your process actually used, you can
see that information in the results.json file saved for each task.

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
all data is uploaded, it will run the equivilent docker command locally to simulate execution.  This can be helpful for debugging issues.

```
kubeque sub --local python3 '^mandelbrot.py' 0 0 0.5
```

Submit a sample job reserving 1G of memory (or you can update the memory
settings in .kubeque)

```
kubeque sub -r memory=1G -n sample-job python3 '^mandelbrot.py' 0 0 0.5
```

Download the results

```
kubeque fetch sample-job
```

Submit multiple parameterized by csv file

```
kubeque sub --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'
kubeque sub --fetch results --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'
```

Add additional machines to be used for a job:

```
# the following will add 2 more worker nodes which will be used by the last
# job submitted. Instead of the word "LAST" you can use a job id. Most
# commands which accept a jobid also understand "LAST" as a synonym for the
# last submitted job.
kubeque addnodes LAST 2
```

## Cleaning up

Kubeque remembers jobs until you explicitly remove them.   To remove all non-running jobs:

```
kubeque clean
```

## Killing a job
The following will kill the last job (change LAST to a specific job id if
you wish to kill a different job) and stop the nodes associated with that
job.

``
kubeque kill LAST 
``

## Resubmitting failures

You may have some of your jobs fail (not enough memory, or some data
specific bug) and you want to rerun only those that failed. To do this, you
can query for only those jobs which did not complete, and get their
parameters. Once you have those parameters, you can resubmit only those
parameters which had problems.

```

# The first submission submits everything
> kubeque sub --params parameters.csv process_file.py '{^filename}'

# but, oh no! some of the jobs failed. After you've made your fix to
# process_file.py, you can resubmit the failures:
> kubeque listparams --incomplete missing-tasks.csv
> kubeque sub --params missing-tasks.csv process_file.py '{^filename}'
```

(If you want to see which parameters were associated with which task, that
information is contained within results.json in the output directory for each task.)

# Viewing output of tasks

IF you submit a task with the "--loglive" option, the output of your tasks
will be written to StackDriver (https://cloud.google.com/logging/) where you
can view the output as its written.

For example if you submit a command such as:
```
kubeque sub --loglive -n myjob my-executable
```

You can go the google cloud console, under "StackDriver" select "logging".
Here you should see a list of log messages from across your entire project
with a search box at the top.

For this example, entering "label:kubeque-task-id:myjob.0" will show you
only the feed from task "0" (the first one) of the job submitted with id
"myjob".


# The crazy steps neccessary to view progress

In a future version, we will have a 'kubeque peek' command for viewing
stdout live. However, there are several technical hurdles to overcome to
implement that. 

In the mean time, viewing logs live is complicated, but can be done as
follows (assuming JOB_ID is the name of your job):

```
kubeque status JOB_ID --detailed
```

In the output look for a statement like `started on pod: ggp-5598619720951178934`
This will give you the name of the host the task is running on. (In this
case ggp-5598619720951178934).

Now, ssh into this host (and you may need to specify the zone this host is
on) and enter the container where the command is running.

```
# ssh into the host
gcloud compute ssh --zone us-east1-d ggp-5598619720951178934

# run bash inside the container
docker exec -it `docker ps | tail -1 | cut -f 1 -d ' '` bash

# change to the directory where the current task is running
cd `ls -td /mnt/kubeque-data/tasks/* | head -1`/work

# This is the directory the task is running from. You can see here all the
# files that have been downloaded or written. To watch the output from the
# command you can run 'tail -f'
tail -f stdout.txt
```

# Development notes (Not useful for users)

### Polling efficiency

We could use the pub/sub service to publish an event whenever a task state is successfully updated.  We could then only query the
state from datastore when we receive a message for that jobid and eliminate polling.

Upon job creation, create a channel for updates.  On clean/remove delete channels.
Change jq update to write update to channel.  In this way, we can query database for initial snapshot and then listen to channel for updates.
Does something write merged view somewhere?  Skip for now...

### Requests to functionality

Write a props somewhere in GCS output directory?
    - Maybe also write out csv file with job id after fetching?
Show props in status output
Set up configuration for all gcloud ops to simplify setup (x)
    - update instructions 
    - have kubeque detect whether login is needed. (x)
Status should report where kubernetes job exists
Reaper should look for example of lots of failed pods (sign of kubeque-consume failing)
Something should clean out dead pods
statically linked version of kubeque-consume

Lower hanging fruit: Reaper suffered from incorrectly believing that jobs
were failing. However, common failure is container being incorrectly
created. Would be good if status poll warned if pods were being respawned.
Maybe status should show both a task view and pod view of what's running?
(Even better if we dump stdout of failed pod for user to review)

TODO: 
    - essential
        - to test: kill
    - productivity
        - new feature: reservation
        - broken: resubmit with new resource requirements
    - nice to have
        - missing: resource utilization over time
        - new feature: speed up submission of jobs with > 100 tasks

done:
    - new feature: generate a csv file of parameters of tasks which did not complete successfully
    - new feature: LAST as an alias for last submitted job when invoking kill, reset, status, etc


## lua workflow executor

Supports checkpointing and full control of job definitions.

todo:
    JobBuilder:
        setCommandTemplate
        createTasksFor
        (see incomplete test)
    Make an example which creates a job submission based on a csv file
    update kubeque to allow programatic:
        job submission via json file
        job wait (Automatically download all results.json to dest location?)

## Todo
Fix warning about jobs queued up to list job ids of other jobs.

Add peek output (somehow)

