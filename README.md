# Sparkle Spray: Easy submission of batch jobs to google compute engine

Sparkle spray, or "sparkles" for short, is a command line tool to make it easy to submit adhoc batch jobs
for execution on GCE instances.

# Getting started:

## Overview

To run a process or batch of processes via sparkles, you will need to:

1. Create a docker image and upload into a repository that the cluster can access.
   (or use an existing image)
2. Submit the actual job (sparkles sub ...)
3. You then may optionally download the output or leave it in google storage for later. (sparkles fetch ...)

## Prerequisites

Create a google project if you do not already have one you wish to use. In the below, we'll assume the project id is
"your-project". Also create a google bucket to store results.
We'll assume the bucket name is "your-bucket" below.

Install the Google Cloud SDK ( https://cloud.google.com/sdk/install ) and
then set up gcloud:

```
# authenticate for the gcloud tool
gcloud auth login
```

## Setting up

### Set up a python 3.6 virtual environment

sparkles uses google's services python client libraries, which in turn have a
fair number of their own dependencies, so it's really best to create virtual
environment to install sparkles to keep it isolated from other libraries
installed on your system. One can probably use virtualenv do
this, but I'm including conda because that's what I personally use and have
tested with.

Create the conda environment and activate it:

```
conda create -n sparkles python=3.6
source activate sparkles
```

### Installing sparkles into the virtual environment

Download the [latest release tar file](https://github.com/broadinstitute/sparklespray/releases), extract it and run the install:

```
tar xzf sparklespray-LATEST_VERSION.tar.gz
cd sparklespray-LATEST_VERSION
pip install .
```

This will add the `sparkles` command which is used for all operations.

Then to configure sparkles, create a config file ".sparkles" in your home
directory or in the directory you'll be submitting jobs from. This config
file should contain the following:

```
[config]
default_url_prefix=gs://your-bucket
project=your-project
default_image=alpine
machine_type=n1-standard-1
zones=us-east1-b
region=us-east1
```

Be sure to replace "your-bucket" and "your-project" with the bucket name and
your project id respectively. (The bucket is used to hold results from jobs
and cache data pushed to nodes. You can either give a name of a nonexistant
bucket, which will automatically be created when you run "setup" below, or
an existing bucket as long as its in the same project.)

Once you have a config file you can have sparkles use the gcloud command and
the account you authenticated above to setup the project with everything
that sparklespray requires. (Specifically, this command enables DataStore, Google
Storage, Container Registry service and the Genomics Pipeline API and creates a role account which has
access to those services. It will also enable docker to use google credentials when
authentication to Google's Container Registry service.)

```
sparkles setup
```

After this completes successfully, you can run a series of checks to confirm
that everything is set up correctly:

```
sparkles validate
```

If this completes without errors, you are good to go! Try the following
submission:

```
sparkles sub echo Done
```

Once you've seen your first sparkles job complete successfully, you can
change "zones", and "default_image" based on your needs.

# Command reference

## Running jobs

Let's take a trivial "echo hello" submission and look at the output.

```
$ sparkles sub echo hello
0 files (0 bytes) out of 1 files will be uploaded
Submitting job: 20200417-225753-3a37
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: submitted(type=preemptible) (1)
tasks: pending (1), worker nodes: staging(type=preemptible) (1)
```

Let's break this into parts. First the intial submission is executed to tell sparkles that we want to run "echo hello" on some host using our configuation information stored in `.sparkles`:

```
$ sparkles sub echo hello
```

The first line of output is telling us how many files needs to be uploaded to cloud storage before the job can start:

```
0 files (0 bytes) out of 1 files will be uploaded
```

In this case, there was one file (an internal file used by sparkles itself) but that file already had been uploaded before, and therefore already was residing in cloud storage. As a result 0 bytes actually needed to be uploaded.

Next, sparkles reports the name of the job being submitted. Since we did not name the job with the `-n` parameter, it generated a name of the format `timestamp-randomstring`. This job name is used to identify the job and required various other commands such as `sparkles kill` or `sparkles status`

```
Submitting job: 20200417-225753-3a37
```

After the job is submitted, by default sparkles will start periodically poll the status of the job. Each time the state of the job changes, a line is printed. The initial line looks like the following:

```
tasks: pending (1), worker nodes:
```

This line is reporting that this job has a single `task`, and there are no workers (VMs) availible to run the task.

Since sparkles discovers there are no workers already running, but our job has a task to run, proceeds to request a single work by powering on a new VM. When this worker comes online, it will start executing any availible tasks.

The next update for this job looks like:

```
tasks: pending (1), worker nodes: submitted(type=preemptible) (1)
```

Again, we still have a single task waiting to execute, but now there is one worker with the "submitted" status. Also, we can see that this worker is going to be running on a preemptible VM. After a little time an another update is displayed:

```
tasks: pending (1), worker nodes: staging(type=preemptible) (1)
```

The only change is the worker has switched from "submitted" to "staging" which means the VM has successfully powered on, and is being initialized. Once fully initialized the worker will find a task to run.

Our next update reports:

```
Job finished. 1 tasks completed successfully, 0 tasks failed
[22:59:21] hello
Done waiting for job. You can download results via 'gsutil rsync -r gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37 DEST_DIR'
```

In this case, the job finished before our next poll. The task was picked by the worker as soon as it finished initializing, and we can see the output with a timestamp on the following line. Lastly, since the job completed, sparkles reports where you can find the outputs from all tasks in the job.

The outputs that are stored in cloud storage after the task completes are any files stored in the task's working directory (excluding those which were downloaded and placed before the start of the task)

If we look in this folder we can see there's a single file:

```
$ gsutil ls gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37
gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37/1/
```

We see there's a single folder named "1". We will have a single folder for each task that was executed.

If we then look into that folder, we'll see two additional file:

```
$ gsutil ls gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37/1/
gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37/1/result.json
gs://broad-achilles-kubeque/test-kube/20200417-225753-3a37/1/stdout.txt
```

The `results.json` file is an internal file sparkles writes with some metrics about the task's execution (ie: memory used, exit code, etc) and stdout.txt contains everything writen to stderr or stdout from the task.

Now let's try that again, except this time, let's write a script that:

1. takes a little longer to run
2. writes an output file

This is a better simulation of what a typical job submission will look like.

In this example, I'm making a shell script named "write_and_wait.sh" which contains the following:

```
echo writing output...
echo hello > world.txt
echo sleeping...
sleep 10
echo exiting...
```

This script is simply going to write the word "hello" to the file named "world.txt" and then wait 10 seconds before terminating.

So, now let's submit this job for execution:

```
1 files (88 bytes) out of 2 files will be uploaded
Submitting job: test-job
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: submitted(type=preemptible) (1)
tasks: pending (1), worker nodes: staging(type=preemptible) (1)
tasks: claimed (1), worker nodes: running(type=preemptible) (1)
Job finished. 1 tasks completed successfully, 0 tasks failed
[23:24:35] writing output...
           sleeping...
           exiting...
Done waiting for job. You can download results via 'gsutil rsync -r gs://broad-achilles-kubeque/test-kube/test-job DEST_DIR'
```

Again, let's break down what happened. There are a few extra command line parameters, which we'll go through:

1. `-n test-job` was added to name the job `test-job`. See "reusing VMs" to see the advantage of always naming jobs.
2. `-u write_and_wait.sh` was added to tell sparkles that we need this file on the remote host to run the command. Sparkles will take care of staging it on the remote worker before task starts.
3. `sh write_and_wait.sh` is the command that will actually run for the task.

Also in the output there's a change in the first line:

```
1 files (88 bytes) out of 2 files will be uploaded
```

We see that this time, there was an additional file that needs to be transfer to the remote host (`write_and_wait.sh`) and since sparkles has never seen this file, we'll need to upload the contents to cloud storage.

Also, this time our job wrote some output, so let's look at the task's output folder in cloud storage:

```
$ gsutil ls gs://broad-achilles-kubeque/test-kube/test-job/1
gs://broad-achilles-kubeque/test-kube/test-job/1/result.json
gs://broad-achilles-kubeque/test-kube/test-job/1/stdout.txt
gs://broad-achilles-kubeque/test-kube/test-job/1/world.txt
```

We can see our output file world.txt was automaticly recognized as an output and uploaded for us.

Reusing VMs

So far, each job submission has resulted in a new VM being powered on, which means that we have to wait a while before our script starts executing. If we are name the job and haven't changed anything that impacts the configuration of the worker we're using (ie: machine type, docker image, etc) then we can reuse VMs from past executions.

Each time a batch of workers runs out of tasks to execute, one worker will stay online for an additional 10 minutes in case a subsequent job shows up. This is a frequent scenerio when developing, as one often needs to submit repeatly with small code changes, and this the use case that sparkles is optimized for.

So now if we make a minor change to `write_and_wait.sh` and resubmit, we get the following:

```
\$ sparkles sub -n test-job -u write_and_wait.sh sh write_and_wait.sh
1 files (88 bytes) out of 2 files will be uploaded
Submitting job: test-job
tasks: claimed (1), worker nodes: running(type=preemptible) (1)
[23:29:22][starting tail of log test-job.1]
[23:29:24] writing output...
sleeping...
[23:29:24] Processes running in container: 5, total memory used: 0.345 GB, data memory used: 0.296 GB, shared used 0.034 GB, resident 0.073 GB
Job finished. 1 tasks completed successfully, 0 tasks failed
[23:29:58] writing output...
sleeping...
exiting...
Done waiting for job. You can download results via 'gsutil rsync -r gs://broad-achilles-kubeque/test-kube/test-job DEST_DIR'

```

In this case, the job started almost immediately with no delay. You can also see the first status line:

```

tasks: claimed (1), worker nodes: running(type=preemptible) (1)

```

Didn't report that it didn't need to submit a request for a new worker, it immediately saw there already was one in the `running` state, and it immediately updated the task's state to `claimed` when it started running the task.


### GPU usage

Sparkles supports GPUs via `--gpu_count {number_of_gpus}`. By default, it will spawn a Nvidia Tesla p100.
You would need to be in the zone us-east1-b (or one supporting this GPU) to have access to this feature.

If you would want to use them, you would need to have a [compatible Docker image](https://www.tensorflow.org/install/gpu).
If you are using Tensorflow, it is highly recommended to derive your Docker image from [tensorflow/tensorflow:latest-gpu-py3](https://hub.docker.com/r/tensorflow/tensorflow/).

Benchmarking will be coming is available [here](benchmark.html) to decide on your machine specifications.

## Submitting along with multiple files that are needed by job

Files can automatically be uploaded from your local host on submission, and will be downloaded to the working directory before your job starts. You can specify what files you'd like uploaded with the "-u" option.

For example:

```

sparkles sub -n sample-job -u mandelbrot.py python3 mandelbrot.py 0 0 0.5

```

will upload the latest mandelbrot.py and download it onto the remote machine before execution starts. It's worth noting that this is equvilient to:

```

sparkles sub -n sample-job python3 '^mandelbrot.py' 0 0 0.5

```

If you have many files that your job depends on, it may be easier to list the files in a seperate file (one filename per line) and upload all of the files by specifying '-u @file_list'

If a directory is specified then each file within that directory will be uploaded.
When files are downloaded onto the remote node, they are always placed within the current working directory. You can override that behavior by appending ":destination_path" onto the end of the filename.

For example "-u /users/pgm/foo" will be stored on the execution host in "./foo". However, if you specify the file as '-u /users/pgm/foo:pgm/foo' then it will be stored in ./pgm/foo

## Simulating a submission by running it locally

The following will do all the upload data and bookkeeping normally done for jobs, but will not actually create a kubernetes job to run it. Instead, after
all data is uploaded, it will run the equivilent docker command locally to simulate execution. This can be helpful for debugging issues.

```

sparkles sub --local python3 '^mandelbrot.py' 0 0 0.5

```

Submit a sample job reserving 1G of memory (or you can update the memory
settings in .sparkles)

```

sparkles sub -r memory=1G -n sample-job python3 '^mandelbrot.py' 0 0 0.5

```

Download the results

```

sparkles fetch sample-job

```

Submit multiple parameterized by csv file

```

sparkles sub --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'
sparkles sub --fetch results --params params.csv python3 '^mandelbrot.py' '{x_scale}' '{y_scale}' '{zoom}'

```

Add additional machines to be used for a job:

```

## the following will add 2 more worker nodes which will be used by the last

## job submitted. Instead of the word "LAST" you can use a job id. Most

## commands which accept a jobid also understand "LAST" as a synonym for the

## last submitted job.

sparkles addnodes LAST 2

```

## Cleaning out old jobs

Sparklespray remembers jobs until you explicitly remove them. You can use the
"clean" command to forget tracking information about a given job. Note, this
does not delete the results stored in the bucket from that job, only delete
data about the tasks that made up the job.

To remove all non-running jobs:

```

sparkles clean

```

## Killing a job

The following will kill the last job (change LAST to a specific job id if
you wish to kill a different job) and stop the nodes associated with that
job.

`sparkles kill LAST`

## Resubmitting failures

If you have a job that failed due to some transient failure, you can tell sparkles to take the tasks which did not complete successfully and mark them as 'pending' to try them again. (Note: Only run this after the job is finished running. If there are any tasks still running, marked 'claimed', this will reset those as well)

```

# find all the incomplete tasks and mark them as ready to be run again

> sparkles reset JOBID

# Now, kick of executing the job again

> sparkles watch JOBID

```

That will work for transient failures. However, more often you may have failures that are deterministic and require you change something and then re-run.

For example, you may have some of your jobs fail (not enough memory, or some data
specific bug). In such a case, you might want to rerun only those that failed after you've corrected the underlying problem. To do this, you
can query for only those jobs which did not complete, and get their
parameters. Once you have those parameters, you can resubmit only those
parameters which had problems.

```

# The first submission submits everything

> sparkles sub --params parameters.csv process_file.py '{^filename}'

# but, oh no! some of the jobs failed. After you've made your fix to

# process_file.py, you can resubmit the failures:

> sparkles list --params --filter exit_code!=0 -o missing-tasks.csv
> sparkles sub --params missing-tasks.csv process_file.py '{^filename}'

```

(If you want to see which parameters were associated with which task, that
information is contained within results.json in the output directory for each task.)

## Viewing output of tasks

TODO: Write docs about setting up firewall rule to allow traffic on port
6032

## View the status of a job

```

sparkles status JOB_ID

```

You can also see summary stats (execution times and memory usage) by adding the --stats parameter.

```

sparkles status JOB_ID --stats

```

## Mark jobs as needing re-execution

```

sparkles reset JOB_ID

```

## Show details of tasks in the job

```

sparkles show JOB_ID

```

There's also a more powerful query tool "list" which can export all attributes of all tasks and supports filtering.

```

sparkles list JOB_ID

```

## Poll the status of a job, adding nodes if necessary

```

sparkles watch JOB_ID

```

# Developing sparklespray

The repo contains code in two languages: go and python. The go code is for
the "consumer" process which runs on each worker and takes jobs from the
queue. The python code is used by the "sparkles" command line tool.

To build a new release need the "consumer" compiled, and the python code
packaged up into a tar.gz file. You can do this by running:

```

# compile the go code and save the binary as ./sparklespray/bin/kubequeconsume

\$ sh build-consumer.sh

# make the installable python package which will be saved in the dist

# directory with the version on the end of the name.

# (Example: dist/sparklespray-3.0.2.tar.gz)

\$ python setup.py sdist

```

sparkles sub -u train_mlp_sm.py -u data -u benchmark_train_mlp.py --gpu n --gpu_count 0 -i tensorflow/tensorflow:latest-py3 --machine-type n1-standard-4 python benchmark_train_mlp.py --data_size small --test_description \'Machine type n1-standard-4, GPU count 0, small dataset\'

## Changing the protocol between "sparkles" and "consumer"

The command line tool communicates with workers via gRPC. If a change is
made to the protocol, we need to regenerate the python and go code used by
the client/server ends by running:

```

\$ sh scripts/build-grpc.sh # You might need to install protobuf first

```

This will write generated code to the following files:

```

./go/src/github.com/broadinstitute/kubequeconsume/pb/pb.pb.go
./sparklespray/pb_pb2.py
./sparklespray/pb_pb2_grpc.py

```

```
