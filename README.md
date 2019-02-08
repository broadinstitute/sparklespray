# Sparkle Spray: Easy submission of batch jobs to google compute engine

Sparkle spray, or "sparkles" for short, is a command line tool to make it easy to submit adhoc batch jobs
for execution on GCE instances.

# Getting started:

## Overview

To run a process or batch of processes via sparkles, you will need to:

1. Create a docker image and upload into a repository that the cluster can access.
(or use an existing image)
2. Submit the actual job (sparkles sub ...)
3. You then may optionally download the output or leave it in google storage for later.  (sparkles fetch ...) 

## Prerequisites

Create a google project if you do not already have one you wish to use.  In the below, we'll assume the project id is
"your-project". Also create a google bucket to store results. We'll assume
the bucket name is "your-bucket" below.

Add a firewall rule to your project: https://console.cloud.google.com/networking/firewalls => `Create firewall rule`
=> name = sparklespray-monitor, targets = All instances of the network, source IP ranges = 0.0.0.0/0, check tcp port and enter value 6032

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

Download the latest release tar file, extract it and run the install:

```
tar xzf sparklespray-LATEST_VERSION.tar.gz
cd sparklespray-LATEST_VERSION
python setup.py install
```

This will add the `sparkles` command which is used for all operations.

Then to configure sparkles, create a config file ".sparkles" in your home
directory or in the directory you'll be submitting jobs from. This config
file should contain the following:

```
[config]
default_url_prefix=gs://your-bucket
project=your-project
default_image=octoblu/alpine-ca-certificates
machine_type=n1-standard-1
zones=us-east1-b
```

Be sure to replace "your-bucket" and "your-project" with the bucket name and 
your project id respectively. (The bucket is used to hold results from jobs
and cache data pushed to nodes. You can either give a name of a nonexistant
bucket, which will automatically be created when you run "setup" below, or
an existing bucket as long as its in the same project.)

Once you have a config file you can have sparkles use the gcloud command and
the account you authenticated above to setup the project with everything
that sparklespray requires. (Specifically, this command enables DataStore, Google
Storage, and the Genomics Pipeline API and creates a role account which has
access to those services.)

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
sparkles sub sh -c 'echo Done!'
```

Once you've seen your first sparkles job complete successfully, you can
change "zones", "default_image", "default_resource_cpu", and
"default_resource_memory" based on your needs.

# Command reference

## Running jobs

Submitting a sample job

There's a sample script in the examples/sample-job directory. In order to
run, you will need to make a '.sparkles' file in that directory with the
following content:

```
[config]
default_url_prefix=gs://YOUR_BUCKET
project=YOUR_PROJECT
default_image=python
machine_type=n1-standard-1
zones=us-east1-b
```

You should replace "YOUR_

To run:

```
> sparkles sub python '^mandelbrot.py' 0 0 0.5
2017-09-15 09:49:48,062 Already in CAS cache, skipping upload of mandelbrot.py
2017-09-15 09:49:48,171 Already in CAS cache, skipping upload of /Users/pmontgom/dev/sparkles/sparkles/bin/sparklesconsume
2017-09-15 09:49:48,171 Submitting job with id: 20170915-094947-1fb5
2017-09-15 09:49:48,386 Saved task definition batch containing 1 tasks
2017-09-15 09:49:49,195 Saved job definition with 1 tasks
2017-09-15 09:49:49,891 Adding initial node for cluster
2017-09-15 09:49:50,554 Node's log will be written to: gs://broad-achilles-sparkles/test/kube/node-logs/EJCHtq7oKxjbzq-lrdL-xg8gtubt_vUYKg9wcm9kdWN0aW9uUXVldWU
2017-09-15 09:49:50,554 Waiting for job to terminate
2017-09-15 09:49:50,805 Tasks: pending: 1
2017-09-15 09:49:50,976 Nodes: (no nodes)
2017-09-15 09:50:01,603 Nodes: RUNNING: 1
2017-09-15 09:51:04,076 Tasks: complete(code=0): 1
2017-09-15 09:51:04,076 Done waiting for job to complete, results written to gs://broad-achilles-sparkles/test/kube/20170915-094947-1fb5
2017-09-15 09:51:04,076 You can download results via 'gsutil rsync -r gs://broad-achilles-sparkles/test/kube/20170915-094947-1fb5 DEST_DIR'
```

Note, it took about 10 seconds to get the first worker node started (@
09:49) and then another minute for it to pull the docker container and start
running the task. (The task itself, took less than a second and completed @ 9:51). However, if we submit a second job which has the same requirements
(# number of CPUs required, same memory required, same docker image) then we
can use the worker that is still running from this last invocation.

```
> sparkles sub python '^mandelbrot.py' 0 0 0.4
2017-09-15 09:51:18,430 Already in CAS cache, skipping upload of mandelbrot.py
2017-09-15 09:51:18,538 Already in CAS cache, skipping upload of /Users/pmontgom/dev/sparkles/sparkles/bin/sparklesconsume
2017-09-15 09:51:18,538 Submitting job with id: 20170915-095118-af10
2017-09-15 09:51:18,735 Saved task definition batch containing 1 tasks
2017-09-15 09:51:19,439 Saved job definition with 1 tasks
2017-09-15 09:51:20,086 Cluster already exists, not adding node. Cluster status: RUNNING: 1
2017-09-15 09:51:20,086 Waiting for job to terminate
2017-09-15 09:51:20,361 Tasks: claimed: 1
2017-09-15 09:51:20,524 Nodes: RUNNING: 1
2017-09-15 09:51:25,632 Tasks: complete(code=0): 1
2017-09-15 09:51:25,632 Done waiting for job to complete, results written to gs://broad-achilles-sparkles/test/kube/20170915-095118-af10
2017-09-15 09:51:25,632 You can download results via 'gsutil rsync -r gs://broad-achilles-sparkles/test/kube/20170915-095118-af10 DEST_DIR'
```

Note at 9:51 it recognizes there's already a worker running, so the task
gets picked up right away at 09:51:20 and the whole process takes only 7
seconds.

### A note on resource requirements

The CPUs and memory required are used as minimums and will determine the
smallest machine type which satisfies the requirements. (The machine types are listed 
here https://cloud.google.com/compute/docs/machine-types)
The actual machine type chosen may have more memory/cpus than was required,
and your process is free to use the additional resources. 

Expect that larger requirements will require more expensive instances in
order to run. 

If you are interested in how much memory your process actually used, you can
see that information in the results.json file saved for each task.

### GPU usage

Sparkles supports GPUs via `--gpu_count {number_of_gpus}`. By default, it will spawn a Nvidia Tesla p100.
You would need to be in the zone us-east1-b (or one supporting this GPU) to have access to this feature.

If you would want to use them, you would need to have a [compatible Docker image](https://www.tensorflow.org/install/gpu).
If you are using Tensorflow, it is highly recommended to derive your Docker image from [tensorflow/tensorflow:latest-gpu-py3](https://hub.docker.com/r/tensorflow/tensorflow/).

Benchmarking will be coming is available [here](benchmark.html) to decide on your machine specifications.

## Submitting along with multiple files that are needed by job

Files can automatically be uploaded from your local host on submission, and will be downloaded to the working directory before your job starts.  You can specify what files you'd like uploaded with the "-u" option.

For example:

```
sparkles sub -n sample-job -u mandelbrot.py python3 mandelbrot.py 0 0 0.5
```

will upload the latest mandelbrot.py and download it onto the remote machine before execution starts.   It's worth noting that this is equvilient to:
```
sparkles sub -n sample-job python3 '^mandelbrot.py' 0 0 0.5
```

If you have many files that your job depends on, it may be easier to list the files in a seperate file (one filename per line) and upload all of the files by specifying '-u @file_list'

If a directory is specified then each file within that directory will be uploaded.
When files are downloaded onto the remote node, they are always placed within the current working directory.  You can override that behavior by appending ":destination_path" onto the end of the filename.

For example "-u /users/pgm/foo" will be stored on the execution host in "./foo".     However, if you specify the file as '-u /users/pgm/foo:pgm/foo' then it will be stored in ./pgm/foo

## Simulating a submission by running it locally

The following will do all the upload data and bookkeeping normally done for jobs, but will not actually create a kubernetes job to run it.  Instead, after
all data is uploaded, it will run the equivilent docker command locally to simulate execution.  This can be helpful for debugging issues.

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

Sparklespray remembers jobs until you explicitly remove them.  You can use the
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

``
sparkles kill LAST 
``

## Resubmitting failures

You may have some of your jobs fail (not enough memory, or some data
specific bug) and you want to rerun only those that failed. To do this, you
can query for only those jobs which did not complete, and get their
parameters. Once you have those parameters, you can resubmit only those
parameters which had problems.

```

# The first submission submits everything
> sparkles sub --params parameters.csv process_file.py '{^filename}'

# but, oh no! some of the jobs failed. After you've made your fix to
# process_file.py, you can resubmit the failures:
> sparkles listparams --incomplete missing-tasks.csv
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

## Mark jobs as needing re-execution

```
sparkles reset JOB_ID
```

## Show details of tasks in the job

```
sparkles show JOB_ID
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
$ sh build-consumer.sh

# make the installable python package which will be saved in the dist
# directory with the version on the end of the name. 
# (Example: dist/sparklespray-3.0.2.tar.gz)
$ python setup.py sdist
```
sparkles sub -u train_mlp_sm.py -u data -u benchmark_train_mlp.py --gpu n --gpu_count 0 -i tensorflow/tensorflow:latest-py3 --machine-type n1-standard-4 python benchmark_train_mlp.py --data_size small --test_description \'Machine type n1-standard-4, GPU count 0, small dataset\'
## Changing the protocol between "sparkles" and "consumer"

The command line tool communicates with workers via gRPC. If a change is
made to the protocol, we need to regenerate the python and go code used by
the client/server ends by running:

```
$ sh scripts/build-grpc.sh # You might need to install protobuf first
```

This will write generated code to the following files:

```
./go/src/github.com/broadinstitute/kubequeconsume/pb/pb.pb.go
./sparklespray/pb_pb2.py
./sparklespray/pb_pb2_grpc.py
```


Setup
    validate            Run a series of tests to confirm the configuration is
                        valid

    setup               Configures the google project chosen in the config to
                        be compatible with sparklespray. (requires gcloud
                        installed in path)

    version             print the version and exit

Submissions
    sub                 Submit a command (or batch of commands) for execution
    logs                Print out logs from failed tasks
    show                Write to a csv file the parameters for each task
    status              Print the status for the tasks which make up the
                        specified job
    watch               Monitor the job
    kill                Terminate the specified job
    fetch               Download results from a completed job

Debugging
    reset               Mark any 'claimed', 'killed' or 'failed' jobs as ready
                        for execution again. Useful largely only during
                        debugging issues with job submission. Potentially also
                        useful for retrying after transient failures.
    clean               Remove jobs which are not currently running from the
                        database of jobs

    dump-operation      primarily used for debugging. If a sparkles cannot
                        turn on a node, this can be used to dump the details
                        of the operation which requested the node.
