## Introduction

Sparkle spray, or "sparkles" for short, is a command line tool to make it easy to submit adhoc batch jobs for execution on the cloud on the Google Cloud Platform.

"sparkles" was designed in the spirit of batch submission tools such as Grid Engine [http://www.univa.com/products/] or Slurm [https://slurm.schedmd.com/sbatch.html] with the key difference that sparkles powers up nodes on GCP, instead of scheduling the work on a traditional HPC cluster.

The largest challenge of running jobs on the cloud is getting the execution environment and data accessible on each node. In most HPC clusters, there is a shared network filesystem which greatly simplifies the problem. Sparkles uses a combination of using docker and the ability to push or overlay addition files to create the execution environment with each job submission. While less convenient, the abilty of the cloud to elastically expand or contract compute on demand makes this tradeoff worthwhile.

The goal of sparkles is to lower the amount of effort required to run a non-interactive script or other compute on the cloud. In addition, once one can run a script via sparkles, it's trivial to run a batch of commands with some varying parameters all in parallel.


## Quick start

All sparkles commands are subcommands of the command line tool "sparkles". The most commonly used is "sub" which is short for "submit".

Perhaps the simplest example of a submission would be to run:

```
sparkles sub echo hello world
```

This should yield output similar to:

```
$ sparkles sub echo hello world        
0 files (0 bytes) out of 1 files will be uploaded
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: staging (1)
[21:29:46] hello world
Job finished. 1 tasks completed successfully, 0 tasks failed
Done waiting for job. You can download results via 'gsutil rsync -r gs://test-sparkles-bucket/20190212-212815-bf0a DEST_DIR'
```

The "hello world" which was written at the output, actually executed on a VM which was powered on solely to run the "echo" command. The stdout of that command was then written to the terminal and then the VM automatically shuts down after the command's completion.

It's worth noting that this trival example took around 80 seconds to execute, which is fairly uninspiring. The time spent is largely due to the time it takes to power on a VM. However, later in the tutorial, we'll see we can get job submissions down to a few seconds by letting sparkles reuse VMs.

## Tutorial

### Terminology

First some basic terminology:

**node** a VM (more specifically, a Google Compute Engine instance) which sparkles powered on.

**task** A single command to be executed.

**job** - The unit of work submitted by executing `sparkles sub`. One job may have multiple *tasks* associated with it.

You will see these terms appear in the output of sparkles and through out the documentation.

All interaction with sparkles is through the `sparkles` command which is divided into 
several sub commands. Executing `sparkles --help` will list all subcommands with a description for each.

### Job submission

`sparkles sub ...` is used for submitting a job and has by far the most options. 

Taking the example from the quick start, let's explain what the output means:

```
$ sparkles sub echo hello world        
0 files (0 bytes) out of 1 files will be uploaded
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: staging (1)
[21:29:46] hello world
Job finished. 1 tasks completed successfully, 0 tasks failed
Done waiting for job. You can download results via 'gsutil rsync -r gs://test-sparkles-bucket/20190212-212815-bf0a DEST_DIR'
```

The first line of output...

```
0 files (0 bytes) out of 1 files will be uploaded
```

...explains how much data needs to be pushed from your local machine to the cloud before the job can start. This only needs to happen the first time a job refers to a file, and resubmissions will reuse cached copies of the data.

The next lines...

```
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: staging (1)
```

...describe the state of the *tasks* and *nodes* associated with the running job. The "pending" refers to the state of the tasks associated with this job and the number in parenthes refers to the count of tasks with that state. The "worker nodes" are discribed similarly. 

In this example, the first line said our job had one task waiting for execution, and in the second line we see a node "staging" or getting ready to start executing tasks.

The next line...

```
[21:29:46] hello world
```

...is reporting output from our task. For longer running jobs, we would probably see another line in the status of the job saying:

```
tasks: claimed (1), worker nodes: running (1)
```

indicating that we have a node which is running tasks, and our one task is now claimed by a node. However, in this example the tasks executed and completed so quickly, sparkles never got a chance to see this state change occur.

Now that all tasks are complete, we get a final summary:

```
Job finished. 1 tasks completed successfully, 0 tasks failed
Done waiting for job. You can download results via 'gsutil rsync -r gs://test-sparkles-bucket/20190212-212815-bf0a DEST_DIR'
```

which tells you counts of tasks that succeeded and failed and how to download all the files that those tasks wrote out. 

#### Naming jobs to improve performance

The first option that we generally always want to use is `-n` to give the job a name. In practice, giving a job a name substantially speeds up job submissions, because when a job is submitted, sparkles decides whether there is an existing VM already running which can take tasks for that job, or whether it needs to turn on new nodes.

Turning on new nodes always takes around a minute or two, but if there's an existing node, then it can pick up the task immediately with no delay. The critera for a "compatible VM" is one where the machine type, the docker image and the job name all match. If job name is not specified a unique one is generated each time, resulting in an new VM each submission.

#### Testing submissions by running locally

When getting started with sparkles, it can be convenient to run the submission locally. Instead of turning on a new VM, the submission with run inside docker on the local machine.

This has the advantage of not needing to wait for a VM and so submissions should always be quicker. However, it is a much more limited mode. The requested machine type will be ignored, so memory and # of cpus are restricted to what is availible locally. (In addition, docker under MacOS has a memory cap more restricted than the physical memory availible, which can be controlled in Docker's settings.) Also, in this mode, only one task per job will run at a time. 

This mode is really best limited to testing, but can be a good way to get started with sparkles.

#### Pushing files along with job submission

As shown in the quick start, a submission can be as simple as `sparkles sub echo hello`, however, without any additional options, we can only execute commands where all files are present within the docker image.

More commonly, we will want to start with a generic docker image, and execute a script using that docker image. In order to do so, we need to tell sparkles to "localize" the file from our local filesystem to the remote node before executing.

For example, if we had an R script named sample.R, we could run it on a remote node by adding a few more options to `sparkles sub`:

```
sparkles sub -i r-base -u sample.R Rscript sample.R
```

The first option (`-i`) overrides the default docker image to use and uses the r-base image from dockerhub. The second flag (`-u`) tells sparkles to push sample.R to the remote node before executing the command `Rscript sample.R`

In practice, you may have many files that you want to push as part of the job submission. To avoid specfying them all on the command line, you can instead put all of the filenames in a text file specify the filename as `-u @filename`.

For example, if we created a file named "files.txt" with the contents:
```
sample.R
data.Rds
```

And we executed:

```
sparkles sub -i r-base -u @files.txt Rscript sample.R
```

Then, sparkles will read `files.txt` to learn that it needs to push `sample.R` and `data.Rds` to the remote node before executing `Rscript sample.R`.

On the surface, it may seem that pushing the data to the remote node each time, would add a lot of time to job submissions. However 'pushing' data aggressively caches files at multiple layers, so the first time a file is referenced, it will need to be uploaded to google cloud storage. For large files this may take some time, but on subsequent job submissions that use the same file, it will recognize if the file is unchanged and avoid uploading it a second time.

#### Submission of parallel jobs

Now that we can submit a single command, it's easy to submit a batch of commands which are identical except for a single parameter.

If all we need can write our script so that it is parameterized by a single integer (similar to what many batch submission tools called a "job array") we can add the `--seq` option and in our command we put `'{index}` as the placeholder for where the task index should be provided.

For example the following submission executes three tasks in parallel:

```
$ sparkles sub -i alpine --seq 3 echo I am task '{index}'
0 files (0 bytes) out of 1 files will be uploaded
tasks: pending (3), worker nodes:
tasks: pending (3), worker nodes: staging (1)
Job finished. 3 tasks completed successfully, 0 tasks failed
Done waiting for job. You can download results via 'gsutil rsync -r gs://test-sparkles-bucket/20190213-110827-8755 DEST_DIR'
```

Now, if you were to look under gs://test-sparkles-bucket/20190213-110827-8755, you'll see three folders named "1", "2", and "3". Each folder contains a stdout.txt file which contains the message written from the `echo` command.

Often we have more than a single parameter we want to vary so sparkles allows the parameters to be taken from a csv file.

Imagine we have a csv file named `parameters.csv` containing:

```
lambda,size
0.1,small
1.0,large
```

We can submit a command using those parameters with the `--params` flag:

```
$ sparkles sub echo '{lambda}' '{size}'
```

This will result in the command `echo 0.1 small` and `echo 1.0 large` being run as two tasks in parallel.

#### Advanced parallel submission with --foreach

A common pattern that arrises is one where we have some compute we'd like to run on some large dataset, but we can chop up the input data into pieces that can be computed independently. Then for each piece in parallel, we run the compute. After all those tasks are done, we merge all the results together.

Sometimes this pattern is referred to as "scatter-gather".

Using the options described so far one would have to write at least three scripts:

1. a script to generate a csv with some identifier to which piece of data each task should use.
2. a script which takes that identifier and computes some result
3. a script to download the results and merge them together into the final result.

This scripts 1 and 3 end up being written over and over again for each type of job. 

To streamline this use case, `sparkles` has the `--foreach` option which makes it easy to write a scatter/gather style script in either python or R.

The syntax is slightly different between R and python, but the concepts are identical so once you've used one, the other is probably obvious.

##### Python implementation

In `examples/foreach` we have an example called `sample-scatter.py`. This is a toy example script in which we scale the vector `[1,2,3,4]` by a constant, provided at the time of job submission. The multiplication of the element by the constant can be done independantly, so in this example, each element will be computed by a seperate task.

After all elements are scaled, the final step prints out the resulting list of values.

To run `sample-scatter.py` we can execute:

```
sparkles sub -i python:2.7-alpine -n sample --foreach sample-scatter.py 2.0
```

The `-i` and `-n` options were described earlier, however the important new option here is `--foreach`. This causes the remaining parameters to be interpreted not as a command, but as a script conforming to a few requirements, described below, and a series of arguments.

When running with `--foreach` we require that the script have functions named `get_foreach_args`, `foreach` and `gather`.

In this example, the `sample-scatter.py` script consists of:

```
def get_foreach_args(scale):
    return dict(elements=[1,2,3,4], extra_args=[float(scale)])

def foreach(x, scale):
    return x * scale

def gather(x, *other_args):
    print(x)
```

The first step has `get_foreach_args` run with any arguments passed to in as part of the submission. In the example `sparkles sub ...` submission we can see that value is `"2.0"`

Then `get_foreach_args` returns a dictionary with a list named `elements`. Each element in that list will have `foreach` run on it. In addition `get_foreach_args` returns `extra_args` which are passed to `foreach`.

After `get_foreach_args` a task will be created for each element and as part of that task. Each task will evaluate `foreach` on a single element and any extra arguments returned from `get_foreach_args`.

Executing the submission, we see output like the following:

```
$ sparkles sub -i python:2.7-alpine -n sample --foreach sample-scatter.py 2.0
Submitting job named sample-getargs to execute sample-getargs('2.0')
0 files (0 bytes) out of 3 files will be uploaded
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: staging (1)
Job finished. 1 tasks completed successfully, 0 tasks failed
Submitting job named sample to evaluate foreach on 4 elements
0 files (0 bytes) out of 3 files will be uploaded
tasks: pending (4), worker nodes: running (1)
[22:35:46] [starting tail of log sample.1]
[22:35:48] Processes running in container: 3, total memory used: 0.343 GB, data memory used: 0.297 GB, shared used 0.032 GB, resident 0.072 GB
Job finished. 1 tasks completed successfully, 0 tasks failed
Submitting job named sample-gather to evaluate gather
0 files (0 bytes) out of 3 files will be uploaded
tasks: pending (1), worker nodes: running (1)
tasks: claimed (1), worker nodes: running (1)
[22:35:55] [starting tail of log sample-gather.1]
[22:35:57] [2.0, 4.0, 6.0, 8.0]
Job finished. 1 tasks completed successfully, 0 tasks failed
```

In this output you can see the three distinct phases been run as 
three different jobs. After all three phases have completed, the `sparkles sub ...` command terminates.

##### R implementation

The R implementation works identically, and the choice whether to run via Rscript or python is determined by the file extension of the script being run. 

You can see `examples/foreach/sample-scatter.r` as an example of an R script which fits a linear model for each feature of the iris dataset using all other features in the dataset. Then for each model, it computes the RMSE, and collects all the results and writes out a csv file.

Again the functions `get_foreach_args`, `foreach` and `gather` are invoked, with the same parameters. Only the syntax is different.

The contents of `examples/foreach/sample-scatter.r` is as follows:

```
# function for converting factors to one-hot columns
one.hot <- function(x) {
  sapply( levels(x), function(level) {
    as.numeric(x == level)
  })
}

# regress the target feature using all features except itself
fit.and.get.rmsq <- function(target.index, features) {
  model <- lm.fit(features[,-target.index], features[,target.index])
  # return RMSE
  mean(model$residuals ** 2) ** 0.5
}

# returns the list of elements to compute foreach on and additional arguments to pass to foreach
get_foreach_args <- function() {
  data(iris)

  # turn species from catagorical to one-hot encoded and get a numeric matrix of features
  features <- as.matrix(cbind(iris[,1:4], one.hot(iris[,5])))

  list(elements=seq(ncol(features)), extra_args=list(features))
}

# evaluated once per element return from scatter()
foreach <- function(i, features) {
  data.frame(target=colnames(features)[[i]], rmse=fit.and.get.rmsq(i, features))
}

# evaluated on the list of values returned from all foreach calls
gather <- function(results, ...) {
 print("results")
 str(results)
  df <- do.call(rbind, results)
  write.csv(df, file="results.csv")
}
```

This script can be run as:

```
sparkles sub --foreach -i r-base:3.5.2 -n sample sample-scatter.r
```

Upon completion, we can download `results.csv` and see that it contains:

```
"","target","rmse"
"1","Sepal.Length",0.300626956897024
"2","Sepal.Width",0.262392490381384
"3","Petal.Length",0.257419025528041
"4","Petal.Width",0.163249617363952
"5","setosa",0.0751863401588153
"6","versicolor",0.0818033953243054
"7","virginica",0.0683978359513745
```

### Monitoring jobs

`sparkles watch jobname`

#### Controlling parallelism
        controlling scale
            --nodes
            --preemptable

### Killing jobs

`sparkles kill jobname`

### Inspecting jobs

`sparkles logs jobname`
`sparkles list jobname`

`sparkles status`
