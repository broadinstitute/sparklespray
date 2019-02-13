### Introduction

Sparkle spray, or "sparkles" for short, is a command line tool to make it easy to submit adhoc batch jobs for execution on the cloud on the Google Cloud Platform.

"sparkles" was designed in the spirit of batch submission tools such as Grid Engine [http://www.univa.com/products/] or Slurm [https://slurm.schedmd.com/sbatch.html] with the key difference that sparkles powers up nodes on GCP, instead of scheduling the work on a traditional HPC cluster.

The goal of sparkles is to lower the amount of effort required to run a non-interactive script or other compute on the cloud. In addition, once one can run a script via sparkles, it's trivial to run a batch of commands with some varying parameters in parallel.


### Quick start

All sparkles commands are subcommands of the command line tool "sparkles". The most commonly used is "sub" which is short for "submit".

Perhaps the simplest example of a submission would be to run:

```
sparkles sub echo hello world
```

This should yield output similar to:

```
$ sparkles sub echo hello world                                                       0 files (0 bytes) out of 1 files will be uploaded
tasks: pending (1), worker nodes:
tasks: pending (1), worker nodes: staging (1)
[21:29:46] hello world
Job finished. 1 tasks completed successfully, 0 tasks failed
Done waiting for job. You can download results via 'gsutil rsync -r gs://test-sparkles-bucket/20190212-212815-bf0a DEST_DIR'
```

The "hello world" which was written at the output, actually executed on a VM which was powered on solely to run the "echo" command. The stdout of that command was then written to the terminal and then the VM automatically shuts down after the command's completion.

It's worth noting that this trival example took around 80 seconds to execute, which is fairly uninspiring. The time spent is largely due to the time it takes to power on a VM. However, later in the tutorial, we'll see we can get job submissions down to a few seconds by letting sparkles reuse VMs.

### Tutorial

#### Terminology

First some basic terminology:

"node" - a VM (more specifically, a Google Compute Engine instance) which sparkles powered on.
"task" - A single command to be executed.
"job" - The unit of work submitted by executing `sparkles sub`. One job may have multiple "tasks" associated with it.

You will see these terms appear in the output of sparkles and through out the documentation.

All interaction with sparkles is through the `sparkles` command which is divided into 
several sub commands. Executing `sparkles --help` will list all subcommands with a description for each.

#### Job submission

`sparkles sub ...` is used for submitting a job and has by far the most options. 

##### Naming jobs to improve performance

The first option that we generally always want to use is `-n` to give the job a name. In practice, giving a job a name substantially speeds up job submissions, because when a job is submitted, sparkles decides whether there is an existing VM already running which can take tasks for that job, or whether it needs to turn on new nodes.

Turning on new nodes always takes around a minute or two, but if there's an existing node, then it can pick up the task immediately with no delay. The critera for a "compatible VM" is one where the machine type, the docker image and the job name all match. If job name is not specified a unique one is generated each time, resulting in an new VM each submission.

##### Pushing files along with job submission

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

##### Submission of parallel jobs

            sub --params
            sub --seq
        
        foreach

#### Monitoring jobs

`sparkles watch jobname`

##### Controlling parallelism
        controlling scale
            --nodes
            --preemptable

#### Killing jobs

`sparkles kill jobname`

#### Inspecting jobs

`sparkles logs jobname`
`sparkles list jobname`

`sparkles status`
