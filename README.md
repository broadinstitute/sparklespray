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

### Set up a python 3.11 virtual environment

sparkles uses google's services python client libraries, which in turn have a
fair number of their own dependencies, so it's really best to create virtual
environment to install sparkles to keep it isolated from other libraries
installed on your system. One can probably use virtualenv do
this, but I'm including conda because that's what I personally use and have
tested with.

Create the conda environment and activate it:

```
conda create -n sparkles python=3.11
conda activate sparkles
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

### Parameterized Jobs

Sparklespray supports two main methods of parameterization: sequence-based and CSV-based. These allow you to run multiple variations of a job with different parameters.

#### Sequence-Based Parameterization

Use `--seq N` to run N variations of your command, with an `{index}` parameter varying from 1 to N.

```bash
# Run 5 variations
sparkles sub --seq 5 python script.py --index {index}

# This will execute:
# python script.py --index 1
# python script.py --index 2
# python script.py --index 3
# python script.py --index 4
# python script.py --index 5
```

Common use cases:

```bash
# Process different data shards
sparkles sub --seq 10 python process.py --shard {index} --total-shards 10

# Train models with different random seeds
sparkles sub --seq 5 python train.py --seed {index} --model-dir model_{index}

# Process different date ranges
sparkles sub --seq 12 python analyze.py --month {index} --year 2024
```

#### CSV-Based Parameterization

Use `--params filename.csv` to run variations based on CSV file contents. Each row in the CSV generates a separate task.

Basic CSV parameterization:

```csv
# params.csv
input_file,output_file,threshold
data1.txt,result1.txt,0.5
data2.txt,result2.txt,0.7
data3.txt,result3.txt,0.6
```

```bash
sparkles sub --params params.csv python script.py --input {input_file} --output {output_file} --threshold {threshold}
```

Advanced CSV examples:

1. Machine Learning Hyperparameter Sweep:

```csv
# hyperparams.csv
learning_rate,batch_size,hidden_layers,dropout
0.001,32,2,0.1
0.001,64,2,0.1
0.0001,32,3,0.2
0.0001,64,3,0.2
```

```bash
sparkles sub --params hyperparams.csv \
    -u train.py -u data/ \
    python train.py \
    --lr {learning_rate} \
    --batch-size {batch_size} \
    --layers {hidden_layers} \
    --dropout {dropout} \
    --output-dir model_{learning_rate}_{batch_size}_{hidden_layers}_{dropout}
```

2. Data Processing Pipeline:

```csv
# pipeline.csv
input_path,output_path,start_date,end_date,region
gs://data/raw/us,gs://data/processed/us,2024-01-01,2024-03-31,US
gs://data/raw/eu,gs://data/processed/eu,2024-01-01,2024-03-31,EU
gs://data/raw/asia,gs://data/processed/asia,2024-01-01,2024-03-31,ASIA
```

```bash
sparkles sub --params pipeline.csv \
    python process.py \
    --input {input_path} \
    --output {output_path} \
    --start {start_date} \
    --end {end_date} \
    --region {region}
```

3. Feature Engineering:

```csv
# features.csv
feature_name,window_size,aggregation,min_samples
daily_revenue,7,sum,100
weekly_users,30,average,500
monthly_growth,90,percentage,1000
```

```bash
sparkles sub --params features.csv \
    python engineer_features.py \
    --feature {feature_name} \
    --window {window_size} \
    --agg {aggregation} \
    --min-samples {min_samples} \
    --output features_{feature_name}.parquet
```

#### Parameter Substitution Rules

1. Basic Substitution

   - Use `{param_name}` to substitute CSV column values
   - Parameters are case-sensitive
   - Missing parameters result in error

2. File Path Handling

   ```bash
   # Automatic path joining
   sparkles sub --params params.csv python script.py \
       --data-dir data/{region}/{date} \
       --output results/{region}/{date}/output.csv
   ```

3. Nested Parameters
   ```bash
   # Parameters can be used multiple times
   sparkles sub --params params.csv python script.py \
       --input data_{region}/input_{date}.csv \
       --output data_{region}/output_{date}.csv \
       --log logs/{region}_{date}.log
   ```

#### Best Practices for Parameterization

1. CSV Organization

   - Use clear, descriptive column names
   - Include all required parameters
   - Keep CSV files version controlled
   - Document parameter meanings and valid ranges

2. Parameter Validation

   ```python
   # In your script
   def validate_params(args):
       assert float(args.learning_rate) > 0, "Learning rate must be positive"
       assert int(args.batch_size) > 0, "Batch size must be positive"
       assert args.region in ["US", "EU", "ASIA"], "Invalid region"
   ```

3. Output Management

   - Include parameters in output paths
   - Use consistent naming patterns
   - Avoid parameter values that create invalid paths

4. Resource Considerations

   ```bash
   # Scale nodes based on parameter count
   sparkles sub --params large_sweep.csv --nodes 10 python train.py ...
   ```

5. Debugging Parameterized Jobs
   ```bash
   # Test with subset of parameters
   head -n 2 params.csv > test_params.csv
   sparkles sub --params test_params.csv --local python script.py ...
   ```

# Configuration reference

Sparklespray uses a configuration file (`.sparkles`) to define how jobs are executed and managed. The file can be placed in your home directory or any parent directory of where you run the `sparkles` command.

## Basic Configuration Format

The configuration file uses INI format with a `[config]` section:

```ini
[config]
default_url_prefix=gs://your-bucket
project=your-project
default_image=alpine
machine_type=n1-standard-1
zones=us-east1-b
region=us-east1
```

## Required Parameters

| Parameter            | Description                                    |
| -------------------- | ---------------------------------------------- |
| `default_url_prefix` | Base GCS path for job outputs and temp storage |
| `project`            | Google Cloud project ID                        |
| `default_image`      | Default Docker image for jobs                  |
| `machine_type`       | GCE machine type (e.g., n1-standard-1)         |
| `region`             | GCP region for resources                       |
| `account`            | GCP service account email                      |
| `zones`              | Which zones to create VMs in                   |

Some configuration values can be inherited from your gcloud configuration (`~/.config/gcloud/configurations/config_default`):

- `project`
- `region`
- `zones` (from compute/zone)

## Optional Parameters

### General Settings

| Parameter                 | Default                          | Description                                      |
| ------------------------- | -------------------------------- | ------------------------------------------------ |
| `monitor_port`            | 6032                             | Port for job monitoring interface                |
| `work_root_dir`           | "/mnt/"                          | Base directory for job execution                 |
| `cas_url_prefix`          | `{default_url_prefix}/CAS/`      | Storage location for CAS files (temporary files) |
| `sparklesworker_exe_path` | Auto-detected                    | Path to sparklesworker executable                |
| `cache_db_path`           | ".kubeque-cached-file-hashes"    | Path to file hash cache                          |
| `debug_log_prefix`        | `{default_url_prefix}/node-logs` | Location for debug logs                          |

### Storage Configuration

| Parameter            | Default          | Description                               |
| -------------------- | ---------------- | ----------------------------------------- |
| `boot_volume_in_gb`  | 20               | Size of boot disk in GB                   |
| `mount_count`        | 1                | Number of additional disk mounts          |
| `mount_N_path`       | "/mnt" (for N=1) | Mount path for disk N                     |
| `mount_N_type`       | Varies\*         | Disk type for mount N                     |
| `mount_N_size_in_gb` | 100              | Size in GB for mount N                    |
| `mount_N_name`       | None             | Name of existing disk to mount (optional) |

\*Default disk type depends on machine type:

- n4-\*: "hyperdisk-balanced"
- n1-_ or n2-_: "local-ssd"
- Others: "pd-balanced"

### Preemption Settings

| Parameter                        | Default | Description                            |
| -------------------------------- | ------- | -------------------------------------- |
| `preemptible`                    | "y"     | Use preemptible instances ("y" or "n") |
| `max_preemptable_attempts_scale` | 2       | Max retry attempts for preempted jobs  |

### Authentication Settings

| Parameter             | Default                                         | Description                      |
| --------------------- | ----------------------------------------------- | -------------------------------- |
| `service_account_key` | `~/.sparkles-cache/service-keys/{project}.json` | Path to service account key file |

## Example Configurations

Basic configuration with minimal settings:

```ini
[config]
default_url_prefix=gs://my-bucket/sparkles
project=my-project-id
default_image=ubuntu:20.04
machine_type=n1-standard-2
zones=us-east1-b
region=us-east1
account=my-service-account@project.iam.gserviceaccount.com
```

Configuration with a local SSD attached for temp storage:

```ini
[config]
default_url_prefix=gs://my-bucket/sparkles
project=my-project-id
default_image=ubuntu:20.04
machine_type=n1-standard-2
zones=us-east1-b
region=us-east1
account=my-service-account@project.iam.gserviceaccount.com
boot_volume_in_gb=50
mount_count=2
mount_1_path=/mnt/data
mount_1_type=pd-ssd
mount_1_size_in_gb=200
mount_2_path=/mnt/temp
mount_2_type=local-ssd
mount_2_size_in_gb=375
```

### Configuration File Location

Sparklespray searches for the configuration file in the following order:

1. Path specified by `--config` parameter
2. `.sparkles` in the current directory
3. `.sparkles` in any parent directory
4. `.kubeque` in the current directory (legacy)
5. `.kubeque` in any parent directory (legacy)

# Command reference

## The `setup` Command

The `setup` command configures your Google Cloud Project for use with Sparklespray by enabling required services and setting up necessary permissions.

```bash
sparkles setup
```

### Prerequisites

Before running setup:

1. **Google Cloud SDK**

   - Must be installed and in PATH
   - Authenticated via `gcloud auth login`
   - Default project configured

2. **Configuration File**

   - Valid `.sparkles` config file
   - Must contain:
     - `project`: Google Cloud project ID
     - `default_url_prefix`: GCS bucket URL (gs://bucket-name/...)
     - `service_account_key`: Path to service account key file

3. **Permissions**
   - Must have sufficient permissions to:
     - Enable Google Cloud APIs
     - Create service accounts
     - Create storage buckets
     - Manage IAM permissions

### What Setup Does

The setup command:

1. **Enables Required APIs**

   - Google Cloud Storage
   - Cloud Datastore
   - Cloud Batch
   - Cloud Container Registry

2. **Creates Resources**

   - Storage bucket (if doesn't exist)
   - Service account with required permissions
   - Service account key file

3. **Configures Permissions**
   - Grants necessary IAM roles
   - Sets up storage access
   - Configures service account

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

## The `status` Command

The `status` command provides detailed information about jobs and their tasks, including execution statistics and failure information.

## Basic Usage

```bash
sparkles status [options] [jobid_pattern]
```

### Command Options

| Option     | Description                         |
| ---------- | ----------------------------------- |
| `--stats`  | Show detailed execution statistics  |
| `--failed` | Show information about failed tasks |

### Basic Status Information

Without any options, the command shows a summary of task statuses:

```bash
# Check single job
sparkles status job-20240313-abc123
job-20240313-abc123: running(5), pending(10), complete(85)

# Check multiple jobs using pattern
sparkles status "job-2024*"
job-20240313-abc123: complete(100)
job-20240314-def456: running(20), pending(80)
```

## Detailed Statistics

Using the `--stats` flag provides comprehensive execution metrics:

```bash
sparkles status --stats job-20240313-abc123
```

Statistics include:

1. **Task Claim Information**

   ```
   Number of times a task was claimed quantiles: 1, 1, 1, 2, 5, mean: 1.4
   ```

   - Shows how many times tasks were retried
   - Provides min, 25%, median, 75%, max, and mean values

2. **Execution Time Statistics**

   ```
   task count: 100, execution time quantiles (in minutes):
   2.1, 2.8, 3.2, 3.9, 8.5, mean: 3.4
   ```

   - Shows task runtime distribution in minutes
   - Helps identify performance outliers

3. **Memory Usage**
   ```
   max memory quantiles:
   512MB, 768MB, 1024MB, 1536MB, 2048MB, mean: 1126MB
   ```
   - Shows peak memory usage across tasks
   - Helps with resource planning

## Reset command

```

sparkles reset JOB_ID

```

## Show details of tasks in the job

```

sparkles show JOB_ID

```

## The `list` Command

The `list` command provides detailed information about tasks within a job, with filtering and output formatting capabilities.

#### Basic Usage

```bash
sparkles list [options] job-id
```

#### Command Options

| Option           | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `--filter`       | Filter tasks using expressions (can be used multiple times) |
| `--fields`       | Specify which fields to include (comma-separated)           |
| `--format`       | Output format: 'csv' (default) or 'json'                    |
| `--output`, `-o` | Output file (defaults to stdout)                            |
| `--params`       | Only show parameters from original submission               |

#### Filtering Tasks

Use `--filter` to select specific tasks. Filter expressions follow the format:

```
field_name[.nested_field]=value
field_name[.nested_field]!=value
```

Example filters:

```bash
# Show only failed tasks
sparkles list job-id --filter "status=failed"

# Show tasks with specific exit code
sparkles list job-id --filter "exit_code!=0"

# Filter by parameter value
sparkles list job-id --filter "args.parameters.batch_size=64"
```

#### Field Selection

Use `--fields` to specify which fields to include in output:

```bash
# Basic fields
sparkles list job-id --fields "task_id,status,exit_code"

# Include nested fields
sparkles list job-id --fields "task_id,args.parameters.batch_size,status"
```

Common fields:

- `task_id`: Unique task identifier
- `status`: Current task status
- `exit_code`: Task exit code
- `owner`: Node that ran the task
- `args`: Task arguments and parameters
- `history`: Task execution history

#### Output Formats

1. **CSV Format** (default)

```bash
# Output to file
sparkles list job-id --format csv --output tasks.csv

# View in terminal
sparkles list job-id --format csv
```

2. **JSON Format**

```bash
# Detailed JSON output
sparkles list job-id --format json --output tasks.json
```

#### Parameter Extraction

Use `--params` to focus on task parameters:

```bash
# Extract original parameters
sparkles list job-id --params --output params.csv
```

Useful for:

- Identifying failed parameter combinations
- Rerunning specific parameter sets

#### Example Use Cases

2. **Parameter Analysis**

```bash
# Extract parameters of successful tasks
sparkles list job-id \
    --filter "exit_code=0" \
    --params \
    --output successful_params.csv
```

1. **Multiple Conditions**

```bash
sparkles list job-id \
    --filter "status=complete" \
    --filter "exit_code!=0" \
    --filter "args.parameters.batch_size=128"
```

2. **Parameter-Based Filtering**

```bash
sparkles list job-id \
    --filter "args.parameters.learning_rate=0.001" \
    --filter "args.parameters.model_type=resnet" \
    --fields "task_id,status,args.parameters"
```

1. **Finding Failed Tasks for Resubmission**

```bash
# Export failed task parameters
sparkles list job-id \
    --filter "status=failed" \
    --params \
    --output failed_params.csv
```

3. **Resource Analysis**

```bash
# Export resource usage patterns
sparkles list job-id \
    --fields "task_id,memory_used,cpu_used,args.parameters" \
    --format csv \
    --output resources.csv
```

## The `watch` Command

The `watch` command monitors job execution, streams logs, and manages worker nodes. It's automatically invoked after job submission (unless `--no-wait` is specified) but can also be used separately to monitor existing jobs.

### Basic Usage

```bash
sparkles watch [options] job-id
```

### Command Options

| Option          | Default         | Description                                     |
| --------------- | --------------- | ----------------------------------------------- |
| `--nodes`, `-n` | From job config | Target number of worker nodes                   |
| `--verify`      | False           | Verify completion status and reset failed tasks |
| `--loglive`     | True            | Stream output logs in real-time                 |
| `--no-loglive`  | False           | Disable log streaming                           |

### Features

1. **Task Monitoring**

   - Tracks task status changes
   - Reports job progress
   - Shows completion statistics
   - Real-time output from running tasks

2. **Node Management**

   - Automatic worker scaling
   - Preemptible instance handling
   - Node health monitoring

3. **Interruption Handling**
   - Safe to Ctrl+C watch command
   - Job continues running
   - Can resume watching later

### Examples

Basic job monitoring:

```bash
# Monitor job by ID
sparkles watch job-20240313-abc123
```

Adjust worker count:

```bash
# Scale to 5 workers
sparkles watch my-job --nodes 5
```

Verify completion and reset failed tasks (only needed for debugging an troubleshooting. Normal users should not need to ever use this option):

```bash
sparkles watch my-job --verify
```

Disable log streaming:

```bash
sparkles watch my-job --no-loglive
```

#### Status Updates

The watch command provides periodic status updates showing:

```
tasks: running (5), pending (10), complete (85), failed (0)
workers: running (3), staging (1), terminated (0)
```

Status fields:

- **Tasks**
  - `pending`: Waiting to be executed
  - `running`: Currently executing
  - `complete`: Successfully finished. (This does not mean that the command exited without any errors, but rather, the command was successfully executed and the results collected. One will need to also check the exit code to confirm that the command was successful.)
  - `failed`: Failed execution (The command failed to start or there was an issue with the worker which resulted in the command being aborted before it was complete.)
- **Workers**
  - `running`: Actively processing tasks
  - `staging`: Starting up
  - `submitted`: Requested but not yet staging
  - `terminated`: Shut down

#### Worker Management

The watch command manages worker nodes based on:

1. Target node count (`--nodes`)
2. Preemptible instance settings
3. Job requirements
4. Current task queue

Scaling behavior:

```python
# Example scaling scenarios
workers = min(target_nodes, pending_tasks)
preemptible = min(max_preemptable_attempts, workers)
```

#### Log Streaming

When `--loglive` is enabled (default):

1. Automatically selects a running task
2. Streams stdout/stderr in real-time
3. Switches to new task if current completes
4. Shows timestamp for each log line

Example output:

```
[22:15:30] Starting task processing...
[22:15:31] Loading input data
[22:15:35] Processing batch 1/10
[22:15:40] Processing batch 2/10
```

#### Completion Verification

When `--verify` is used:

1. Checks all completed tasks
2. Verifies output files exist
3. Resets tasks with missing outputs
4. Reports verification progress

Example verification:

```
Verified 85 out of 100 completed tasks successfully wrote output
task task_123 missing gs://bucket/results/output.txt, resetting
```

# Developing sparklespray

The repo contains code in two languages: go and python. The go code is for
the "consumer" process which runs on each worker and takes jobs from the
queue. The python code is used by the "sparkles" command line tool.

To build a new release need the "consumer" compiled, and the python code
packaged up into a tar.gz file. This is automatically done by the github
action on this repo. If you go to the action run page, there should be
an "Artifacts" section where you can download the built tar.gz file.

```

# compile the go code and save the binary as ./sparklespray/bin/kubequeconsume

\$ sh build-consumer.sh

# make the installable python package which will be saved in the dist

# directory with the version on the end of the name.

# (Example: dist/sparklespray-3.0.2.tar.gz)

\$ python setup.py sdist

```

sparkles sub -u train_mlp_sm.py -u data -u benchmark_train_mlp.py --gpu n --gpu_count 0 -i tensorflow/tensorflow:latest-py3 --machine-type n1-standard-4 python benchmark_train_mlp.py --data_size small --test_description \'Machine type n1-standard-4, GPU count 0, small dataset\'

## Running Workflows

Sparkles now supports running multi-step workflows defined in JSON files. The workflow feature allows you to:

- Define a sequence of steps to be executed in order
- Specify different Docker images for each step
- Pass parameters between steps
- Fan out execution using CSV parameter files

### Usage

```
sparkles workflow run JOB_NAME WORKFLOW_DEFINITION_FILE [options]
```

#### Arguments:

- `JOB_NAME`: A name for your workflow job
- `WORKFLOW_DEFINITION_FILE`: Path to a JSON file containing the workflow definition

#### Options:

- `--retry`: Retry any failed tasks
- `--nodes N`: Maximum number of nodes to power on at one time
- `--parameter VAR=VALUE` or `-p VAR=VALUE`: Define variables to be used in the workflow

### Workflow Definition Format

Workflows are defined in JSON files with the following structure:

```json
{
  "steps": [
    {
      "command": ["echo", "Hello {job_name}"],
      "image": "ubuntu:latest",
      "parameters_csv": "path/to/parameters.csv"
    },
    {
      "command": ["python", "process.py", "--input={prev_job_path}/output.txt"],
      "image": "python:3.9"
    }
  ]
}
```

Each step can include:

- `command`: List of command arguments (supports variable expansion)
- `image`: Docker image to use (optional)
- `parameters_csv`: Path to a CSV file for fan-out execution (optional)
- `run_local`: Boolean flag to run the command locally (currently not supported)

### Variable Expansion

The workflow system supports variable expansion in commands and parameter CSV paths:

- `{job_name}`: Current job name
- `{job_path}`: Path to the current job
- `{prev_job_name}`: Previous step's job name
- `{prev_job_path}`: Path to the previous step's job
- Custom variables defined with `--parameter`

### Example

```
# Run a workflow with custom parameters
sparkles workflow run my-analysis workflow.json --parameter input_file=gs://mybucket/input.txt --nodes 10
```

This will execute all steps in the workflow, passing the parameters to each step as needed.

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

brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

# Common failure modes

* Cannot pull docker image
   * boot volume is too small
   * Insufficent permissions
* Cannot execute command
   * because command is not executable/missing
   * because image built for wrong arch
* Process exhausts memory and is killed
* VM becomes non-responsive
* VM is preempted


TODO:
(x) get tests passing
(x) get pyright checks passing
(x) Fix node count
(x) reimplement "validate"
(x) Fix drive assignment
Clean out excessive print statements (Maybe tackle this after vcr in place?)
```
