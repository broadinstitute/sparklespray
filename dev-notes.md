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
    - have sparkles detect whether login is needed. (x)
Status should report where kubernetes job exists
Reaper should look for example of lots of failed pods (sign of sparkles-consume failing)
Something should clean out dead pods
statically linked version of sparkles-consume

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
    update sparkles to allow programatic:
        job submission via json file
        job wait (Automatically download all results.json to dest location?)

## Todo
Fix warning about jobs queued up to list job ids of other jobs.

Add zones to job entity in datastore so that we know which zones to look for nodes
in.

When adding nodes, wait until operation reaches provisioning and print
description if it's not.
for example:
  events:
  - description: 'Warning: Creating VM and disk(s) would exceed "LOCAL_SSD_TOTAL_GB"
      in region us-east1, will try again'
    startTime: '2018-02-06T03:17:39.655426353Z'

- A mode for watch which keeps cluster size stable. If pool shrinks add
  nodes back.

- bring back pipeline code. Make it such that function runs on a fetched
  tree to make it easier to debug

