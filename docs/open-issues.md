When a poller writes out a problem no where to see it from the UI

When a workpool is halted, no way to reset

Insufficient permissions granted to service account to actually run batch jobs.

(Silently hangs) Added owner to SA to workaround

configuring docker registry auth for us-central1-docker.pkg.dev/cds-docker-containers/docker/daintree:0.5.0: docker-credential-gcr configure-docker: exec: "docker-credential-gcr": executable file not found in $PATH:

No way to terminate a workpool completely

No way to tell if a job has completed just from gcs (which is an issue because sprinkles tracking is transient. GCS is the final record)

No way to resume a partially completed task (add a resume flag which onto task 1. skips running anything if we already have a successful result.json, 2. localizes files from folder and re-runs command)
Would enable jobs to be able to periodically checkpoint. (Checkpointing directly to GCS would probably be better...) Regardless, this mechanism would be helpful for detecting expired tasks which actually already have been run.

Need a "retry" button the UI for re-running failed/complete with error tasks. (And corresponding API endpoint)

Add support for custom metrics? (probably best to skip... but could be handy. Need motivating usecase to prototype)

Top level page should be jobs, system log, workpools

Add flag to control streaming of logs. (Check on firestore costs)

Need to think: Some way to hash job parameters to detect we're submitting something that's already been run?
