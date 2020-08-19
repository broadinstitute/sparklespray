# Version 2.4.0

- Added --loglive flag to "submit"

- Fixed broken "kill" command. Now defaults to shutting down nodes as well
  as marking job as killed. Waits for nodes to terminate before exiting.

- Added check to verify jobid is unique before submission.

- Added --clean flag to allow deleting job with existing name before
  submitting.

- Bug fixes to "clean" command

- Allow "gs://" urls to be specified as the source location of files and better support for downloading nested
  folders.
