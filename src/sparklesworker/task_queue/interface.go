// Package task_queue is a compatibility shim. All types and interfaces have
// moved to the backend package. This package re-exports them as type aliases
// so existing callers compile without changes.
package task_queue

import (
	"github.com/broadinstitute/sparklesworker/backend"
)

// Type aliases — these are identical to the backend types.
type UploadSpec = backend.UploadSpec
type TaskSpec = backend.TaskSpec
type TaskHistory = backend.TaskHistory
type Task = backend.Task
type Job = backend.Job
type CachedTaskEntry = backend.CachedTaskEntry

// Interface aliases.
type TaskQueue = backend.TaskStore
type TaskCache = backend.TaskCache

// Status constants.
const (
	StatusClaimed   = backend.StatusClaimed
	StatusPending   = backend.StatusPending
	StatusComplete  = backend.StatusComplete
	StatusKilled    = backend.StatusKilled
	StatusFailed    = backend.StatusFailed
	JobStatusKilled = backend.JobStatusKilled
)
