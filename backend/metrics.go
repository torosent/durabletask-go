package backend

import (
	"context"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

// WorkItemKind identifies an orchestration or activity work item.
type WorkItemKind string

const (
	WorkItemKindOrchestration WorkItemKind = "orchestration"
	WorkItemKindActivity      WorkItemKind = "activity"
)

// WorkerActivityState describes a worker lifecycle transition.
type WorkerActivityState string

const (
	WorkerActivityStarted   WorkerActivityState = "started"
	WorkerActivityCompleted WorkerActivityState = "completed"
	WorkerActivityAbandoned WorkerActivityState = "abandoned"
)

// BacklogMetric reports the current depth and oldest age of a work-item queue.
type BacklogMetric struct {
	Kind      WorkItemKind
	Depth     int64
	OldestAge time.Duration
}

// WorkerActivityMetric reports one worker lifecycle transition.
type WorkerActivityMetric struct {
	Kind         WorkItemKind
	State        WorkerActivityState
	InstanceID   api.InstanceID
	ActivityName string
	RetryCount   int32
	InFlight     int64
	Duration     time.Duration
}

// RetryMetric reports a durable task retry that was scheduled.
type RetryMetric struct {
	InstanceID           api.InstanceID
	OrchestrationName    string
	OrchestrationVersion string
	TaskKind             WorkItemKind
	TaskName             string
	TaskVersion          string
	FailedAttempt        int
	NextAttempt          int
	MaxAttempts          int
	Delay                time.Duration
	ErrorType            string
	ErrorMessage         string
}

// HistoryMetric reports orchestration history usage for one execution turn.
type HistoryMetric struct {
	InstanceID           api.InstanceID
	OrchestrationName    string
	OrchestrationVersion string
	HistoryLength        int
	ProcessedEvents      int
	HistoryLimitExceeded bool
}

// MetricsHooks contains optional backend-neutral metric callbacks.
// Callbacks must return quickly and must not block worker progress.
type MetricsHooks struct {
	Backlog        func(BacklogMetric)
	WorkerActivity func(WorkerActivityMetric)
	Retry          func(RetryMetric)
	History        func(HistoryMetric)
}

// BacklogSnapshotProvider is implemented by backends that can inspect local queues.
type BacklogSnapshotProvider interface {
	GetOrchestrationBacklog(context.Context) (BacklogMetric, error)
	GetActivityBacklog(context.Context) (BacklogMetric, error)
}
