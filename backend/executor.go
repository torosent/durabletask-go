package backend

import (
	"context"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// ExecutionResults carries the orchestrator actions produced by a single
// orchestrator execution turn. It is transport-neutral: the DTS worker sends
// [ExecutionResults.Response] back to the scheduler over gRPC.
type ExecutionResults struct {
	Response *protos.OrchestratorResponse
}

// Executor executes orchestrator and activity work items handed to it by a
// worker. [github.com/microsoft/durabletask-go/task.NewTaskExecutor] is the
// in-process implementation the DTS worker uses.
type Executor interface {
	ExecuteOrchestrator(
		ctx context.Context,
		iid api.InstanceID,
		oldEvents []*protos.HistoryEvent,
		newEvents []*protos.HistoryEvent,
	) (*ExecutionResults, error)
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	Shutdown(ctx context.Context) error
}
