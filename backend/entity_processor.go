package backend

import (
	"context"
	"fmt"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type entityProcessor struct {
	backend  EntityBackend
	executor EntityExecutor
}

// NewEntityWorker creates a local worker for native entity work items.
func NewEntityWorker(be EntityBackend, executor EntityExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	return NewTaskWorker(&entityProcessor{backend: be, executor: executor}, logger, opts...)
}

func (*entityProcessor) Name() string {
	return "entity-processor"
}

func (p *entityProcessor) FetchWorkItem(ctx context.Context) (WorkItem, error) {
	return p.backend.GetEntityWorkItem(ctx)
}

func (p *entityProcessor) ProcessWorkItem(ctx context.Context, workItem WorkItem) error {
	wi := workItem.(*EntityWorkItem)
	request := &protos.EntityRequest{
		InstanceId:        wi.InstanceID.String(),
		ExecutionId:       wi.ExecutionID,
		OperationRequests: wi.Operations,
	}
	if wi.State != nil {
		request.EntityState = wrapperspb.String(*wi.State)
	}
	batch, operationInfos, err := EntityBatchFromRequestV2(request)
	if err != nil {
		return err
	}
	result, err := p.executor.ExecuteEntity(ctx, batch)
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("entity executor returned no result")
	}
	if result.FailureDetails != nil {
		return fmt.Errorf("entity batch dispatch failed: %s", result.FailureDetails.ErrorMessage)
	}
	if result.RequiresState {
		return fmt.Errorf("entity executor requested state that was already supplied by the backend")
	}
	if len(operationInfos) > len(result.Results) {
		operationInfos = operationInfos[:len(result.Results)]
	}
	result.OperationInfos = append([]*protos.OperationInfo(nil), operationInfos...)
	wi.Result = result
	return nil
}

func (p *entityProcessor) CompleteWorkItem(ctx context.Context, workItem WorkItem) error {
	return p.backend.CompleteEntityWorkItem(ctx, workItem.(*EntityWorkItem))
}

func (p *entityProcessor) AbandonWorkItem(ctx context.Context, workItem WorkItem) error {
	return p.backend.AbandonEntityWorkItem(ctx, workItem.(*EntityWorkItem))
}

func (p *entityProcessor) GetBacklogMetric(ctx context.Context) (BacklogMetric, bool, error) {
	provider, ok := p.backend.(interface {
		GetEntityBacklog(context.Context) (BacklogMetric, error)
	})
	if !ok {
		return BacklogMetric{}, false, nil
	}
	metric, err := provider.GetEntityBacklog(ctx)
	return metric, true, err
}
