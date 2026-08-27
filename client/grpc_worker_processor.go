package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (w *TaskHubGrpcWorker) consumeConnection(run *grpcWorkerRun, connection *grpcWorkerConnection) (bool, error) {
	observedMessage := false
	for {
		workItem, err := w.receiveWorkItem(connection)
		if err != nil {
			return observedMessage, err
		}
		observedMessage = true

		switch request := workItem.Request.(type) {
		case *protos.WorkItem_HealthPing:
			continue
		case *protos.WorkItem_OrchestratorRequest:
			if err := w.dispatchOrchestration(run, connection, workItem.GetCompletionToken(), request.OrchestratorRequest); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_ActivityRequest:
			if err := w.dispatchActivity(run, connection, workItem.GetCompletionToken(), request.ActivityRequest); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_EntityRequest:
			if err := w.dispatchEntity(run, connection, workItem.GetCompletionToken(), func(ctx context.Context) {
				w.processEntityBatch(ctx, connection.client, workItem.GetCompletionToken(), request.EntityRequest, nil)
			}); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_EntityRequestV2:
			if err := w.dispatchEntity(run, connection, workItem.GetCompletionToken(), func(ctx context.Context) {
				w.processEntityV2(ctx, connection.client, workItem.GetCompletionToken(), request.EntityRequestV2)
			}); err != nil {
				return observedMessage, err
			}
		default:
			w.logger.Warnf("received unknown work item type with completion token present=%t", workItem.GetCompletionToken() != "")
		}
	}
}

func (w *TaskHubGrpcWorker) receiveWorkItem(connection *grpcWorkerConnection) (*protos.WorkItem, error) {
	workItem, err := recvBeforeSilenceTimeout(connection.stream.Recv, connection.cancelStream, w.options.silentDisconnectTimeout)
	if err != nil {
		return nil, err
	}
	if workItem == nil {
		return nil, status.Error(codes.Internal, "received a nil work item")
	}
	return workItem, nil
}

// recvBeforeSilenceTimeout receives one message from a server stream, canceling
// the stream and reporting errSilentDisconnect if nothing arrives before the
// configured silent disconnect timeout.
func recvBeforeSilenceTimeout[T any](recv func() (T, error), cancelStream context.CancelFunc, timeout time.Duration) (T, error) {
	timedOut := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		cancelStream()
		close(timedOut)
	})
	message, err := recv()
	if !timer.Stop() {
		<-timedOut
		var zero T
		return zero, errSilentDisconnect
	}
	return message, err
}

func (w *TaskHubGrpcWorker) dispatchOrchestration(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	request *protos.OrchestratorRequest,
) error {
	return w.dispatch(run, connection, run.orchestrationSlots,
		func(ctx context.Context) { w.abandonOrchestration(ctx, connection.client, completionToken) },
		func(ctx context.Context) { w.processOrchestration(ctx, connection.client, completionToken, request) },
	)
}

func (w *TaskHubGrpcWorker) dispatchActivity(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	request *protos.ActivityRequest,
) error {
	return w.dispatch(run, connection, run.activitySlots,
		func(ctx context.Context) { w.abandonActivity(ctx, connection.client, completionToken) },
		func(ctx context.Context) { w.processActivity(ctx, connection.client, completionToken, request) },
	)
}

func (w *TaskHubGrpcWorker) dispatchEntity(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	process func(context.Context),
) error {
	return w.dispatch(
		run,
		connection,
		run.entitySlots,
		func(ctx context.Context) { w.abandonEntity(ctx, connection.client, completionToken) },
		process,
	)
}

// dispatch reserves a concurrency slot and runs process in the background under
// the processing context, so a graceful drain can still complete in-flight work.
// If intake is canceled before a slot is free, the work item is abandoned instead.
func (w *TaskHubGrpcWorker) dispatch(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	slots chan struct{},
	abandon func(context.Context),
	process func(context.Context),
) error {
	select {
	case slots <- struct{}{}:
	case <-run.intakeCtx.Done():
		abandon(run.processingCtx)
		return run.intakeCtx.Err()
	}

	run.pending.Add(1)
	connection.pending.Add(1)
	go func() {
		defer func() {
			<-slots
			connection.pending.Done()
			run.pending.Done()
		}()
		process(run.processingCtx)
	}()
	return nil
}

func (w *TaskHubGrpcWorker) processOrchestration(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.OrchestratorRequest,
) {
	pastEvents := request.PastEvents
	if request.RequiresHistoryStreaming {
		history, err := w.streamHistory(ctx, client, request)
		if err != nil {
			w.logger.Errorf("%s: failed to stream required orchestration history: %v", request.InstanceId, err)
			w.abandonOrchestration(ctx, client, completionToken)
			return
		}
		pastEvents = history
	}

	results, err := w.executor.ExecuteOrchestrator(ctx, api.InstanceID(request.InstanceId), pastEvents, request.NewEvents)
	var versionMismatch *task.VersionMismatchError
	if errors.As(err, &versionMismatch) {
		w.logger.Warnf("%s: orchestration version mismatch; abandoning work item: %v", request.InstanceId, err)
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}
	if err != nil && ctx.Err() != nil {
		w.logger.Warnf("%s: orchestration execution canceled; abandoning work item", request.InstanceId)
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}

	response := &protos.OrchestratorResponse{
		InstanceId:                request.InstanceId,
		CompletionToken:           completionToken,
		OrchestrationTraceContext: request.OrchestrationTraceContext,
	}
	switch {
	case err != nil:
		response.Actions = []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			wrapperspb.String("An internal error occurred while executing the orchestration."),
			nil,
			&protos.TaskFailureDetails{
				ErrorType:    fmt.Sprintf("%T", err),
				ErrorMessage: err.Error(),
			},
		)}
	case results == nil || results.Response == nil:
		response.Actions = []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			wrapperspb.String("The orchestration executor returned no response."),
			nil,
			&protos.TaskFailureDetails{
				ErrorType:    "MissingOrchestratorResponse",
				ErrorMessage: "the orchestration executor returned no response",
			},
		)}
	default:
		response = proto.Clone(results.Response).(*protos.OrchestratorResponse)
		response.InstanceId = request.InstanceId
		response.CompletionToken = completionToken
		if response.OrchestrationTraceContext == nil {
			response.OrchestrationTraceContext = request.OrchestrationTraceContext
		}
	}

	err = w.executeRPCWithRetry(ctx, "complete orchestration task", func(callCtx context.Context) error {
		_, callErr := client.CompleteOrchestratorTask(callCtx, response)
		return callErr
	})
	if err != nil {
		w.logger.Errorf("%s: failed to complete orchestration work item: %v", request.InstanceId, err)
		w.abandonOrchestration(ctx, client, completionToken)
	}
}

func (w *TaskHubGrpcWorker) streamHistory(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	request *protos.OrchestratorRequest,
) ([]*protos.HistoryEvent, error) {
	historyCtx, cancelHistory := context.WithCancel(ctx)
	defer cancelHistory()
	stream, err := client.StreamInstanceHistory(historyCtx, &protos.StreamInstanceHistoryRequest{
		InstanceId:            request.InstanceId,
		ExecutionId:           request.ExecutionId,
		ForWorkItemProcessing: true,
	})
	if err != nil {
		return nil, err
	}

	var history []*protos.HistoryEvent
	for {
		chunk, recvErr := recvBeforeSilenceTimeout(stream.Recv, cancelHistory, w.options.silentDisconnectTimeout)
		if errors.Is(recvErr, io.EOF) {
			return history, nil
		}
		if recvErr != nil {
			return nil, recvErr
		}
		if chunk == nil {
			return nil, status.Error(codes.Internal, "received a nil history chunk")
		}
		history = append(history, chunk.Events...)
	}
}

func (w *TaskHubGrpcWorker) processActivity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.ActivityRequest,
) {
	if request.OrchestrationInstance == nil {
		w.logger.Error("received activity work item without an orchestration instance; abandoning it")
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	event := helpers.NewTaskScheduledEvent(
		request.TaskId,
		request.Name,
		request.Version,
		request.Input,
		request.ParentTraceContext,
	)
	event.GetTaskScheduled().Tags = contextprop.Clone(request.Tags)
	result, err := w.executor.ExecuteActivity(ctx, api.InstanceID(request.OrchestrationInstance.InstanceId), event)
	var versionMismatch *task.VersionMismatchError
	if errors.As(err, &versionMismatch) {
		w.logger.Warnf(
			"%s/%s#%d: activity version mismatch; abandoning work item: %v",
			request.OrchestrationInstance.InstanceId,
			request.Name,
			request.TaskId,
			err,
		)
		w.abandonActivity(ctx, client, completionToken)
		return
	}
	if err != nil && ctx.Err() != nil {
		w.logger.Warnf("%s/%s#%d: activity execution canceled; abandoning work item", request.OrchestrationInstance.InstanceId, request.Name, request.TaskId)
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	response := &protos.ActivityResponse{
		InstanceId:      request.OrchestrationInstance.InstanceId,
		TaskId:          request.TaskId,
		CompletionToken: completionToken,
	}
	if err != nil {
		response.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: err.Error(),
		}
	} else if completed := result.GetTaskCompleted(); completed != nil {
		response.Result = completed.Result
	} else if failed := result.GetTaskFailed(); failed != nil {
		response.FailureDetails = failed.FailureDetails
	} else {
		response.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    "UnknownTaskResult",
			ErrorMessage: "activity executor returned an unknown task result",
		}
	}

	err = w.executeRPCWithRetry(ctx, "complete activity task", func(callCtx context.Context) error {
		_, callErr := client.CompleteActivityTask(callCtx, response)
		return callErr
	})
	if err != nil {
		w.logger.Errorf("%s/%s#%d: failed to complete activity work item: %v", request.OrchestrationInstance.InstanceId, request.Name, request.TaskId, err)
		w.abandonActivity(ctx, client, completionToken)
	}
}

func (w *TaskHubGrpcWorker) processEntityV2(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.EntityRequest,
) {
	batch, operationInfos, err := backend.EntityBatchFromRequestV2(request)
	if err != nil {
		w.logger.Errorf("invalid V2 entity work item: %v", err)
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	w.processEntityBatch(ctx, client, completionToken, batch, operationInfos)
}

func (w *TaskHubGrpcWorker) processEntityBatch(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.EntityBatchRequest,
	operationInfos []*protos.OperationInfo,
) {
	executor, ok := w.executor.(backend.EntityExecutor)
	if !ok {
		w.logger.Error("task executor does not support entity work items")
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	result, err := executor.ExecuteEntity(ctx, request)
	if err != nil {
		w.logger.Errorf("%s: entity execution failed; abandoning work item: %v", request.GetInstanceId(), err)
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	if result == nil {
		w.logger.Errorf("%s: entity executor returned no result; abandoning work item", request.GetInstanceId())
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	result = proto.Clone(result).(*protos.EntityBatchResult)
	result.CompletionToken = completionToken
	if len(operationInfos) > len(result.Results) {
		operationInfos = operationInfos[:len(result.Results)]
	}
	result.OperationInfos = append([]*protos.OperationInfo(nil), operationInfos...)

	err = w.executeRPCWithRetry(ctx, "complete entity task", func(callCtx context.Context) error {
		_, callErr := client.CompleteEntityTask(callCtx, result)
		return callErr
	})
	if err != nil {
		w.logger.Errorf("%s: failed to complete entity work item: %v", request.GetInstanceId(), err)
		w.abandonEntity(ctx, client, completionToken)
	}
}

func (w *TaskHubGrpcWorker) abandonOrchestration(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon orchestration work item without a completion token")
		return
	}
	if err := w.executeRPCWithRetry(ctx, "abandon orchestration task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskOrchestratorWorkItem(callCtx, &protos.AbandonOrchestrationTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon orchestration work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) abandonActivity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon activity work item without a completion token")
		return
	}
	if err := w.executeRPCWithRetry(ctx, "abandon activity task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskActivityWorkItem(callCtx, &protos.AbandonActivityTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon activity work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) abandonEntity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon unsupported entity work item without a completion token")
		return
	}
	if err := w.executeRPCWithRetry(ctx, "abandon entity task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskEntityWorkItem(callCtx, &protos.AbandonEntityTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon unsupported entity work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) executeRPCWithRetry(
	ctx context.Context,
	operation string,
	action func(context.Context) error,
) error {
	var lastErr error
	for attempt := 1; attempt <= w.options.transientRetryMaxAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, w.options.rpcTimeout)
		lastErr = action(callCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransientWorkerRPCError(lastErr) {
			return fmt.Errorf("%s failed with a non-retryable error: %w", operation, lastErr)
		}
		if attempt == w.options.transientRetryMaxAttempts {
			break
		}
		if err := waitForRetry(ctx, w.retryDelay(attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, w.options.transientRetryMaxAttempts, lastErr)
}

func (w *TaskHubGrpcWorker) retryDelay(attempt int) time.Duration {
	delay := w.options.transientRetryBaseDelay
	for i := 1; i < attempt && delay < w.options.transientRetryMaxDelay; i++ {
		if delay > w.options.transientRetryMaxDelay/2 {
			return w.options.transientRetryMaxDelay
		}
		delay *= 2
	}
	if delay > w.options.transientRetryMaxDelay {
		return w.options.transientRetryMaxDelay
	}
	return delay
}

func isTransientWorkerRPCError(err error) bool {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	return isTransientWorkerGRPCCode(grpcStatus.Code())
}
